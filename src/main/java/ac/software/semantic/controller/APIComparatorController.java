package ac.software.semantic.controller;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.ComparatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.request.AnnotatorUpdateRequest;
import ac.software.semantic.payload.request.ComparatorUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.DatasetResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ComparatorService;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.ProjectService;
import ac.software.semantic.service.PrototypeService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.lookup.PrototypeLookupProperties;
import io.swagger.v3.oas.annotations.Parameter;

@RestController
@RequestMapping("/api/comparator")
public class APIComparatorController {

	@Autowired
    private ComparatorService comparatorService;

	@Autowired
    private DatasetService datasetService;

	@Lazy
	@Autowired
    private ProjectService projectService;

	@Autowired
	private APIUtils apiUtils;
	
	@Autowired
    @Qualifier("database")
    private Database database;

	@GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam Optional<ObjectId> datasetId, 
			                                        @RequestParam(required = false) ObjectId projectId,
			                                        @RequestParam(required = false) Integer page,
				                                    @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		if (datasetId.isPresent()) {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, Arrays.asList(new ObjectId[] {datasetId.get()}), comparatorService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
		} else if (projectId != null) {
			ListResult<DatasetResponse> datasets = apiUtils.getAllByUser(currentUser, currentUser, Arrays.asList(new ObjectId[] {projectId}), datasetService, projectService);
			
			return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasets.getData().stream().map(p -> new ObjectId(p.getId())).collect(Collectors.toList()), comparatorService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
		} else {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, comparatorService, apiUtils.pageable(page, size))).toResponseEntity();
		}
	}
	
	@GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam Optional<ObjectId> datasetId, 
			                                        @RequestParam(required = false) ObjectId projectId,
			                                        @RequestParam(required = false) Integer page,
				                                    @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		if (datasetId.isPresent()) {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId.get()}), comparatorService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
		} else if (projectId != null) {
			ListResult<DatasetResponse> datasets = apiUtils.getAllByUser(currentUser, currentUser, Arrays.asList(new ObjectId[] {projectId}), datasetService, projectService);
			
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasets.getData().stream().map(p -> new ObjectId(p.getId())).collect(Collectors.toList()), comparatorService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
		} else {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, comparatorService, apiUtils.pageable(page, size))).toResponseEntity();
		}
	}

    @PostMapping(value = "/new")
	public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true)@CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId, @RequestBody ComparatorUpdateRequest ur)  {
		return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), ur, comparatorService, datasetService).toResponseEntity();
	} 	
    
	@GetMapping(value = "/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), comparatorService).toResponseEntity();
	}	
    
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), comparatorService, datasetService).toResponseEntity();
	}
	
    @PostMapping(value = "/update/{id}")
    public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody ComparatorUpdateRequest ur)  {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, comparatorService).toResponseEntity();
	}
    
    @GetMapping(value = "/exists-identifier",
            produces = "application/json")
	public ResponseEntity<APIResponse> existsIdentifier(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true)  @RequestParam String identifier)  {
		
    	ComparatorDocument ur = new ComparatorDocument(database);
    	ur.setIdentifier(identifier);
    	
    	return apiUtils.identifierConflict(ur, IdentifierType.IDENTIFIER, comparatorService).toResponseEntity();
	}
}
