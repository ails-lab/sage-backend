package ac.software.semantic.controller;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.DatasetResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.ProjectService;
import ac.software.semantic.service.PrototypeService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.lookup.PrototypeLookupProperties;
import io.swagger.v3.oas.annotations.Parameter;

@RestController
@RequestMapping("/api/prototype")
public class APIPrototypeController {

	@Autowired
    private PrototypeService prototypeService;

	@Autowired
    private DatasetService datasetService;

	@Lazy
	@Autowired
    private ProjectService projectService;

	@Autowired
	private APIUtils apiUtils;

	@GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam Optional<ObjectId> datasetId, 
			                                        @RequestParam PrototypeType type,
			                                        @RequestParam(required = false) Integer page,
				                                    @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		PrototypeLookupProperties lp = new PrototypeLookupProperties();
		lp.setPrototypeType(type);
		
		if (datasetId.isPresent()) {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, Arrays.asList(new ObjectId[] {datasetId.get()}), lp, prototypeService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
		} else {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, lp, prototypeService, apiUtils.pageable(page, size))).toResponseEntity();
		}
	}
	
	@GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam Optional<ObjectId> datasetId, 
			                                        @RequestParam PrototypeType type,
			                                        @RequestParam(required = false) ObjectId projectId,
			                                        @RequestParam(required = false) Integer page,
				                                    @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		PrototypeLookupProperties lp = new PrototypeLookupProperties();
		lp.setPrototypeType(type);
		
		if (datasetId.isPresent()) {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId.get()}), lp, prototypeService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
		} else if (projectId != null) {
			ListResult<DatasetResponse> datasets = apiUtils.getAllByUser(currentUser, currentUser, Arrays.asList(new ObjectId[] {projectId}), datasetService, projectService);
			
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasets.getData().stream().map(p -> new ObjectId(p.getId())).collect(Collectors.toList()), lp, prototypeService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
		} else {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, lp, prototypeService, apiUtils.pageable(page, size))).toResponseEntity();
		}
	}

    @GetMapping("/get-content/{id}")
 	public ResponseEntity<?> getContent(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.getContent(currentUser, new SimpleObjectIdentifier(id), prototypeService).toResponseEntity();
 	}

    @PostMapping(value = "/new")
	public ResponseEntity<APIResponse> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("datasetId") ObjectId datasetId, @RequestPart("json") String json, @RequestPart("file") Optional<MultipartFile> file)  {
    	return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), json, file.orElse(null), prototypeService, datasetService).toResponseEntity();
	}   
    
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), prototypeService, datasetService).toResponseEntity();
	}
	
    @PostMapping(value = "/update/{id}")
	public ResponseEntity<?> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestPart("json") String json, @RequestPart("file") Optional<MultipartFile> file)  {
    	return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), json, file.orElse(null), prototypeService).toResponseEntity();
    }
    
}
