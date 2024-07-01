package ac.software.semantic.controller;

import io.swagger.v3.oas.annotations.Parameter;

import java.util.Arrays;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.payload.request.DistributionUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.DistributionService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Index API")

@RestController
@RequestMapping("/api/distribution")
public class APIDistributionController {

	@Autowired
    @Qualifier("database")
    private Database database;
	
    @Autowired
    private DistributionService distributionService;

    @Autowired
    private DatasetService datasetService;
    
	@Autowired
	private APIUtils apiUtils;

    @PostMapping(value = "/new")
	public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId, @RequestBody DistributionUpdateRequest ur)  {
    	return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), ur, distributionService, datasetService).toResponseEntity();
	}
    
	@PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody DistributionUpdateRequest ur) {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, distributionService).toResponseEntity();
	}    
    
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), distributionService, datasetService).toResponseEntity();
	}
    
    @PostMapping(value = "/create/{id}")
	public ResponseEntity<APIResponse> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.create(currentUser, new SimpleObjectIdentifier(id), distributionService, datasetService).toResponseEntity();
 	}	
    
	@PostMapping(value = "/stop-create/{id}")
	public ResponseEntity<APIResponse> stop(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.stopCreate(currentUser, new SimpleObjectIdentifier(id), distributionService).toResponseEntity();
	}    
	
	@PostMapping(value = "/destroy/{id}")
	public ResponseEntity<APIResponse> destroy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.destroy(currentUser, new SimpleObjectIdentifier(id), distributionService).toResponseEntity();
	} 

    @GetMapping("/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
   		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, distributionService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
    
    @GetMapping("/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    
   		return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, distributionService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
    
    @GetMapping(value = "/exists-identifier",
                produces = "application/json")
	public ResponseEntity<APIResponse> existsIdentifier(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true)  @RequestParam ObjectId datasetId, @RequestParam String identifier)  {
		
    	DistributionDocument ur = new DistributionDocument(database);
    	ur.setDatasetId(datasetId);
    	ur.setIdentifier(identifier);
    	
    	return apiUtils.identifierConflict(ur, IdentifierType.IDENTIFIER, distributionService).toResponseEntity();
	}
}
