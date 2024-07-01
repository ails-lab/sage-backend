package ac.software.semantic.controller;

import io.swagger.v3.oas.annotations.Parameter;

import java.util.Arrays;

import javax.validation.constraints.NotNull;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.ClustererDocument;
import ac.software.semantic.model.constants.type.AnnotationValidationMode;
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.payload.request.ClustererUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.ClustererService;
import ac.software.semantic.service.ClustererService.ClustererContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.exception.ContainerNotFoundException;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Clusterer API")

@RestController
@RequestMapping("/api/clusterer")
public class APIClustererController {
    
	@Autowired
    private ClustererService clustererService;

	@Autowired
    private DatasetService datasetService;
	
	@Autowired
	private APIUtils apiUtils;

	@GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllByUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                           @RequestParam ObjectId datasetId,
			                           @RequestParam(required = false) Integer page,
	                                   @RequestParam(defaultValue = "${api.pagination.size}") int size)  {

		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, clustererService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
	
	@GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                           @RequestParam ObjectId datasetId,
			                           @RequestParam(required = false) Integer page,
	                                   @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, clustererService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}

	@PostMapping(value = "/new")
	public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true)@CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId, @RequestBody ClustererUpdateRequest ur)  {
		return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), ur, clustererService, datasetService).toResponseEntity();
	} 	
	
	@GetMapping(value = "/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();
	}	

	@PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody ClustererUpdateRequest ur)  {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, clustererService).toResponseEntity();
	}

	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), clustererService, datasetService).toResponseEntity();
	}
	
	@PostMapping(value = "/prepare/{id}")
	public ResponseEntity<APIResponse> prepare(@Parameter(hidden = true)@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.prepare(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();
	}
	
//    @GetMapping(value = "/preview/{id}", produces = "application/json")
//    public ResponseEntity<APIResponse> view(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(value="page", defaultValue="1") int page, @RequestParam(value="mode", defaultValue="ALL") AnnotationValidationMode mode)  {
//		try {
//			AnnotatorContainer ac = (AnnotatorContainer)apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), annotatorService);
//			
////			return APIResponse.result(annotatorService.view(ac, page)).toResponseEntity(); // Reloading for each page!!! Can this be avoided?
//			return APIResponse.result(annotatorService.view2(ac, page)).toResponseEntity(); // Reloading for each page!!! Can this be avoided?
//			
//		} catch (ContainerNotFoundException ex) {
//			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			return APIResponse.serverError(ex).toResponseEntity();
//		}			
//    } 
    
    @GetMapping(value = "/cluster-preview/{id}", produces = "application/json")
    public ResponseEntity<APIResponse> clusterview(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(value="page", defaultValue="1") int page)  {
		try {
			ClustererContainer ac = (ClustererContainer)apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), clustererService);
			
			return APIResponse.result(clustererService.clusterview(ac, page)).toResponseEntity(); // Reloading for each page!!! Can this be avoided?
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}			
    } 
    
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<APIResponse> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.execute(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();
	}
	
	@PostMapping(value = "/stop-execution/{id}")
	public ResponseEntity<APIResponse> stopExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.stopExecution(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();		
	}
	
	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();
	} 		
	
   	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
   		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();
   	}	
   	
   	@PostMapping(value = "/republish/{id}")
	public ResponseEntity<APIResponse> republish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
   		return apiUtils.republish(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();
   	}	
    
    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), clustererService).toResponseEntity();
 	}	
    
    @GetMapping(value = "/preview/{id}", produces = "application/json")
    public ResponseEntity<APIResponse> clusterview(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(value="page", defaultValue="1") int page, @RequestParam(value="mode", defaultValue="ALL") AnnotationValidationMode mode)  {
		try {
			ClustererContainer ac = (ClustererContainer)apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), clustererService);
			
			return APIResponse.result(clustererService.clusterview(ac, page)).toResponseEntity(); // Reloading for each page!!! Can this be avoided?
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}			
    } 
    
    @GetMapping(value = "/preview-last-execution/{id}")
	public ResponseEntity<?> previewLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
    	return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, clustererService);
	}
    
    @GetMapping(value = "/preview-published-execution/{id}")
	public ResponseEntity<?> previewPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, clustererService);
	}

    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), clustererService, FileType.zip);
	}
    
    @GetMapping(value = "/download-last-clustering/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastClustering(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam FileType fileType)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), clustererService, fileType);
	} 
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
       	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), clustererService);
	}   
	
    @GetMapping(value = "/exists-identifier")
	public ResponseEntity<APIResponse> existsIdentifier(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true) @NotNull @RequestParam String identifier, @RequestParam ObjectId datasetId)  {
		
    	try {
    		DatasetContainer dc = datasetService.getContainer(currentUser, datasetId);
	    			
    	    if (dc == null) {
    	    	return APIResponse.notFound(DatasetContainer.class).toResponseEntity();
    	    }
    	    	
    	    ClustererDocument doc = new ClustererDocument(dc.getObject());
	    	doc.setIdentifier(identifier);
	    	
	    	return apiUtils.identifierConflict(doc, IdentifierType.IDENTIFIER, clustererService).toResponseEntity();
		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}	
	
}
