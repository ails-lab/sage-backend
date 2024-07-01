package ac.software.semantic.controller;

import java.util.Arrays;

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
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.payload.request.EmbedderUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.EmbedderService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Embedder API")

@RestController
@RequestMapping("/api/embedder")
public class APIEmbedderController {
    
	@Autowired
    private EmbedderService embedderService;

	@Autowired
    private DatasetService datasetService;

	@Autowired
	private APIUtils apiUtils;

	@GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllByUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam ObjectId datasetId,
			                                        @RequestParam(required = false) Integer page,
				                                    @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, embedderService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
	
	@GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam ObjectId datasetId,
			                                        @RequestParam(required = false) Integer page,
				                                    @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, embedderService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}

	@PostMapping(value = "/new")
	public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,@RequestParam ObjectId datasetId,  @RequestBody EmbedderUpdateRequest ur)  {
		return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), ur, embedderService, datasetService).toResponseEntity();
	}

	@GetMapping(value = "/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), embedderService).toResponseEntity();
	}	
	
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), embedderService, datasetService).toResponseEntity();
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<APIResponse> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.execute(currentUser, new SimpleObjectIdentifier(id), embedderService).toResponseEntity();
	}

	@PostMapping(value = "/stop-execution/{id}")
	public ResponseEntity<APIResponse> stopExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.stopExecution(currentUser, new SimpleObjectIdentifier(id), embedderService).toResponseEntity();		
	}

    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), embedderService).toResponseEntity();
 	}		

	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), embedderService).toResponseEntity();
	} 		
	
   	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
   		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), embedderService).toResponseEntity();
   	}		

    @GetMapping(value = "/preview-last-execution/{id}")
	public ResponseEntity<?> previewLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
    	return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, embedderService);
	}
    
    @GetMapping(value = "/preview-published-execution/{id}")
	public ResponseEntity<?> previewPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, embedderService);
	}

    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), embedderService, FileType.zip);
	}    
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), embedderService);
	}   

}
