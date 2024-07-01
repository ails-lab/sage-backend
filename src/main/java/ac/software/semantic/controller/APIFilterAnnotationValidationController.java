package ac.software.semantic.controller;

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
import ac.software.semantic.payload.request.FilterAnnotationValidationUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService;
import ac.software.semantic.service.FilterAnnotationValidationService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Filter Annotation Validation API")
@RestController
@RequestMapping("/api/filter-annotation-validation")
public class APIFilterAnnotationValidationController  {

	@Autowired
	private FilterAnnotationValidationService favService;

	@Autowired
	private AnnotationEditGroupService aegService;

	@Autowired
	private APIUtils apiUtils;

	@PostMapping(value = "/new", produces = "application/json")
	public ResponseEntity<?> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId aegId, @RequestBody FilterAnnotationValidationUpdateRequest ur) {
		return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(aegId), ur, favService, aegService).toResponseEntity();
	}

	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), favService, aegService).toResponseEntity();
	}
	
	@PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody FilterAnnotationValidationUpdateRequest ur) {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, favService).toResponseEntity();
	}

	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<APIResponse> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.execute(currentUser, new SimpleObjectIdentifier(id), favService).toResponseEntity();	
	}

	@PostMapping(value = "/stop-execution/{id}")
	public ResponseEntity<APIResponse> stopExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.stopExecution(currentUser, new SimpleObjectIdentifier(id), favService).toResponseEntity();		
	}
	
    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), favService).toResponseEntity();
 	}	
	
	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), favService).toResponseEntity();
	}    
	
	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), favService).toResponseEntity();
	}     
    
	@GetMapping(value = "/preview-last-execution/{id}")
	public ResponseEntity<?> previewLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
		return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, favService);
	}	
	
    @GetMapping(value = "/preview-published-execution/{id}")
	public ResponseEntity<?> previewPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, favService);
    }
	
    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), favService, FileType.zip);
	}
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), favService);
	}

}