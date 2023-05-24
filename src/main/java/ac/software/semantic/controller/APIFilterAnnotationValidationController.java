package ac.software.semantic.controller;

import java.util.Arrays;
import java.util.List;

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
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.FilterValidationUpdateRequest;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.FilterAnnotationValidationService;
import ac.software.semantic.service.SimpleObjectIdentifier;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.TaskConflictException;
import ac.software.semantic.service.TaskService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Filter Annotation Validation API")
@RestController
@RequestMapping("/api/filter-annotation-validation")
public class APIFilterAnnotationValidationController  {

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

	@Autowired
	private FilterAnnotationValidationService favService;

    @Autowired
	private TaskService taskService;
	
	@Autowired
	private APIUtils apiUtils;

	@PostMapping(value = "/create", produces = "application/json")
	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestParam("aegId") String aegId, @RequestBody FilterValidationUpdateRequest fur) {

		try {
			FilterAnnotationValidation fav = favService.create(currentUser, aegId, fur.getName(), fur.getFilters());
			FilterAnnotationValidationContainer favc = favService.getContainer(currentUser, fav);
			
			return APIResponse.created(favc);
		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}

	@PostMapping(value = "/update/{id}", produces = "application/json")
	public ResponseEntity<?> update(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestBody FilterValidationUpdateRequest fur) {

		FilterAnnotationValidationContainer favc = null;
		try {
			favc = favService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (favc == null) {
	    		return APIResponse.notFound(FilterAnnotationValidationContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			favService.update(favc, fur.getName(), fur.getFilters());
			
			return APIResponse.updated(favc);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
	
	@DeleteMapping(value = "/delete/{id}",
			produces = "application/json")
	public ResponseEntity<?> delete(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id) {
		
		SimpleObjectIdentifier objId = new SimpleObjectIdentifier(id);
		
		synchronized (FilterAnnotationValidationService.syncString(objId.toHexString())) {
			FilterAnnotationValidationContainer favc = null;
			try {
				favc = favService.getContainer(currentUser, objId);
		    	if (favc == null) {
		    		return APIResponse.notFound(FilterAnnotationValidationContainer.class);
		    	} else if (favc.isPublished()) {
		    		throw TaskConflictException.isPublished(favc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
				
				List<TaskType> ct  = Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE } );
				
		    	synchronized (favc.synchronizationString(ct)) {
		    		taskService.checkIfActiveFilterAnnotationValidationTask(favc, null, ct);
		    		
		    		favc.delete(); // what if this fails ?
		    
		    		return APIResponse.deleted(favc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}	   			
		}
	}

	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<APIResponse> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.execute(currentUser, new SimpleObjectIdentifier(id), favService);	
	}
	
    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), favService);
 	}	
	
	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), favService);
	}    
	
	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), favService);
	}     
    
	@GetMapping(value = "/preview-last-execution/{id}", 
			    produces = "text/plain")
	public ResponseEntity<?> previewLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), favService);
	}	
	
    @GetMapping(value = "/preview-published-execution/{id}", 
    		    produces = "text/plain")
	public ResponseEntity<?> previewPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), favService);
    }
	
    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), favService);
	}
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), favService);
	}

}