package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import ac.software.semantic.service.PagedAnnotationValidationPageLocksService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.type.AnnotationValidationMode;
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.RefractoredAnnotationEditDetails;
import ac.software.semantic.payload.request.PagedAnnotationValidationUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.AnnotationEditResponse;
import ac.software.semantic.payload.response.ErrorResponse;
import ac.software.semantic.payload.response.PagedAnnotationValidatationDataResponse;
import ac.software.semantic.payload.response.RefractoredAnnotationEditResponse;
import ac.software.semantic.payload.response.modifier.PagedAnnotationValidationResponseModifier;
import ac.software.semantic.payload.response.modifier.ResponseModifier;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationPageRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService;
import ac.software.semantic.service.AnnotationEditService;
import ac.software.semantic.service.PagedAnnotationValidationService;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.TaskSpecification;
import ac.software.semantic.service.VocabularyService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Paged Annotation Validation API")
@RestController
@RequestMapping("/api/paged-annotation-validation")
public class APIPagedAnnotationValidationController {

	@Autowired
	private PagedAnnotationValidationPageRepository pavpRepository;

	@Autowired
	private AnnotationEditGroupRepository aegRepository;

	@Autowired
	private PagedAnnotationValidationService pavService;

	@Autowired
	private AnnotationEditService annotationEditService;

	@Autowired
	private AnnotationEditGroupService aegService;

	@Autowired
	private PagedAnnotationValidationPageLocksService locksService;
	
	@Autowired
	private TaskService taskService;

	@Autowired
	private VocabularyService vocabularyService;

	@Autowired
	private APIUtils apiUtils;

	private Map<String, String> commitPageMutex = Collections.synchronizedMap(new HashMap<>());
	
	public enum PageRequestMode {
		UNANNOTATED_ONLY_SPECIFIC_PAGE,
		UNANNOTATED_ONLY_SERIAL,
		ANNOTATED_ONLY_SERIAL,
		ANNOTATED_ONLY_SPECIFIC_PAGE,
		ANNOTATED_ONLY_NOT_COMPLETE,
		ANNOTATED_ONLY_NOT_VALIDATED
	}

	public enum NavigatePageMode {
		RIGHT,
		LEFT
	}

	@PostMapping(value = "/new", produces = "application/json")
	public ResponseEntity<APIResponse> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId aegId, @RequestBody PagedAnnotationValidationUpdateRequest ur) {
		return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(aegId), ur, pavService, aegService).toResponseEntity();
	}
	
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), pavService, aegService).toResponseEntity();
	}

	@GetMapping(value = "/get/{id}", produces = "application/json")
	public ResponseEntity<APIResponse> progress(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam(required = false, defaultValue = "full") List<String> details) {
		return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), pavService, ResponseModifier.createModifier(PagedAnnotationValidationResponseModifier.class, details)).toResponseEntity();
	}

	@Operation(summary = "Request a page to perform validation.", security = @SecurityRequirement(name = "bearerAuth"))
	@GetMapping(value = "/view/{id}", produces = "application/json")
	public ResponseEntity<?> view(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam(required = false) Integer currentPage, @RequestParam PageRequestMode mode,@RequestParam(required = false) NavigatePageMode navigation, @RequestParam(required = false) Integer requestedPage) {
		PagedAnnotationValidatationDataResponse res;

		if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SERIAL) ||
			mode.equals(PageRequestMode.ANNOTATED_ONLY_SERIAL) ||
			mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED) ||
			mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)){
			res = pavService.determinePageSerial(currentUser, id, currentPage, mode, navigation);
		}
		else if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE) || mode.equals(PageRequestMode.ANNOTATED_ONLY_SPECIFIC_PAGE)) {
			res = pavService.getSpecificPage(currentUser, id, requestedPage, mode, currentPage);
		}
		else {
			res = null;
		}

		return ResponseEntity.ok(res);
	}

	@PostMapping(value = "/commit-page/{pavpid}", produces = "application/json")
	public ResponseEntity<?> commitPage(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("pavpid") String pavpId, @RequestParam String lockId, @RequestBody List<RefractoredAnnotationEditResponse> edits) {

		if (commitPageMutex.putIfAbsent(pavpId, pavpId) == null) { 
			
			try {
				Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findById(pavpId);
				if (!pavpOpt.isPresent()) {
					return new ResponseEntity<>(HttpStatus.NOT_FOUND);
				}
		
				PagedAnnotationValidationPage pavp = pavpOpt.get();
		
				if(!locksService.checkForLock(currentUser, pavp.getPagedAnnotationValidationId().toString(), lockId, pavp.getMode(), pavp.getPage())) {
					commitPageMutex.remove(pavpId);
					return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
				}
		
				Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(pavp.getAnnotationEditGroupId());
				if (!aegOpt.isPresent()) {
					return new ResponseEntity<>(HttpStatus.NOT_FOUND);
				}
		
				AnnotationEditResponse tmp;
				List<AnnotationEditResponse> mappedList = new ArrayList<>();
				for (RefractoredAnnotationEditResponse res : edits) {
					for (RefractoredAnnotationEditDetails det : res.getEdits()) {
						tmp = new AnnotationEditResponse(det, res.getPropertyValue());
						mappedList.add(tmp);
					}
				}
		
				for (AnnotationEditResponse vad : mappedList) {
					annotationEditService.processEdit(currentUser, aegOpt.get(), vad, pavp);
				}
	
				pavpRepository.save(pavp);
			
				return new ResponseEntity<>(HttpStatus.OK);
			
			} catch (Exception ex) {
				ex.printStackTrace();
				
				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

			} finally {
				commitPageMutex.remove(pavpId);
			}
		
		} else {
			
			return new ResponseEntity<>(HttpStatus.CONFLICT);
		}
	}

    @PostMapping(value = "/stop/{id}")
 	public ResponseEntity<APIResponse> stopValidation(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
 		
		PagedAnnotationValidationContainer pavc = null;
		try {
			pavc = pavService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (pavc == null) {
	    		return APIResponse.notFound(PagedAnnotationValidationContainer.class).toResponseEntity();
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest().toResponseEntity();
		}
		
		try {
			List<TaskType> ct  = Arrays.asList(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME } );
			
			synchronized (pavc.synchronizationString()) {
				pavc.checkIfActiveTask(null, ct);
				
				pavService.stopValidation(pavc);

				return APIResponse.SuccessResponse("The paged annotation validation has been stopped.", pavc.asResponse(), HttpStatus.OK).toResponseEntity(); 
			}			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
 	}
	
	@PostMapping(value = "/resume/{id}", produces = "application/json")
	public ResponseEntity<APIResponse> resumeValidation(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id) {
		
		PagedAnnotationValidationContainer pavc = null;
		try {
			pavc = pavService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (pavc == null) {
	    		return APIResponse.notFound(PagedAnnotationValidationContainer.class).toResponseEntity();
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest().toResponseEntity();
		}
		
		try {
			
			TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.PAGED_ANNOTATION_VALIDATION_RESUME).createTask(pavc);
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return APIResponse.SuccessResponse("The paged annotation validation has been scheduled for resuming.", HttpStatus.ACCEPTED).toResponseEntity();
	    	} else {
	    		return APIResponse.serverError().toResponseEntity();
	    	}
		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	
    	
	}
	
    @GetMapping(value = "/create-lock/{id}")
    public ResponseEntity<?> lock(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam int page, @RequestParam AnnotationValidationMode mode) {
        try {
            ObjectId result = locksService.obtainLock(currentUser.getId(), id, page, mode);
            if (result != null) {
                return ResponseEntity.ok(result.toString());
            }
            else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return APIResponse.serverError(ex).toResponseEntity();
        }
    }

    @PostMapping(value = "/remove-lock/{id}")
    public ResponseEntity<?> removeLock(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam AnnotationValidationMode mode, @RequestParam int page) {
        try {
            if(locksService.removeLock(currentUser.getId(), id, page, mode)) {
                return ResponseEntity.status(HttpStatus.OK).body(new ErrorResponse("Lock successfully removed"));
            }
            else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Lock did not exist"));
            }
        } catch(Exception ex) {
            ex.printStackTrace();
            return APIResponse.serverError(ex).toResponseEntity();
        }
    }
	
	@PostMapping(value = "/update/{id}", produces = "application/json")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestBody PagedAnnotationValidationUpdateRequest ur) {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, pavService).toResponseEntity();
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<APIResponse> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.execute(currentUser, new SimpleObjectIdentifier(id), pavService).toResponseEntity();
	}
	
	@PostMapping(value = "/stop-execution/{id}")
	public ResponseEntity<APIResponse> stopExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.stopExecution(currentUser, new SimpleObjectIdentifier(id), pavService).toResponseEntity();		
	}
	
    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), pavService).toResponseEntity();
 	}	
    
	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), pavService).toResponseEntity();
	}    
	
	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), pavService).toResponseEntity();
	}     
    
	@GetMapping(value = "/preview-last-execution/{id}")
	public ResponseEntity<?> previewLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
		return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, pavService);
	}	
	
    @GetMapping(value = "/preview-published-execution/{id}")
	public ResponseEntity<?> previewPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), shard, offset, pavService);
    }
	
    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), pavService, FileType.zip);
	}
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), pavService);
	}   
    
	@GetMapping(value = "/vocabularies/{id}/resolve", produces = "application/json")
	public ResponseEntity<APIResponse> resolve(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam String resource) {

		try {
			PagedAnnotationValidationContainer pavc = pavService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (pavc == null) {
	    		return APIResponse.notFound(PagedAnnotationValidationContainer.class).toResponseEntity();
	    	}
	    	
	    	List<Object> res = vocabularyService.getLabel(resource, pavc.getResourceContext(resource), false);
	    	
	    	return APIResponse.result(res).toResponseEntity();
	    	
	    } catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}

}