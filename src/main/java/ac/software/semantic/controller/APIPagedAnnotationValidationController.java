package ac.software.semantic.controller;

import java.util.ArrayList;
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
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.AnnotationEditResponse;
import ac.software.semantic.payload.DatasetProgressResponse;
import ac.software.semantic.payload.PagedAnnotationValidatationDataResponse;
import ac.software.semantic.payload.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.ProgressResponse;
import ac.software.semantic.payload.RefractoredAnnotationEditDetails;
import ac.software.semantic.payload.RefractoredAnnotationEditResponse;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.PagedAnnotationValidationPageRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditService;
import ac.software.semantic.service.PagedAnnotationValidationService;
import ac.software.semantic.service.SimpleObjectIdentifier;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.TaskService;
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
	private PagedAnnotationValidationPageLocksService locksService;
	
	@Autowired
	private TaskService taskService;
	
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

	@PostMapping(value = "/create", produces = "application/json")
	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestParam("aegId") String aegId, @RequestBody PagedAnnotationValidationResponse body) {

		try {
			PagedAnnotationValidation pav = pavService.create(currentUser, aegId, body.getName(), body.getMode());
			PagedAnnotationValidationContainer pavc = pavService.getContainer(currentUser, pav);
			
			return APIResponse.created(pavc);
		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
		
	}
	
	@PostMapping(value = "/update/{id}", produces = "application/json")
	public ResponseEntity<?> update(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestBody PagedAnnotationValidationResponse body) {
		
		PagedAnnotationValidationContainer pavc = null;
		try {
			pavc = pavService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (pavc == null) {
	    		return APIResponse.notFound(PagedAnnotationValidationContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			pavService.updatePagedAnnotationValidation(pavc, body.getName(), body.getMode());
			
			return APIResponse.updated(pavc);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}

	@Operation(summary = "Get progress on validation procedure", security = @SecurityRequirement(name = "bearerAuth"))
	@GetMapping(value = "/progress/{id}", produces = "application/json")
	public ResponseEntity<?> progress(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id) {
		ProgressResponse progressResponse = pavService.getProgress(currentUser, id);
		if (progressResponse != null) {
			return ResponseEntity.status(HttpStatus.OK).body(progressResponse);
		}
		else {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}

	@Operation(summary = "Get progress of validation on all dataset properties", security = @SecurityRequirement(name = "bearerAuth"))
	@GetMapping(value = "/dataset-progress/{uuid}", produces = "application/json")
	public ResponseEntity<?> datasetProgress(@CurrentUser UserPrincipal currentUser, @PathVariable("uuid") String uuid) {
		List<DatasetProgressResponse> res = pavService.getDatasetProgress(currentUser, uuid);
		return ResponseEntity.ok(res);
	}

	@Operation(summary = "Request a page to perform validation.", security = @SecurityRequirement(name = "bearerAuth"))
	@GetMapping(value = "/view/{id}", produces = "application/json")
	public ResponseEntity<?> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(required = false) Integer currentPage, @RequestParam PageRequestMode mode,@RequestParam(required = false) NavigatePageMode navigation, @RequestParam(required = false) Integer requestedPage) {
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
 	public ResponseEntity<?> stopValidation(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
 		
		PagedAnnotationValidationContainer pavc = null;
		try {
			pavc = pavService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (pavc == null) {
	    		return APIResponse.notFound(PagedAnnotationValidationContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			synchronized (pavc.synchronizationString(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME)) {
				taskService.checkIfActivePagedAnnotationValidationTask(pavc, null);
				
				pavService.stopValidation(pavc);

				return new ResponseEntity<>(APIResponse.SuccessResponse("The paged annotation validation has been stopped.", pavc.asResponse()), HttpStatus.OK); 
			}			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
 	}
	
	@PostMapping(value = "/resume/{id}", produces = "application/json")
	public ResponseEntity<?> resumeValidation(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id) {
		
		PagedAnnotationValidationContainer pavc = null;
		try {
			pavc = pavService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (pavc == null) {
	    		return APIResponse.notFound(PagedAnnotationValidationContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			TaskDescription tdescr = taskService.newPagedAnnotationValidationResumeTask(pavc);
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return new ResponseEntity<>(APIResponse.SuccessResponse("The paged annotation validation has been scheduled for resuming."), HttpStatus.ACCEPTED);
	    	} else {
	    		return new ResponseEntity<>(APIResponse.FailureResponse("Server error."), HttpStatus.INTERNAL_SERVER_ERROR);
	    	}
		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	
    	
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<APIResponse> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.execute(currentUser, new SimpleObjectIdentifier(id), pavService);
	}
	
    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), pavService);
 	}	
    
	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), pavService);
	}    
	
	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), pavService);
	}     
    
	@GetMapping(value = "/preview-last-execution/{id}", 
			    produces = "text/plain")
	public ResponseEntity<?> previewLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), pavService);
	}	
	
    @GetMapping(value = "/preview-published-execution/{id}", 
    		    produces = "text/plain")
	public ResponseEntity<?> previewPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), pavService);
    }
	
    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), pavService);
	}
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), pavService);
	}   

}