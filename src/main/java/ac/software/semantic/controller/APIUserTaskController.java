package ac.software.semantic.controller;

import java.util.Arrays;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.request.UserTaskUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.TaskSpecification;
import ac.software.semantic.service.UserTaskService;
import ac.software.semantic.service.UserTaskService.UserTaskContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Tasks API")
@RestController
@RequestMapping("/api/user-tasks")
public class APIUserTaskController {

	Logger logger = LoggerFactory.getLogger(APIUserTaskController.class);

	@Autowired
	private TaskService taskService;
	
	@Autowired
	private UserTaskService userTaskService;

	@Autowired
	private DatasetService datasetService;

	@Autowired
	private APIUtils apiUtils;

    @GetMapping("/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
		                                      @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, userTaskService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
    
    @GetMapping("/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
		                                      @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, userTaskService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
    
	@GetMapping(value = "/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), userTaskService).toResponseEntity();
	}

    @PostMapping(value = "/new")
    public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId, @RequestBody UserTaskUpdateRequest ur)  {
    	return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), ur, userTaskService, datasetService).toResponseEntity();
	}
    
	@PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody UserTaskUpdateRequest ur) {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, userTaskService).toResponseEntity();
	}
	
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), userTaskService, datasetService).toResponseEntity();
	}
	
	@PostMapping(value = "/run/{id}")
	public ResponseEntity<APIResponse> run(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {

		synchronized (userTaskService.synchronizedString(id.toHexString())) {
			UserTaskContainer uc = null;
			try {
		    	uc = userTaskService.getContainer(currentUser, new SimpleObjectIdentifier(id));
		    	if (uc == null) {
		    		return APIResponse.notFound(UserTaskContainer.class).toResponseEntity();
		    	}
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest().toResponseEntity();
			}
			
			try {
				TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.USER_TASK_DATASET_RUN).createTask(uc, uc.buildPossiblyRelevantProperties());
	
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.SuccessResponse("The user task has been scheduled for execution.", HttpStatus.ACCEPTED).toResponseEntity();
		    	} else {
		    		return APIResponse.serverError().toResponseEntity();
		    	}
		    	
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex).toResponseEntity();
			}
		}		
	}
	
	@PostMapping(value = "/schedule/{id}")
	public ResponseEntity<APIResponse> schedule(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {

		synchronized (userTaskService.synchronizedString(id.toHexString())) {
			UserTaskContainer uc = null;
			try {
		    	uc = userTaskService.getContainer(currentUser, new SimpleObjectIdentifier(id));
		    	if (uc == null) {
		    		return APIResponse.notFound(UserTaskContainer.class).toResponseEntity();
		    	}
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest().toResponseEntity();
			}
			
			try {
				uc.schedule();
				
				return APIResponse.updated(uc).toResponseEntity();
		    	
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex).toResponseEntity();
			}
		}		
	}
	
	@PostMapping(value = "/unschedule/{id}")
	public ResponseEntity<APIResponse> unschedule(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {

		synchronized (userTaskService.synchronizedString(id.toHexString())) {
			UserTaskContainer uc = null;
			try {
		    	uc = userTaskService.getContainer(currentUser, new SimpleObjectIdentifier(id));
		    	if (uc == null) {
		    		return APIResponse.notFound(UserTaskContainer.class).toResponseEntity();
		    	}
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest().toResponseEntity();
			}
			
			try {
				uc.unschedule();
				
				return APIResponse.updated(uc).toResponseEntity();
				
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex).toResponseEntity();
			}
		}			
	}

	@GetMapping(value = "/validate-cron-expression")
	public ResponseEntity<APIResponse> validate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam String expression) {
		try {
			new CronTrigger(expression);
			return APIResponse.ok().toResponseEntity();
		} catch (Exception ex) {
			return APIResponse.invalidCronExpression().toResponseEntity();
		}
	}


}
