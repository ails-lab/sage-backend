package ac.software.semantic.controller;


import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.CreateEmbedderRequest;
import ac.software.semantic.payload.EmbedderDocumentResponse;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.EmbedderService;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.SimpleObjectIdentifier;
import ac.software.semantic.service.TaskConflictException;
import ac.software.semantic.service.TaskService;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Embedder API")

@RestController
@RequestMapping("/api/embedder")
public class APIEmbedderController {
    
	@Autowired
    private EmbedderService embedderService;

	@Autowired
	private TaskService taskService;

	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("database")
	private Database database;

	@Autowired
	private APIUtils apiUtils;
	
	@PostMapping(value = "/create")
	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestBody CreateEmbedderRequest req)  {

		try {
			EmbedderDocument edoc = embedderService.create(currentUser, req.getDatasetUri(), req.getEmbedder(), req.getVariant(), req.getIndexElement(), req.getOnClass(), req.getKeys());
			EmbedderContainer ec = embedderService.getContainer(currentUser, edoc);
			
			return APIResponse.created(ec);
		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}			
	}
	
	@GetMapping("/get/{id}")
	public ResponseEntity<?> get(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {

		EmbedderContainer ec = null;
		try {
	    	ec = embedderService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (ec == null) {
	    		return APIResponse.notFound(EmbedderContainer.class);
	    	} else {
	    		return APIResponse.retrieved(ec);
	    	}
	    	
		} catch (Exception ex) {
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}
	}
	
	@GetMapping(value = "/get-all-by-user")
	public ResponseEntity<?> getAllByUser(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<EmbedderDocumentResponse> docs = embedderService.getEmbedders(currentUser, datasetUri);
		
		return ResponseEntity.ok(docs);
	}

	@DeleteMapping(value = "/delete/{id}",
			produces = "application/json")
	public ResponseEntity<?> delete(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id) {

		EmbedderContainer ec = null;
		try {
	    	ec = embedderService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (ec == null) {
	    		return APIResponse.notFound(EmbedderContainer.class);
	    	}
		} catch (Exception ex) {
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}

		try {
			
			List<TaskType> ct  = Arrays.asList(new TaskType[] { TaskType.EMBEDDER_PUBLISH, TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_EXECUTE } );
			
	    	synchronized (ec.synchronizationString(ct)) {
	    		taskService.checkIfActiveEmbedderTask(ec, null, ct);
	    		
	    		ec.delete(); // what if this fails ?
	    
	    		return APIResponse.deleted(ec);
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	  
	}

	
	@PostMapping(value = "/stop/{id}")
	public ResponseEntity<?> stop(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {

		EmbedderContainer ec = null;
		try {
	    	ec = embedderService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (ec == null) {
	    		return APIResponse.notFound(EmbedderContainer.class);
	    	}
		} catch (Exception ex) {
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}
    	
		try {
	    	synchronized (ec.synchronizationString(TaskType.EMBEDDER_EXECUTE)) {
	    		TaskDescription td =  taskService.getActiveTask(ec, TaskType.EMBEDDER_EXECUTE);
	    		if (td != null) {
	    			if (taskService.requestStop(td.getId())) {
	    				return new ResponseEntity<>(APIResponse.SuccessResponse("The embedder is being stopped."), HttpStatus.ACCEPTED);
	    			} else {
	    				return new ResponseEntity<>(APIResponse.FailureResponse("The embedder could not be stopped."), HttpStatus.CONFLICT);
	    			}
	    		} else {
	    			
	    			EmbedderDocument edoc = embedderService.failExecution(ec);

	    			return new ResponseEntity<>(APIResponse.SuccessResponse("The embedder is not being executed.", 
	    					edoc != null ? ec.asResponse() : null),
	    					HttpStatus.OK);
	    		}
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	    	
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<?> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.execute(currentUser, new SimpleObjectIdentifier(id), embedderService);
	}
	
    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), embedderService);
 	}		

	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), embedderService);
	} 		
	
   	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
   		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), embedderService);
   	}		

    @GetMapping(value = "/preview-last-execution/{id}",
                produces = "text/plain")
	public ResponseEntity<?> previewLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), embedderService);
	}
    
    @GetMapping(value = "/preview-published-execution/{id}",
                produces = "text/plain")
	public ResponseEntity<?> previewPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), embedderService);
	}

    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), embedderService);
	}    
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), embedderService);
	}   

}
