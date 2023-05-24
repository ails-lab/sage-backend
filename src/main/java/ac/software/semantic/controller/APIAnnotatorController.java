package ac.software.semantic.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import io.swagger.v3.oas.annotations.Parameter;
import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.constants.AnnotatorPrepareStatus;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.AnnotatorDocumentResponse;
import ac.software.semantic.payload.CreateAnnotatorRequest;
import ac.software.semantic.payload.DataServiceResponse;
import ac.software.semantic.payload.UpdateAnnotatorRequest;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotatorService;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.SimpleObjectIdentifier;
import ac.software.semantic.service.TaskConflictException;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.TaskSpecification;
import ac.software.semantic.vocs.SEMRVocabulary;

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Annotator API")

@RestController
@RequestMapping("/api/annotator")
public class APIAnnotatorController {
    
	@Autowired
    private AnnotatorService annotatorService;
	
	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private AnnotationEditGroupRepository annotationEditGroupRepository;

	@Autowired
	private DataServiceRepository dataServiceRepository;

	@Autowired
	DatasetRepository datasetRepository;
	
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private TaskService taskService;

	@Autowired
	AnnotatorDocumentRepository annotatorRepository;

	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("database")
	private Database database;

    @Autowired
    @Qualifier("preprocess-functions")
    private Map<Resource, List<String>> functions;
    
    @Autowired
    @Qualifier("preprocess-operations")
    private Map<Resource, List<String>> operations;
        
	@Autowired
	private APIUtils apiUtils;

	@GetMapping(value = "/services")
	public ResponseEntity<?> services(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		List<DataService> services = dataServiceRepository.findByDatabaseIdAndType(database.getId(), DataServiceType.ANNOTATOR);
        List<DataServiceResponse> response = services.stream()
        		.map(doc -> modelMapper.dataService2DataServiceResponse(doc))
        		.collect(Collectors.toList());

		return ResponseEntity.ok(response);
	}
	
	@GetMapping(value = "/vocabularies")
	public ResponseEntity<?> vocabularies(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		List<DataService> services = dataServiceRepository.findByDatabaseIdAndType(database.getId(), DataServiceType.ANNOTATOR);
        List<DataServiceResponse> response = services.stream()
        		.map(doc -> modelMapper.dataService2DataServiceResponse(doc))
        		.collect(Collectors.toList());

		return ResponseEntity.ok(response);
	}
	
	@GetMapping(value = "/functions")
	public ResponseEntity<?> functions(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		
		ObjectMapper mapper = new ObjectMapper();

		ArrayNode farray = mapper.createArrayNode();
		for (Entry<Resource, List<String>> entry : functions.entrySet()) {
			ObjectNode object = mapper.createObjectNode();
			object.put("uri", entry.getKey().toString());
			
			List<String> ps = entry.getValue();
			if (ps.size() == 0) {
				continue;
			}
			
			ArrayNode params = mapper.createArrayNode();
			
			for (String p : entry.getValue()) {
				params.add(p);
			}
			object.put("parameters", params);
			farray.add(object);
		}
		
		ArrayNode oarray = mapper.createArrayNode();
		for (Entry<Resource, List<String>> entry : operations.entrySet()) {
			ObjectNode object = mapper.createObjectNode();
			object.put("uri", entry.getKey().toString());
			
			List<String> ps = entry.getValue();
			if (ps.size() == 0) {
				continue;
			}
			
			ArrayNode params = mapper.createArrayNode();
			
			for (String p : entry.getValue()) {
				params.add(p);
			}
			object.put("parameters", params);
			oarray.add(object);
		}

		ObjectNode object = mapper.createObjectNode();
		object.put("functions", farray);
		object.put("operations", oarray);
		
		return ResponseEntity.ok(object);
	}
	
	@PostMapping(value = "/create")
	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestBody CreateAnnotatorRequest req)  {

		AnnotatorDocument adoc = annotatorService.createAnnotator(currentUser, req.getDatasetUri(), req.getOnPath(), req.getAsProperty(), req.getAnnotator(), req.getThesaurus(), req.getParameters(), req.getPreprocess(), req.getVariant(), req.getDefaultTarget());
		
		AnnotationEditGroup aeg = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(resourceVocabulary.getUuidFromResourceUri(req.getDatasetUri()), PathElement.onPathElementListAsStringList(req.getOnPath()), req.getAsProperty(), new ObjectId(currentUser.getId())).get();
		
		Optional<Dataset> datasetOpt = datasetRepository.findByUuid(adoc.getDatasetUuid());

		if (!datasetOpt.isPresent()) {
			return ResponseEntity.notFound().build();
		}
		
		Dataset dataset = datasetOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
		
		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;
		
		return ResponseEntity.ok(modelMapper.annotator2AnnotatorResponse(vc, adoc, aeg));
		
	} 
	
	@GetMapping("/get/{id}")
	public ResponseEntity<?> get(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {

		AnnotatorContainer ac = null;
		try {
	    	ac = annotatorService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (ac == null) {
	    		return APIResponse.notFound(AnnotatorContainer.class);
	    	} else {
	    		return ResponseEntity.ok(ac.asResponse());
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}
		
	}
	
	@GetMapping(value = "/get-all-by-user")
	public ResponseEntity<?> getAllByUser(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotatorDocumentResponse> docs = annotatorService.getAnnotators(currentUser, datasetUri);
		
		return ResponseEntity.ok(docs);
		
	}

	@GetMapping(value = "/get-all")
	public ResponseEntity<?> getAll(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotatorDocumentResponse> docs = annotatorService.getAnnotators(datasetUri);

		return ResponseEntity.ok(docs);

	}


	@PutMapping("/update/{id}")
	public ResponseEntity<?> update(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody UpdateAnnotatorRequest params)  {
		try {
			AnnotatorDocument updatedAnnotator = annotatorService.updateAnnotator(currentUser, id, params);
			if (updatedAnnotator != null) {
				return ResponseEntity.ok(updatedAnnotator);
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@DeleteMapping(value = "/delete/{id}",
			produces = "application/json")
	public ResponseEntity<?> delete(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id) {
		
		SimpleObjectIdentifier objId = new SimpleObjectIdentifier(id);
		
		synchronized (AnnotatorService.syncString(objId.toHexString())) {
			AnnotatorContainer ac = null;
			try {
		    	ac = annotatorService.getContainer(currentUser, objId);
		    	if (ac == null) {
		    		return APIResponse.notFound(AnnotatorContainer.class);
		    	} else if (ac.isPublished()) {
		    		throw TaskConflictException.isPublished(ac);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
				
				List<TaskType> ct  = Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, TaskType.EMBEDDER_PUBLISH, TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_REPUBLISH } ); 
		    	
				synchronized (ac.synchronizationString(ct)) {
		    		taskService.checkIfActiveAnnotatorTask(ac, null, ct);
		    		
		    		ac.delete(); // what if this fails ?
		    
		    		return APIResponse.deleted(ac);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}	   			
		}
	}
	
	@PostMapping(value = "/prepare/{id}")
	public ResponseEntity<?> prepare(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {

		AnnotatorContainer ac = null;
		try {
	    	ac = annotatorService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (ac == null) {
	    		return APIResponse.notFound(AnnotatorContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
    	
		try {
			AnnotatorPrepareStatus status = annotatorService.prepareAnnotator(ac, false);
			
			if (status == AnnotatorPrepareStatus.PREPARED) {
  				return new ResponseEntity<>(APIResponse.SuccessResponse("The annotator is ready."), HttpStatus.OK);
			} else if (status == AnnotatorPrepareStatus.PREPARING) {
				return new ResponseEntity<>(APIResponse.SuccessResponse("The annotator is being prepared."), HttpStatus.OK);
			} else if (status == AnnotatorPrepareStatus.NOT_PREPARED) {
				return new ResponseEntity<>(APIResponse.SuccessResponse("The annotator is not ready and not being prepared."), HttpStatus.OK);
			} else {
	    		return new ResponseEntity<>(APIResponse.FailureResponse("Unknown annotator status."), HttpStatus.CONFLICT);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	    	
	}
	
    @GetMapping(value = "/preview/{id}", produces = "application/json")
    public ResponseEntity<?> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam(value="page", defaultValue="1") int page, @RequestParam(value="mode", defaultValue="ALL") AnnotationValidationRequest mode)  {

		try {
			org.apache.jena.query.Dataset rdfDataset = annotatorService.load(currentUser, id);
			
			AnnotatorDocument doc = annotatorRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId())).get();

			ValueResponseContainer<ValueAnnotation> res = annotatorService.view(currentUser, doc, rdfDataset, page);

			return ResponseEntity.ok(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}			
    } 
    
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<?> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {

		AnnotatorContainer ac = null;
		try {
	    	ac = annotatorService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (ac == null) {
	    		return APIResponse.notFound(AnnotatorContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			AnnotatorPrepareStatus status = annotatorService.prepareAnnotator(ac, true);
			
			if (status == AnnotatorPrepareStatus.PREPARING) {
				return new ResponseEntity<>(APIResponse.FailureResponse("The annotator is being prepared."), HttpStatus.CONFLICT);
			} else if (status == AnnotatorPrepareStatus.NOT_PREPARED) {
				return new ResponseEntity<>(APIResponse.FailureResponse("The annotator is not ready and not being prepared."), HttpStatus.CONFLICT);
			} else if (status == AnnotatorPrepareStatus.UNKNOWN) {
	    		return new ResponseEntity<>(APIResponse.FailureResponse("Unknown annotator status."), HttpStatus.CONFLICT);
	    	}
			
			TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.ANNOTATOR_EXECUTE).createTask(ac);

	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return new ResponseEntity<>(APIResponse.SuccessResponse("The annotator has been scheduled for execution."), HttpStatus.ACCEPTED);
	    	} else {
	    		return new ResponseEntity<>(APIResponse.FailureResponse("Server error."), HttpStatus.INTERNAL_SERVER_ERROR);
	    	}
	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
		
	}
	
	@PostMapping(value = "/stop/{id}")
	public ResponseEntity<?> stop(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {

		AnnotatorContainer ac = null;
		try {
	    	ac = annotatorService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (ac == null) {
	    		return APIResponse.notFound(AnnotatorContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
    	
		try {
	    	synchronized (ac.synchronizationString(TaskType.ANNOTATOR_EXECUTE)) {
	    		TaskDescription td =  taskService.getActiveTask(ac, TaskType.ANNOTATOR_EXECUTE);
	    		if (td != null) {
	    			if (taskService.requestStop(td.getId())) {
	    				return new ResponseEntity<>(APIResponse.SuccessResponse("The annotator is being stopped."), HttpStatus.ACCEPTED);
	    			} else {
	    				return new ResponseEntity<>(APIResponse.FailureResponse("The annotator could not be stopped."), HttpStatus.CONFLICT);
	    			}
	    		} else {
	    			
	    			AnnotatorDocument adoc = annotatorService.failExecution(ac);

	    			return new ResponseEntity<>(APIResponse.SuccessResponse("The annotator is not being executed.", 
	    					adoc != null ? ac.asResponse() : null),
	    					HttpStatus.OK);
	    		}
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	    	
	}
	
	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		
		SimpleObjectIdentifier objId = new SimpleObjectIdentifier(id);
		
		synchronized (AnnotatorService.syncString(objId.toHexString())) {
			AnnotatorContainer ac = null;
			try {
		    	ac = annotatorService.getContainer(currentUser, objId);
		    	if (ac == null) {
		    		return APIResponse.notFound(AnnotatorContainer.class);
		    	} else if (ac.isPublished()) {
		    		throw TaskConflictException.alreadyPublished(ac);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
		
			try {
				
		    	TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.ANNOTATOR_PUBLISH).createTask(ac);
				
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.acceptedToPublish(ac);
		    	} else {
		    		return new ResponseEntity<>(APIResponse.FailureResponse("Server error."), HttpStatus.INTERNAL_SERVER_ERROR);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);			
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
		}
	} 		
	
   	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
	
   		SimpleObjectIdentifier objId = new SimpleObjectIdentifier(id);
   		
   		synchronized (AnnotatorService.syncString(objId.toHexString())) {
			AnnotatorContainer ac = null;
			try {
		    	ac = annotatorService.getContainer(currentUser, objId);
		    	if (ac == null) {
		    		return APIResponse.notFound(AnnotatorContainer.class);
		    	} else if (!ac.isPublished()) {
		    		throw TaskConflictException.notPublished(ac);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
				TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.ANNOTATOR_UNPUBLISH).createTask(ac);
		
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.acceptedToUnpublish(ac);
		    	} else {
		    		return new ResponseEntity<>(APIResponse.FailureResponse("Server error."), HttpStatus.INTERNAL_SERVER_ERROR);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
   		}
   	}	
    
    @PostMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<APIResponse> clearExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.clearExecution(currentUser, new SimpleObjectIdentifier(id), annotatorService);
 	}	
    
    @GetMapping(value = "/preview-last-execution/{id}",
                produces = "text/plain")
	public ResponseEntity<?> previewLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.previewLastExecution(currentUser, new SimpleObjectIdentifier(id), annotatorService);
	}
    
    @GetMapping(value = "/preview-published-execution/{id}",
                produces = "text/plain")
	public ResponseEntity<?> previewPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.previewPublishedExecution(currentUser, new SimpleObjectIdentifier(id), annotatorService);
	}

    @GetMapping(value = "/download-last-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
    	return apiUtils.downloadLastExecution(currentUser, new SimpleObjectIdentifier(id), annotatorService);
	}    
    
    @GetMapping(value = "/download-published-execution/{id}")
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
       	return apiUtils.downloadPublishedExecution(currentUser, new SimpleObjectIdentifier(id), annotatorService);
	}   
	
	
	
}
