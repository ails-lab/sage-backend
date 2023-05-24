package ac.software.semantic.controller;

import java.io.StringWriter;
import java.io.Writer;
import java.util.*;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.Template;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.constants.ThesaurusLoadStatus;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.ClassStructureResponse;
import ac.software.semantic.payload.CreateDatasetDistributionRequest;
import ac.software.semantic.payload.CreateIndexRequest;
import ac.software.semantic.payload.DatasetResponse;
import ac.software.semantic.payload.ErrorResponse;
import ac.software.semantic.payload.TemplateResponse;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.EmbedderDocumentRepository;
import ac.software.semantic.repository.IndexStructureRepository;
import ac.software.semantic.repository.TemplateRepository;
import ac.software.semantic.service.AnnotatorService;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.IdentifiersService;
import ac.software.semantic.service.IndexService;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.SchemaService;
import ac.software.semantic.service.SchemaService.ClassStructure;
import ac.software.semantic.service.TaskConflictException;
import ac.software.semantic.service.ServiceProperties;
import ac.software.semantic.service.SimpleObjectIdentifier;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.TemplateService;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import org.apache.jena.rdf.model.Model;
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

import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Parameter;

@Tag(name = "Dataset API")
@RestController
@RequestMapping("/api/dataset")
public class APIDatasetController {

	@Autowired
    @Qualifier("database")
    private Database database;
	
    @Autowired
 	private DatasetService datasetService;

    @Autowired
 	private AnnotatorService annotatorService;

    @Autowired
 	private SchemaService schemaService;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	TemplateRepository templateRepository;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
    @Autowired
    private EmbedderDocumentRepository embedderRepository;

    @Autowired
    private IndexStructureRepository indexStructureRepository;
    
	@Autowired
	private TemplateService templateService;
	
	@Autowired
	private IndexService indexService;
	
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;
    
	@Autowired
	private TaskService taskService;

	@Autowired
	private IdentifiersService idService;
	
	@Operation(summary = "Get datasets of logged in user.",
			description = "This endpoint contacts the mongo database. Returns datasets created by the user. This does not concern talking with virtuoso, nor published collections")
	@ApiResponses(value = {
			@io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Operation Successful",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = DatasetResponse.class))}),
			@io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Operation Failed",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
    @GetMapping(value = "/get-all",
	        produces = "application/json")
	public ResponseEntity<?> getDatasets(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("scope") Optional<DatasetScope> scope, @RequestParam("type") DatasetType type)  {

		List<DatasetResponse> list = datasetService.getDatasets(currentUser, scope.orElse(null), type);
		
		return ResponseEntity.ok(list);
	}       
    
    @PostMapping(value = "/create",
		     produces = "application/json")
	public ResponseEntity<?> createDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			                               @RequestBody DatasetResponse body)  {

    	try {
	    	TemplateResponse templateResponse = body.getTemplate();
	    	Template template = null;
	    	if (templateResponse != null) {
	        	if (body.getType().equals(DatasetType.DATASET)) {
					template = templateService.getDatasetImportTemplate(new ObjectId(templateResponse.getId()));
					if (template == null) {
						return ResponseEntity.badRequest().build();
					}
		    	} else if (body.getType().equals(DatasetType.CATALOG)) {
		    		template = templateService.getCatalogImportTemplate(new ObjectId(templateResponse.getId()));
					if (template == null) {
						return ResponseEntity.badRequest().build();
					}
		    	}
	    	}	
	    	
//			TripleStoreConfiguration vc = null;
//			if (body.getTripleStore() != null) {
//				vc = virtuosoConfigurations.getByName(body.getTripleStore());
//			} 
//			
//			if (vc == null) {
//				vc = virtuosoConfigurations.values().iterator().next();
//			}
			
			Dataset dataset = datasetService.createDataset(currentUser, body.getName(), body.getIdentifier(), 
		                                                   body.isPublik(), 
//		                                                   vc, 
		                                                   body.getScope(), body.getType(), body.getTypeUri() != null && body.getTypeUri().size() > 0 ? body.getTypeUri().get(0) : null,
		                                                   body.getAsProperty(),
		                                                   body.getLinks(), templateResponse);
	
			if (template != null) {
//				DatasetContainer dc = datasetService.getUnpublishedContainer(currentUser, dataset.getId().toString(), vc.getName());
				DatasetContainer dc = datasetService.getContainer(currentUser, dataset.getId().toString());
				
		     	TaskDescription tdescr = taskService.newTemplateDatasetTask(dc);
		     	taskService.call(tdescr);
			}
	
			DatasetResponse res = modelMapper.dataset2DatasetResponse(dataset, template);
		
			return ResponseEntity.ok(res);
    	
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    	}
	}
    

    @PostMapping(value = "/update/{id}",
		         produces = "application/json")
	public ResponseEntity<?> updateDataset(@CurrentUser UserPrincipal currentUser, 
			                               @PathVariable("id") String id,
			                               @RequestBody DatasetResponse body)  {

    	
//    	TripleStoreConfiguration vc = null;
//		if (body.getTripleStore() != null) {
//			vc = virtuosoConfigurations.getByName(body.getTripleStore());
//		} 
//		
//		if (vc == null) {
//			vc = virtuosoConfigurations.values().iterator().next();
//		}

		idService.remove(body.getIdentifier());
		
		Dataset dataset = datasetService.updateDataset(currentUser, 
				                                       new ObjectId(id), body.getName(), body.getIdentifier(), 
				                                       body.isPublik(), 
//				                                       vc, 
				                                       body.getScope(), body.getType(), body.getTypeUri() != null && body.getTypeUri().size() > 0 ? body.getTypeUri().get(0) : null,
				                                       body.getAsProperty(),
				                                       body.getLinks());
		
		if (dataset != null) {
			Template template = null;
			if (dataset.getTemplateId() != null) {
				template = templateRepository.findById(dataset.getTemplateId()).get();
			}
			
			if (template != null) {
//				DatasetContainer dc = datasetService.getUnpublishedContainer(currentUser, dataset.getId().toString(), vc.getName());
				DatasetContainer dc = datasetService.getContainer(currentUser, dataset.getId().toString());
				
		     	TaskDescription tdescr = taskService.newTemplateDatasetUpdateTask(dc);
		     	taskService.call(tdescr);
			}
			
			DatasetResponse res = modelMapper.dataset2DatasetResponse(dataset, template);

			return ResponseEntity.ok(res);
		} else {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
	}
    
    @DeleteMapping(value = "/delete/{id}",
		           produces = "application/json")
    public ResponseEntity<?> deleteDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	try {
			boolean deleted = datasetService.deleteDataset(currentUser, id);
			
			if (deleted) {
				return ResponseEntity.ok(new APIResponse(true, "Catalog deleted"));
			} else {
				return ResponseEntity.ok(new APIResponse(false, "Current user is not owner of catalog"));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}					
	}
    

    
    @GetMapping(value = "/get/{id}",
	            produces = "application/json")
	public ResponseEntity<?> getDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
    	try {
			Optional<Dataset> datasetOpt = datasetService.getDataset(currentUser, id);
			
			if (!datasetOpt.isPresent()) {
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
			
			Dataset dataset = datasetOpt.get();
			
			Template template = null;
			if (dataset.getTemplateId() != null) {
				template = templateRepository.findById(dataset.getTemplateId()).get();
			}
	
			ThesaurusLoadStatus st = null;
			if (dataset.getTypeUri().contains(SEMAVocabulary.ThesaurusCollection.toString())) {
				try {
					st = annotatorService.isLoaded(dataset);
		    	} catch (Exception ex) {
		    		ex.printStackTrace();
		    		st = ThesaurusLoadStatus.UNKNOWN;
		    	}
			}
	
			return ResponseEntity.ok(modelMapper.dataset2DatasetResponse(dataset, template, st));
    	
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    	}
		
	}   
    
    @PostMapping(value = "/insert",
	             produces = "application/json")
	public ResponseEntity<?> insertDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("id") String id, @RequestParam("toId") String toId)  {
	
		boolean inserted = datasetService.insert(currentUser, id, toId);
		if (inserted) {
			return ResponseEntity.ok(new APIResponse(true,"")); 
		} else {
			return ResponseEntity.ok(new APIResponse(false,"Target not found"));
		}
	}    
    
    @PostMapping(value = "/remove",
                 produces = "application/json")
	public ResponseEntity<?> removeDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("id") String id, @RequestParam("fromId") String fromId)  {
	
		boolean inserted = datasetService.remove(currentUser, id, fromId);
		if (inserted) {
			return ResponseEntity.ok(new APIResponse(true,"")); 
		} else {
			return ResponseEntity.ok(new APIResponse(false,"Target not found"));
		}
	} 
    

    @PostMapping(value = "/execute-mappings/{id}")
 	public ResponseEntity<?> executeDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		DatasetContainer dc = null;
		try {
//			dc = datasetService.getUnpublishedContainer(currentUser, id, null);
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return new ResponseEntity<>(APIResponse.FailureResponse(), HttpStatus.NOT_FOUND);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse(), HttpStatus.BAD_REQUEST);
		}
		
		try {
	    	TaskDescription tdescr = taskService.newDatasetExecuteMappingsTask(dc); 
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset mappings has been scheduled for execution."), HttpStatus.ACCEPTED);
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
    
    @PostMapping(value = "/execute-mappings-and-republish/{id}")
 	public ResponseEntity<?> executeRepublishDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class);
	    	} else if (!dc.isPublished()) {
	    		throw TaskConflictException.notPublished(dc);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}

		try {
	    	TaskDescription tdescr = taskService.newDatasetExecuteMappingsAndRepublishTask(dc);

	    	if (tdescr != null) {
				idService.remove(dc.getDataset().getIdentifier());

	    		taskService.call(tdescr);
	    		return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset has been scheduled for mapping execution and republication."), HttpStatus.ACCEPTED);
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
    
    @PostMapping(value = "/publish/{id}")
	public ResponseEntity<?> publishDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("triple-store") Optional<String> tripleStore, @PathVariable("id") String id, @RequestParam("visibility") String visibility)  {
    	TripleStoreConfiguration vc;
    	
    	try {
	    	if (tripleStore.isPresent()) {
	    		vc = virtuosoConfigurations.getByName(tripleStore.get());
	    		
		    	if (vc == null) {
		    		return new ResponseEntity<>(APIResponse.FailureResponse("Not found."), HttpStatus.NOT_FOUND);
		    	}
		    	
	    	} else {
	    		vc = virtuosoConfigurations.values().iterator().next();
	    	}
	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}
    	
    	synchronized (DatasetService.syncString(id)) {
    		DatasetContainer dc = null;
			try {
//				dc = datasetService.getUnpublishedContainer(currentUser, id, vc);
				dc = datasetService.getContainer(currentUser, id);
				
		    	if (dc == null) {
		    		return APIResponse.notFound(DatasetContainer.class);
		    	} else if (dc.isPublished()) {
		    		throw TaskConflictException.alreadyPublished(dc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
			}
			
			try {
	   			Properties props = new Properties();
	   			props.put(ServiceProperties.PUBLISH_MODE, visibility.equals("public") ? ServiceProperties.PUBLISH_MODE_PUBLIC : ServiceProperties.PUBLISH_MODE_PRIVATE);
	   			props.put(ServiceProperties.PUBLISH_METADATA, true);
	   			props.put(ServiceProperties.PUBLISH_CONTENT, true);
	   			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
	   			props.put(ServiceProperties.TRIPLE_STORE, vc);
	    			
	   			TaskDescription tdescr = taskService.newDatasetPublishTask(dc, props);
	
		    	if (tdescr != null) {
		   			idService.remove(dc.getDataset().getIdentifier());
		   			taskService.call(tdescr);
		   			
		   			return APIResponse.acceptedToPublish(dc);
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
    
    @PostMapping(value = "/publish-unpublished-content/{id}")
 	public ResponseEntity<?> publishUnpublishedContent(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable("id") String id)  {
    	
    	synchronized (DatasetService.syncString(id)) {
			DatasetContainer dc = null;
			try {
				dc = datasetService.getContainer(currentUser, id);
				
		    	if (dc == null) {
		    		return APIResponse.notFound(DatasetContainer.class);
		    	} else if (!dc.isPublished()) {
		    		throw TaskConflictException.notPublished(dc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
			}
	
			try {
	   			Properties props = new Properties();
	   			props.put(ServiceProperties.PUBLISH_MODE, ServiceProperties.PUBLISH_MODE_CURRENT);
	   			props.put(ServiceProperties.PUBLISH_METADATA, false);
	   			props.put(ServiceProperties.PUBLISH_CONTENT, true);
	   			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, true);
	
	   			TaskDescription tdescr = taskService.newDatasetPublishTask(dc, props);
				
		    	if (tdescr != null) {
		   			idService.remove(dc.getDataset().getIdentifier());
		    		taskService.call(tdescr);
		    		
		    		return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset has been scheduled for unpublished content publication."), HttpStatus.ACCEPTED);
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
	public ResponseEntity<?> unpublishDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
   		synchronized (DatasetService.syncString(id)) {
			DatasetContainer dc = null;
			try {
				dc = datasetService.getContainer(currentUser, id);
				
		    	if (dc == null) {
		    		return APIResponse.notFound(DatasetContainer.class);
		    	} else if (!dc.isPublished()) {
		    		throw TaskConflictException.notPublished(dc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
			}
			
			try {
	   			Properties props = new Properties();
	   			props.put(ServiceProperties.PUBLISH_METADATA, true);
	   			props.put(ServiceProperties.PUBLISH_CONTENT, true);
	   			
	   			TaskDescription tdescr = taskService.newDatasetUnpublishTask(dc, props);
				
		    	if (tdescr != null) {
		   			idService.remove(dc.getDataset().getIdentifier());
	
		    		taskService.call(tdescr);
		    		return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset has been scheduled for unpublication."), HttpStatus.ACCEPTED);
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
   
    @PostMapping(value = "/republish/{id}")
 	public ResponseEntity<?> republishDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	synchronized (DatasetService.syncString(id)) {
			DatasetContainer dc = null;
			try {
				dc = datasetService.getContainer(currentUser, id);
				
		    	if (dc == null) {
		    		return APIResponse.notFound(DatasetContainer.class);
		    	} else if (!dc.isPublished()) {
		    		throw TaskConflictException.notPublished(dc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
			}
			
			try {
		    	TaskDescription tdescr = taskService.newDatasetRepublishTask(dc);
				
		    	if (tdescr != null) {
	    			idService.remove(dc.getDataset().getIdentifier());
	
	    			taskService.call(tdescr);
		    		return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset has been scheduled for republication."), HttpStatus.ACCEPTED);
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
    
    @PostMapping(value = "/republish-metadata/{id}")
 	public ResponseEntity<?> republishMetadata(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class);
	    	} else if (!dc.isPublished()) {
	    		throw TaskConflictException.notPublished(dc);
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}
		
		try {
   			TaskDescription tdescr = taskService.newDatasetRepublishMetadataTask(dc);
			
	    	if (tdescr != null) {
    			idService.remove(dc.getDataset().getIdentifier());

	    		taskService.call(tdescr);
	    		return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset has been scheduled for metadata republication."), HttpStatus.ACCEPTED);
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

    @PostMapping(value = "/create-distribution/{id}")
	public ResponseEntity<?> publishDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                @PathVariable("id") String id,
			                                @RequestBody CreateDatasetDistributionRequest body)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class);
	    	} else if (!dc.isPublished()) {
	    		throw TaskConflictException.notPublished(dc);
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}

		try {
    		Properties props = new Properties();
   			props.put(ServiceProperties.CREATE_DISTRIBUTION_OPTIONS, body);

   			TaskDescription tdescr = taskService.newDatasetCreateDistributionTask(dc, props);
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
    			return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset distribution creation has been scheduled."), HttpStatus.ACCEPTED);
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
    
	@PostMapping(value = "/stop-create-distribution/{id}")
	public ResponseEntity<?> stop(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class);
	    	} else if (!dc.isPublished()) {
	    		throw TaskConflictException.notPublished(dc);
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}

		try {
			synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
	    		TaskDescription td =  taskService.getActiveTask(dc, TaskType.DATASET_CREATE_DISTRIBUTION);
	    		if (td != null) {
	    			if (taskService.requestStop(td.getId())) {
	    				return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset distribution creation is being stopped."), HttpStatus.ACCEPTED);
	    			} else {
	    				return new ResponseEntity<>(APIResponse.FailureResponse("The dataset distribution creation could not be stopped."), HttpStatus.CONFLICT);
	    			}
	    		} else {
	    			datasetService.failCreateDistribution(dc);
	    			  
	    			return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset distribution creation is not being executed.", dc.asResponse()), HttpStatus.OK);
	    		}
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	    	
	}    
    
    @PostMapping(value = "/clear-distribution/{id}")
 	public ResponseEntity<?> clearDistribution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}
		
		try {
			boolean ok = datasetService.clearDistribution(dc); // TODO: error handling
			
			return new ResponseEntity<>(APIResponse.SuccessResponse("The dataset distribution has been cleared."), HttpStatus.OK);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
 	}	

    
    @PostMapping(value = "/flipVisibility/{id}")
  	public ResponseEntity<?> flipVisibility(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable("id") String id)  {

		DatasetContainer dc = datasetService.getContainer(currentUser, id);
		if (dc == null) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
		
		idService.remove(dc.getDataset().getIdentifier());
		
	   	TaskDescription tdescr = taskService.newDatasetFlipVisibilityTask(dc);
	   	taskService.call(tdescr);
  		   	
		return new ResponseEntity<>(HttpStatus.ACCEPTED);
  	}  
    
    @PostMapping(value = "/index/{id}")
	public ResponseEntity<?> indexDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestBody CreateIndexRequest icr)  {
    	
   		if (elasticConfigurations.isEmpty()) {
       		return APIResponse.methodNotAllowed();
       	}
    	
    	synchronized (DatasetService.syncString(id.toHexString())) {
    		DatasetContainer dc = null;
    		IndexStructure isc = null;
    		
			try {
				dc = datasetService.getContainer(currentUser, new SimpleObjectIdentifier(id));
				
		    	if (dc == null) {
		    		return APIResponse.notFound(DatasetContainer.class);
		    	} else if (dc.isIndexed()) {
		    		throw TaskConflictException.alreadyIndexed(dc);
		    	} else if (!dc.isPublished()) {
		    		throw TaskConflictException.notPublished(dc);
		    	}
		    	
		    	if (icr.getIndexId() != null) {
		    		Optional<IndexStructure> iscOpt = indexStructureRepository.findById(icr.getIndexId());
		    		
		    		if (!iscOpt.isPresent()) {
		    			return new ResponseEntity<>(APIResponse.FailureResponse("The index was not found."), HttpStatus.NOT_FOUND);
		    		}
		    		
		    		isc = iscOpt.get();
		    	} else {
		        	ElasticConfiguration ec;

			    	if (icr.getIndexEngine() != null) {
			    		ec = elasticConfigurations.getByName(icr.getIndexEngine());
			    		
				    	if (ec == null) {
				    		return APIResponse.badRequest();
				    	}

			    	} else {
			    		ec = elasticConfigurations.values().iterator().next();
			    	}
			    	
		    		isc = indexService.createIndex(currentUser, icr.getIndexIdentifier(), ec, icr.getIndexStructures(), icr.getKeysMetadata());
		    		
		    		if (isc == null) {
		    			throw new TaskConflictException("An index with the same name already exists");
		    		}
		    	}
		    	
		    	
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
			}
			
			try {
	   			Properties props = new Properties();
	   			props.put(ServiceProperties.INDEX_STRUCTURE, isc);
	   			
	   			TaskDescription tdescr = taskService.newDatasetIndexTask(dc, props);
	
		    	if (tdescr != null) {
		   			taskService.call(tdescr);
		   			
		   			return APIResponse.acceptedToIndex(dc);
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
    	
    	
//    	
//	   	TaskDescription tdescr = taskService.newDatasetIndexTask(currentUser.getId(), id);
//
//	   	try {
//		   	ListenableFuture<Date> task = datasetService.indexDataset(tdescr, currentUser, id, indexId, wsService);
//		   	taskService.setTask(tdescr, task);
//	   	} catch (TaskFailureException ex) {
//	   		ex.printStackTrace();
//	   	}
//	   	
//		return new ResponseEntity<>(HttpStatus.ACCEPTED);
	}     
    
    @PostMapping(value = "/unindex/{id}")
	public ResponseEntity<?> unindexDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

   		if (elasticConfigurations.isEmpty()) {
       		return APIResponse.methodNotAllowed();
       	}
   		
    	synchronized (DatasetService.syncString(id)) {
    		DatasetContainer dc = null;
			try {
				dc = datasetService.getContainer(currentUser, id);
				
		    	if (dc == null) {
		    		return APIResponse.notFound(DatasetContainer.class);
		    	} else if (!dc.isIndexed()) {
		    		throw TaskConflictException.notIndexed(dc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
			}
			
			try {
	   			TaskDescription tdescr = taskService.newDatasetUnindexTask(dc);
	
		    	if (tdescr != null) {
		   			taskService.call(tdescr);
		   			
		   			return APIResponse.acceptedToUnindex(dc);
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

    @GetMapping(value = "/schema/{id}")
    public ResponseEntity<?> schema(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(defaultValue = "ttl", name = "format") String format)  {

		Optional<Dataset> dataset = datasetService.getDataset(currentUser, id);
		if (dataset.isPresent()) {
			Model model = schemaService.readSchema(resourceVocabulary.getDatasetAsResource(dataset.get().getUuid()).toString());
			Writer sw = new StringWriter();
			model.write(sw, format) ;

			return ResponseEntity.ok(sw.toString());
		} else {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
    }
    
    @GetMapping(value = "/schema-classes/{id}")
    public ResponseEntity<?> schemaClasses(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(required = false, defaultValue = "false") boolean embedders)  {

    	try {
			Optional<Dataset> datasetOpt = datasetService.getDataset(currentUser, id);
			if (!datasetOpt.isPresent()) {
				return APIResponse.notFound(DatasetContainer.class);
			}
			
			Dataset dataset = datasetOpt.get();
			
			List<ClassStructure> tcs = schemaService.readTopClasses(dataset);
			
			List<ClassStructureResponse> cs = new ArrayList<>();
			for (ClassStructure css : tcs) {
				ClassStructureResponse csr = ClassStructureResponse.createFrom(css);
				
				if (embedders) {
					for (EmbedderDocument edoc : embedderRepository.findByDatasetUuidAndOnClass(dataset.getUuid(), csr.getClazz())) {
						csr.addEmbedder(edoc.getId().toString(), edoc.getEmbedder());
					}
				}
				
				cs.add(csr);

			}
				
			return ResponseEntity.ok(cs);
		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}

    }
    
    
    @GetMapping(value = "/exists-dataset",
            produces = "application/json")
	public ResponseEntity<?> checkDatasetIdentifier(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true)  @RequestParam("identifier") String identifier)  {
		
    	if (identifier.equals(database.getName())) {
    		return ResponseEntity.ok("{ \"exists\" : true } ");
    	}
    	
    	Optional<Dataset> datasetOpt = datasetRepository.findByIdentifierAndDatabaseId(identifier, database.getId());
		if (datasetOpt.isPresent()) {
			return ResponseEntity.ok("{ \"exists\" : true } ");
		} else {
			return ResponseEntity.ok("{ \"exists\" : false } ");
		}
	}
    
    
    
}
