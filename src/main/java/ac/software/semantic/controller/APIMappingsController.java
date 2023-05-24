package ac.software.semantic.controller;

import java.util.List;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Parameter;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.controller.utils.AsyncUtils;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.MappingType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.MappingResponse;
import ac.software.semantic.payload.MappingUpdateRequest;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.FolderService;
import ac.software.semantic.service.MappingObjectIdentifier;
import ac.software.semantic.service.MappingsService;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.SimpleObjectIdentifier;
import ac.software.semantic.service.TaskService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Mappings API")
@RestController
@RequestMapping("/api/mappings")
public class APIMappingsController {

	Logger logger = LoggerFactory.getLogger(APIMappingsController.class);

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private MappingRepository mappingsRepository;
	
    @Autowired
 	private MappingsService mappingsService;

    @Value("${mapping.execution.folder}")
    private String mappingsFolder;

    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;
    
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
	
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
	@Autowired
	FolderService folderService;

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private TaskService taskService;

	@Autowired
	private APIUtils apiUtils;
	
    @GetMapping("/getAll")
	public List<MappingResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("datasetId") String datasetId)  {

    	return mappingsService.getMappings(currentUser, datasetId);
	}
    
    @GetMapping("/get/{id}")
 	public ResponseEntity<?> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	try {
	    	MappingContainer mc = mappingsService.getContainer(currentUser, id);
	    	
	    	if (mc == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	} else {
	    		return ResponseEntity.ok(mc.asResponse());
	    	}
	    	
    	} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    	}   
    	
 	}
    
    @GetMapping("/get-d2rml/{id}")
 	public ResponseEntity<?> getD2RML(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	Optional<MappingDocument> dopt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
    	
		if (!dopt.isPresent()) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		
   		return ResponseEntity.ok(dopt.get().getFileContents()); 
 	}
	
    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> deleteMapping(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	try {
	    	boolean deleted = mappingsService.deleteMapping(currentUser, id);
			
	    	if (deleted) {
	    		return ResponseEntity.ok(new APIResponse(true, "Resource deleted"));
	    	} else {
	    		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}		    	
	}
    
    @PostMapping(value = "/create")
	public ResponseEntity<?> createMapping(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			                               @RequestParam("datasetId") String datasetId,
			                               @RequestParam("type") MappingType type,
			                               @RequestPart("body") String body,
			                               @RequestPart("file") Optional<MultipartFile> file)  {
    	

		try {    	
			ObjectMapper objectMapper = new ObjectMapper();

	        //read json file and convert to customer object
			MappingUpdateRequest mur = objectMapper.readValue(body, MappingUpdateRequest.class);

			MappingDocument doc;
			if (file.isPresent()) {
				doc = mappingsService.create(currentUser, datasetId, type, mur.getName(), mur.getParameters(), file.get().getOriginalFilename(), new String(file.get().getBytes()));
			} else {
				doc = mappingsService.create(currentUser, datasetId, type, mur.getName(), mur.getParameters(), mur.getTemplateId());
			}
			
//			folderService.saveMappingD2RMLFile(currentUser, doc, file);
    	
			Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));

			if (!datasetOpt.isPresent()) {
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
			
			Dataset dataset = datasetOpt.get();
			
			ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
			final TripleStoreConfiguration vc;
			if (psv != null) {
				vc = psv.getTripleStoreConfiguration();
			} else {
				vc = null;
			}
			
			return ResponseEntity.ok(modelMapper.mapping2MappingResponse(vc, doc, currentUser));
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}			

	}        

    @PostMapping(value = "/update/{id}")
	public ResponseEntity<?> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, 
			@RequestPart("body") String body,
            @RequestPart("file") Optional<MultipartFile> file)  {

		try {    	
			ObjectMapper objectMapper = new ObjectMapper();

			MappingUpdateRequest mur = objectMapper.readValue(body, MappingUpdateRequest.class);

	    	mappingsService.updateMapping(currentUser, id, mur.getName(), mur.getParameters(), file.isPresent() ? file.get().getOriginalFilename() : null, file.isPresent() ? new String(file.get().getBytes()) : null);
			
//	    	if (file != null) {
//				Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//	
//				if (entry.isPresent() && file.isPresent()) {
//					folderService.saveMappingD2RMLFile(currentUser, entry.get(), file.get());
//				}
//			}
			
			return ResponseEntity.ok(new APIResponse(true, "Document updated"));
			
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}		
	}
    
    @PostMapping("/create-instance/{id}")
	public ResponseEntity<?> createInstance(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody List<ParameterBinding> bindings)  {

    	try {
	    	MappingContainer mc = mappingsService.getContainer(currentUser, id);
	    	
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class);
	    	}
	    	
	    	MappingInstance mi = mappingsService.createParameterBinding(mc, bindings);
			
	    	TripleStoreConfiguration vc = mc.getDatasetTripleStoreVirtuosoConfiguration();
	    	
	    	return ResponseEntity.ok(modelMapper.mappingInstance2MappingInstanceResponse(vc, mi));
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
    	}
	}    
    
    @PostMapping("/update-instance/{id}")
	public ResponseEntity<?> updateInstance(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") String instanceId, @RequestBody List<ParameterBinding> bindings)  {

    	try {
	    	MappingContainer mc = mappingsService.getContainer(currentUser, id, instanceId);
	    	
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class);
	    	}
	    	
	    	MappingInstance mi = mappingsService.updateParameterBinding(mc, bindings);
	    	
			TripleStoreConfiguration vc = mc.getDatasetTripleStoreVirtuosoConfiguration();
	
	    	return ResponseEntity.ok(modelMapper.mappingInstance2MappingInstanceResponse(vc, mi));
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
    	}    
    }
    

    @DeleteMapping(value = "/delete-instance/{id}")
	public ResponseEntity<?> deleteInstance(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") String instanceId)  {

		try {
	    	MappingContainer mc = mappingsService.getContainer(currentUser, id, instanceId);
	    	
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class);
	    	}

			boolean ok = mappingsService.deleteParameterBinding(mc);
			return new ResponseEntity<>(APIResponse.SuccessResponse("Mapping instance deleted."), HttpStatus.OK);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
    }	

    
    @PostMapping(value = "/upload-attachment/{id}",
    		     consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<?> uploadAttachment(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId, @RequestParam MultipartFile file)  {

		try {
			Optional<MappingDocument> docOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

			if (docOpt.isPresent()) {
				MappingDocument doc = docOpt.get();
				
				if (!instanceId.isPresent()) {
					String fileName = folderService.saveAttachment(currentUser, doc, file);
					
					doc.addDataFile(fileName);
				} else {
					MappingInstance mi = doc.getInstance(new ObjectId(instanceId.get()));
					
					String fileName = folderService.saveAttachment(currentUser, doc, mi, file);
					
					mi.addDataFile(fileName);
				}
				
				mappingsRepository.save(doc);
			}
			
			return new ResponseEntity<>(HttpStatus.OK);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return APIResponse.serverError(ex);
    	}
	}
    
    @DeleteMapping("/delete-attachment/{id}")
    public ResponseEntity<?> deleteAttachment(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId, @RequestParam String filename)  {

		try {
			Optional<MappingDocument> docOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

			if (docOpt.isPresent()) {
				MappingDocument doc = docOpt.get();
				
				if (!instanceId.isPresent()) {
					folderService.deleteAttachment(currentUser, doc, filename);

					doc.removeDataFile(filename);
				} else {
					MappingInstance mi = doc.getInstance(new ObjectId(instanceId.get()));

					folderService.deleteAttachment(currentUser, doc, mi, filename);
					
					mi.removeDataFile(filename);
				}
					
				mappingsRepository.save(doc);
			}
			
			return new ResponseEntity<>(HttpStatus.OK);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return APIResponse.serverError(ex);
    	}
	}
    
	@PostMapping(value = "/stop/{id}")
	public ResponseEntity<?> stop(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId)  {

		MappingContainer mc = null;
		try {
			mc = mappingsService.getContainer(currentUser, id, instanceId.orElse(null));
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class);
	    	}
		} catch (Exception ex) {
			return new ResponseEntity<>(APIResponse.FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
		}
    	
		try {
			synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
	    		TaskDescription td =  taskService.getActiveTask(mc, TaskType.MAPPING_EXECUTE);
	    		if (td != null) {
	    			if (taskService.requestStop(td.getId())) {
	    				return new ResponseEntity<>(APIResponse.SuccessResponse("The mapping is being stopped."), HttpStatus.ACCEPTED);
	    			} else {
	    				return new ResponseEntity<>(APIResponse.FailureResponse("The mapping could not be stopped."), HttpStatus.CONFLICT);
	    			}
	    		} else {
	    			MappingDocument mdoc = mappingsService.failExecution(mc);
	    			  
	    			return new ResponseEntity<>(APIResponse.SuccessResponse("The mapping is not being executed.", 
	    					mdoc != null ? modelMapper.mapping2MappingResponse(mc.getDatasetTripleStoreVirtuosoConfiguration(), mdoc, currentUser) : null),
	    					HttpStatus.OK);
	    		}
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	    	
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<?> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
		return apiUtils.execute(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingsService);
	}
    
    @PostMapping(value = "/clear-execution/{id}")
    public ResponseEntity<?> clearExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
    	return apiUtils.clearExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingsService);
 	}		
     
    @GetMapping(value = "/preview-last-execution/{id}",
                produces = "text/plain")    
	public ResponseEntity<?> previewLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
		return apiUtils.previewLastExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingsService);
	}	

    @GetMapping(value = "/preview-published-execution/{id}",
            produces = "text/plain")    
	public ResponseEntity<?> previewPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
		return apiUtils.previewPublishedExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingsService);
	}	
     
    @GetMapping(value = "/download-last-execution/{id}")
 	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
     	return apiUtils.downloadLastExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingsService);
 	} 

    @GetMapping(value = "/download-published-execution/{id}")
 	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
     	return apiUtils.downloadPublishedExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingsService);
 	} 
     
    // Experimental
    @PostMapping(value = "/unpublish/{id}")
  	public ResponseEntity<?> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId)  {
  		
 		try {
 			AsyncUtils.supplyAsync(() -> mappingsService.unpublish(currentUser, id, instanceId.isPresent() ? instanceId.get() : null));
 			
 			return new ResponseEntity<>(HttpStatus.ACCEPTED);
 		} catch (Exception e) {
 			e.printStackTrace();
 		}

 		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  	}	
     
     


}
