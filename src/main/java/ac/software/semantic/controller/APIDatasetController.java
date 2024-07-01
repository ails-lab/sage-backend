package ac.software.semantic.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import javax.validation.constraints.NotNull;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.request.DatasetUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.modifier.DatasetResponseModifier;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.ProjectService;
import ac.software.semantic.service.exception.TaskConflictException;
import ac.software.semantic.service.lookup.DatasetLookupProperties;
import ac.software.semantic.service.ServiceProperties;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.TaskSpecification;
import ac.software.semantic.service.container.SimpleObjectIdentifier;

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
 	private ProjectService projectService;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
	@Autowired
	private TaskService taskService;
	
	@Autowired
	private APIUtils apiUtils;
	
    @GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                    @RequestParam DatasetType type, 
			                                    @RequestParam(required = false) List<DatasetScope> scope, 
			                                    @RequestParam(required = false) Boolean onlyPublished, 
			                                    @RequestParam(required = false) ObjectId projectId,
			                                    @RequestParam(required = false) List<String> sortBy,
			                                    @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
    	DatasetLookupProperties lp = new DatasetLookupProperties();
		lp.setDatasetType(type);
		lp.setDatasetScope(scope);
		lp.setOnlyPublished(onlyPublished != null ? onlyPublished : null);
		lp.setSortByFields(sortBy);

		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, projectId != null ? Arrays.asList(new ObjectId[] {projectId}) : null, lp, datasetService, projectService, apiUtils.pageable(page, size))).toResponseEntity();
	}
    
    @GetMapping(value = "/get-all-other-public")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
		                                      @RequestParam DatasetType type,
		                                      @RequestParam(required = false) List<DatasetScope> scope, 
		                                      @RequestParam(required = false) ObjectId projectId,
		                                      @RequestParam(required = false) List<String> sortBy,
			                                  @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
    	DatasetLookupProperties lp = new DatasetLookupProperties();
		lp.setDatasetType(type);
		lp.setDatasetScope(scope);
		lp.setOnlyPublished(true);
		lp.setPublik(true);
		lp.setUserIdNot(new ObjectId(currentUser.getId()));
		lp.setSortByFields(sortBy);

		DatasetResponseModifier rm = new DatasetResponseModifier();
		rm.setUser(ResponseFieldType.EXPAND);

		return APIResponse.result(apiUtils.getAll(projectId, lp, datasetService, projectService, rm, apiUtils.pageable(page, size))).toResponseEntity();
	} 
    

    @PostMapping(value = "/create")
	public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam(required = false) ObjectId projectId,  @RequestBody DatasetUpdateRequest ur)  {
    	return apiUtils.cnew(currentUser, projectId != null ? new SimpleObjectIdentifier(projectId) : null, ur, datasetService, projectService).toResponseEntity();
	}
	
    @PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody DatasetUpdateRequest ur)  {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, datasetService).toResponseEntity();
	}    
    
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), datasetService).toResponseEntity();
	}
	
    @GetMapping(value = "/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false) Boolean details)  {
    	
    	DatasetResponseModifier rm = new DatasetResponseModifier();
		rm.setUser(ResponseFieldType.EXPAND);

    	if (details != null && details) {
    		rm.setMappings(ResponseFieldType.EXPAND);
    		rm.setRdfFiles(ResponseFieldType.EXPAND);
    		rm.setDistributions(ResponseFieldType.EXPAND);
    		rm.setUserTasks(ResponseFieldType.EXPAND);
    		rm.setIndices(ResponseFieldType.EXPAND);
    		rm.setDatasets(ResponseFieldType.EXPAND);
    		rm.setProjects(ResponseFieldType.EXPAND);
    	}    		
    		
    	return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), datasetService, rm).toResponseEntity();
	}   
    
    @PostMapping(value = "/add-to-project/{id}")
	public ResponseEntity<APIResponse> addToProject(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId projectId)  {
    	return apiUtils.addAsMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(projectId), datasetService, projectService).toResponseEntity();
	} 

    @PostMapping(value = "/add-dataset/{id}")
	public ResponseEntity<APIResponse> addDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId datasetId)  {
    	return apiUtils.addMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(datasetId), datasetService, datasetService).toResponseEntity();
	} 
    
    @PostMapping(value = "/remove-dataset/{id}")
	public ResponseEntity<APIResponse> removeDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId datasetId)  {
    	return apiUtils.removeMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(datasetId), datasetService, datasetService).toResponseEntity();
	} 

    @PostMapping(value = "/execute-mappings/{id}")
 	public ResponseEntity<APIResponse> executeMappings(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false) Integer group)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class).toResponseEntity();
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest().toResponseEntity();
		}

		Properties props = new Properties();
		props.put(ServiceProperties.DATASET_GROUP, group != null ? group : -1);

		try {

	    	TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.DATASET_EXECUTE_MAPPINGS).createTask(dc, props); 
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return APIResponse.SuccessResponse("The dataset mappings has been scheduled for execution.", HttpStatus.ACCEPTED).toResponseEntity();
	    	} else {
	    		return APIResponse.FailureResponse("Server error.", HttpStatus.INTERNAL_SERVER_ERROR).toResponseEntity();
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex).toResponseEntity();		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
 	} 

    @PostMapping(value = "/execute-annotators/{id}")
 	public ResponseEntity<APIResponse> executeAnnotators(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, List<String> tags)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class).toResponseEntity();
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest().toResponseEntity();
		}

		Properties props = new Properties();
		props.put(ServiceProperties.ANNOTATOR_TAG, tags);

		try {

	    	TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.DATASET_EXECUTE_ANNOTATORS).createTask(dc, props); 
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return APIResponse.SuccessResponse("The dataset annotators has been scheduled for execution.", HttpStatus.ACCEPTED).toResponseEntity();
	    	} else {
	    		return APIResponse.FailureResponse("Server error.", HttpStatus.INTERNAL_SERVER_ERROR).toResponseEntity();
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex).toResponseEntity();		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
 	} 
    
    @PostMapping(value = "/republish-annotators/{id}")
 	public ResponseEntity<APIResponse> republishAnnotators(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, List<String> tags)  {

		DatasetContainer dc = null;
		try {
			dc = datasetService.getContainer(currentUser, id);
			
	    	if (dc == null) {
	    		return APIResponse.notFound(DatasetContainer.class).toResponseEntity();
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest().toResponseEntity();
		}

		Properties props = new Properties();
		props.put(ServiceProperties.ANNOTATOR_TAG, tags);

		try {

	    	TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.DATASET_REPUBLISH_ANNOTATORS).createTask(dc, props); 
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return APIResponse.SuccessResponse("The dataset annotators has been scheduled for republishing.", HttpStatus.ACCEPTED).toResponseEntity();
	    	} else {
	    		return APIResponse.FailureResponse("Server error.", HttpStatus.INTERNAL_SERVER_ERROR).toResponseEntity();
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex).toResponseEntity();		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
 	} 

    @PostMapping(value = "/publish/{id}")
	public ResponseEntity<APIResponse> publish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("triple-store") Optional<String> tripleStore, @RequestParam(required = false) Integer group)  {
		
    	TripleStoreConfiguration vc;
    	
    	try {
	    	if (tripleStore.isPresent()) {
	    		vc = virtuosoConfigurations.getByName(tripleStore.get());
	    		
		    	if (vc == null) {
		    		return APIResponse.badRequest().toResponseEntity();
		    	}
	    	} else {
	    		vc = virtuosoConfigurations.values().iterator().next();
	    	}
	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest().toResponseEntity();
		}
    	
		Properties props = new Properties();
		props.put(ServiceProperties.METADATA, ServiceProperties.ALL);
		props.put(ServiceProperties.CONTENT,  ServiceProperties.ALL);
		props.put(ServiceProperties.REPUBLISH, Boolean.FALSE);
		props.put(ServiceProperties.TRIPLE_STORE, vc);
		props.put(ServiceProperties.DATASET_GROUP, group != null ? group : -1);

		return apiUtils.publish(currentUser, new SimpleObjectIdentifier(id), datasetService, props).toResponseEntity();
	} 

   	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<APIResponse> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false) Integer group)  {
		
   		Properties props = new Properties();
		props.put(ServiceProperties.METADATA, ServiceProperties.ALL);
		props.put(ServiceProperties.CONTENT,  ServiceProperties.ALL);
		props.put(ServiceProperties.REPUBLISH, Boolean.FALSE);
		props.put(ServiceProperties.DATASET_GROUP, group != null ? group : -1);

   		return apiUtils.unpublish(currentUser, new SimpleObjectIdentifier(id), datasetService, props).toResponseEntity();
   	}
   	
    @PostMapping(value = "/republish/{id}")
 	public ResponseEntity<APIResponse> republish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(defaultValue = "ALL") String content, @RequestParam(required = false) Integer group)  {

		Properties props = new Properties();
		props.put(ServiceProperties.METADATA, ServiceProperties.ALL);
		props.put(ServiceProperties.CONTENT, ServiceProperties.valueFromString(content));
		props.put(ServiceProperties.REPUBLISH, Boolean.TRUE);
		props.put(ServiceProperties.DATASET_GROUP, group != null ? group : -1);
		
		return apiUtils.republish(currentUser, new SimpleObjectIdentifier(id), datasetService, props).toResponseEntity();
 	}  
    
    @GetMapping(value = "/exists-identifier")
	public ResponseEntity<APIResponse> existsIdentifier(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true) @NotNull @RequestParam String identifier, @RequestParam(required = false) ObjectId datasetId, @RequestParam(required = false) ObjectId projectId)  {
		
    	try {
    		Dataset object;
    		
	    	if (datasetId == null) { // for create dataset
	        	object = new Dataset(database);
	
	    		if (projectId != null) {
		    		object.setProjectId(Arrays.asList(new ObjectId[] { projectId }));
		    	}
	    	} else { // for update dataset
	    		DatasetContainer dc = null;
    			dc = datasetService.getContainer(currentUser, datasetId);
	    			
    	    	if (dc == null) {
    	    		return APIResponse.notFound(DatasetContainer.class).toResponseEntity();
    	    	}
    	    	
    	    	object = dc.getObject();
	    	}
	    	
	    	object.setIdentifier(identifier);
	    	
	    	return apiUtils.identifierConflict(object, IdentifierType.IDENTIFIER, datasetService).toResponseEntity();
		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}
    


    
}
