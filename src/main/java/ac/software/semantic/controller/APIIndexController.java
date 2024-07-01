package ac.software.semantic.controller;

import java.util.Arrays;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Parameter;

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

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.payload.request.IndexUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.repository.core.IndexStructureRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.IndexService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Index API")

@RestController
@RequestMapping("/api/index")
public class APIIndexController {

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

    @Autowired
    private IndexService indexService;

    @Autowired
    private DatasetService datasetService;
    
    @Autowired
    private IndexStructureRepository indexStructureRepository;
    
	@Autowired
	private APIUtils apiUtils;

	private void setElasticConfiguration(IndexUpdateRequest ur) throws Exception {
   		ElasticConfiguration ec = elasticConfigurations.getByName(ur.getIndexEngine());

    	if (ec == null) {
    		throw new Exception("Invalid index engine");
    	}

    	if (ur.getIndexStructureId() != null) {
		    Optional<IndexStructure> iscOpt = indexStructureRepository.findById(ur.getIndexStructureId());
		    		
		    if (!iscOpt.isPresent()) {
		    	throw new Exception("Index structure not found");
		    }
    	}
    	
    	ur.setElasticConfiguration(ec);
	}
	
    @PostMapping(value = "/new")
	public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId, @RequestBody IndexUpdateRequest ur)  {
    	
   		if (elasticConfigurations.isEmpty()) {
       		return APIResponse.methodNotAllowed().toResponseEntity();
       	}
   		
		try {
	   		setElasticConfiguration(ur);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	    	
	    
		return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), ur, indexService, datasetService).toResponseEntity();
	}
    
	@PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody IndexUpdateRequest ur) {
		
   		if (elasticConfigurations.isEmpty()) {
       		return APIResponse.methodNotAllowed().toResponseEntity();
       	}
   		
		try {
	   		setElasticConfiguration(ur);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	    	

		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, indexService).toResponseEntity();
	}    
	
    @PostMapping("/change-order/{id}")
 	public ResponseEntity<APIResponse> changeOrder(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam int step)  {
    	return apiUtils.changeOrder(currentUser, new SimpleObjectIdentifier(id), step, indexService, datasetService).toResponseEntity();
 	}
    
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), indexService, datasetService).toResponseEntity();
	}
    
    @PostMapping(value = "/create/{id}")
	public ResponseEntity<APIResponse> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.create(currentUser, new SimpleObjectIdentifier(id), indexService, datasetService).toResponseEntity();
	}
    
	@PostMapping(value = "/stop-create/{id}")
	public ResponseEntity<APIResponse> stop(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.stopCreate(currentUser, new SimpleObjectIdentifier(id), indexService).toResponseEntity();
	}    
	
	@PostMapping(value = "/destroy/{id}")
	public ResponseEntity<APIResponse> unindexDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		return apiUtils.destroy(currentUser, new SimpleObjectIdentifier(id), indexService).toResponseEntity();
	} 
	
    @PostMapping(value = "/recreate/{id}")
	public ResponseEntity<APIResponse> recreate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.recreate(currentUser, new SimpleObjectIdentifier(id), indexService, datasetService).toResponseEntity();
	}

    @GetMapping("/get-all-my")
	public ResponseEntity<APIResponse> getAllByUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, indexService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	} 
    
    @GetMapping("/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, indexService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	} 
}
