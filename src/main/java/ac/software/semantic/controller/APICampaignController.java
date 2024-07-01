package ac.software.semantic.controller;

import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.request.CampaignUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.DatasetResponse;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.modifier.CampaignResponseModifier;
import ac.software.semantic.payload.response.modifier.DatasetResponseModifier;
import ac.software.semantic.service.CampaignService;
import ac.software.semantic.service.CampaignService.CampaignContainer;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.UserService;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.ProjectService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.lookup.CampaignLookupProperties;
import ac.software.semantic.service.lookup.PrototypeLookupProperties;
import io.swagger.v3.oas.annotations.Parameter;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.constants.state.CampaignState;
import ac.software.semantic.model.constants.type.CampaignType;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;

@RestController
@RequestMapping("/api/campaign")
public class APICampaignController {

	@Autowired
	private DatasetService datasetService;
	
	@Autowired
	private UserService userService;
	
	@Autowired
	private CampaignService campaignService;

	@Autowired
	private ProjectService projectService;

	@Autowired
	private APIUtils apiUtils;
	
    @PostMapping(value = "/create")
	public ResponseEntity<APIResponse> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam(required = false) ObjectId projectId, @RequestBody CampaignUpdateRequest ur) {
    	return apiUtils.cnew(currentUser, projectId != null ? new SimpleObjectIdentifier(projectId) : null, ur, campaignService, projectService).toResponseEntity();		                               
	}

    @GetMapping(value = "/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false) Boolean details)  {
    	return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), campaignService, details != null && details ? CampaignResponseModifier.fullModifier() : null).toResponseEntity();
	} 
    
    @DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable ObjectId id) {
    	return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), campaignService).toResponseEntity();
	}

    @PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable ObjectId id, @RequestBody CampaignUpdateRequest ur) {
    	return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, campaignService).toResponseEntity();
	}

    @PostMapping(value = "/add-to-project/{id}")
	public ResponseEntity<APIResponse> addToProject(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId projectId)  {
    	return apiUtils.addAsMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(projectId), campaignService, projectService).toResponseEntity();
	} 

    @PostMapping(value = "/remove-from-project/{id}")
	public ResponseEntity<APIResponse> removeFromProject(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId projectId)  {
    	return apiUtils.removeFromMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(projectId), campaignService, projectService).toResponseEntity();
	}
    
	@PostMapping(value = "/join/{id}")
	public ResponseEntity<APIResponse> join(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.addMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(new ObjectId(currentUser.getId())), campaignService, userService).toResponseEntity();
	}
	
	@PostMapping(value = "/unjoin/{id}")
	public ResponseEntity<APIResponse> unjoin(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false) ObjectId userId) {
		return apiUtils.removeMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(userId != null ? userId : new ObjectId(currentUser.getId())), campaignService, userService).toResponseEntity();
	}

	@PostMapping(value = "/get-members/{id}")
	public ResponseEntity<APIResponse> getMembers(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.getMembers(currentUser, new SimpleObjectIdentifier(id), campaignService, userService, null).toResponseEntity();
	}
	
	
	@GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                    @RequestParam CampaignType type, 
			                                    @RequestParam(required = false) ObjectId projectId,
			                                    @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		CampaignLookupProperties lp = new CampaignLookupProperties();
		lp.setCampaignType(type);
		
		CampaignResponseModifier rm = new CampaignResponseModifier();
		rm.setValidators(ResponseFieldType.EXPAND);

		if (projectId != null) {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, Arrays.asList(new ObjectId[] {projectId}), lp, campaignService, projectService, rm, apiUtils.pageable(page, size))).toResponseEntity();
		} else {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, null, lp, campaignService, projectService, rm, apiUtils.pageable(page, size))).toResponseEntity();
		}
		
	}
	
	@GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam CampaignType type, 
			                                  @RequestParam(required = false) ObjectId projectId,
			                                  @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		CampaignLookupProperties lp = new CampaignLookupProperties();
		lp.setCampaignType(type);
		
		CampaignResponseModifier rm = new CampaignResponseModifier();
		rm.setValidators(ResponseFieldType.EXPAND);
		
		if (projectId != null) {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {projectId}), lp, campaignService, projectService, rm, apiUtils.pageable(page, size))).toResponseEntity();
		} else {
			return APIResponse.result(apiUtils.getAllByUser(currentUser, null, null, lp, campaignService, projectService, rm, apiUtils.pageable(page, size))).toResponseEntity();
		}
	}
	
	
	
	
	@GetMapping(value = "/get-all-active")
	public ResponseEntity<APIResponse> getActiveCampaigns(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                              @RequestParam CampaignType type,
			                                              @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size) {
		
		CampaignLookupProperties lp = new CampaignLookupProperties();
		lp.setCampaignType(type);
		lp.setCampaignState(CampaignState.ACTIVE);
		
		return APIResponse.result(apiUtils.getAll(lp, campaignService, apiUtils.pageable(page, size))).toResponseEntity();
	}
	
	@GetMapping(value = "/get-all-joined")
	public ResponseEntity<APIResponse> getAllJoined(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam CampaignType type,
			                                        @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size) {

		CampaignLookupProperties lp = new CampaignLookupProperties();
		lp.setCampaignType(type);
		lp.setCampaignState(CampaignState.ACTIVE);
		lp.setValidatorId(new ObjectId(currentUser.getId()));
		
		return APIResponse.result(apiUtils.getAll(lp, campaignService, apiUtils.pageable(page, size))).toResponseEntity();
	}
	
	@PostMapping(value = "/add-dataset/{id}")
	public ResponseEntity<APIResponse> addDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId datasetId) {
		return apiUtils.addMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(datasetId), campaignService, datasetService, CampaignResponseModifier.fullModifier()).toResponseEntity();
	}
	
	@PostMapping(value = "/remove-dataset/{id}")
	public ResponseEntity<APIResponse> removeDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId datasetId) {
		return apiUtils.removeMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(datasetId), campaignService, datasetService).toResponseEntity();
	}
	
	@PostMapping(value = "/assign-dataset-to-user/{id}")
	public ResponseEntity<APIResponse> assingDatasetToUser( @Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId datasetId, @RequestParam ObjectId userId) {
		return apiUtils.assign(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(datasetId), new SimpleObjectIdentifier(userId), campaignService, datasetService, DatasetResponseModifier.baseModifier()).toResponseEntity();
	}

	@PostMapping(value = "/assign-all-datasets-to-user/{id}")
	public ResponseEntity<APIResponse> assignAllDatasetsToUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId userId) {
		return apiUtils.assignAll(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(userId), campaignService, datasetService, DatasetResponseModifier.baseModifier()).toResponseEntity();
	}

	@PostMapping(value = "/unassign-dataset-from-user/{id}")
	public ResponseEntity<APIResponse> unassignDatasetFromUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId datasetId, @RequestParam ObjectId userId) {
		return apiUtils.unassign(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(datasetId), new SimpleObjectIdentifier(userId), campaignService, datasetService, DatasetResponseModifier.baseModifier()).toResponseEntity();
	}	

	@PostMapping(value = "/unassign-all-datasets-from-user/{id}")
	public ResponseEntity<APIResponse> unassignAllDatasetFromUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId userId) {
		return apiUtils.unassignAll(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(userId), campaignService, datasetService).toResponseEntity();
	}

	@GetMapping(value = "/get-assigned-datasets-to-user/{id}")
	public ResponseEntity<APIResponse> getMyAssignedDatasets(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId userId) {
		return apiUtils.getAllAssigned(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(userId), campaignService, datasetService, DatasetResponseModifier.baseModifier()).toResponseEntity();
	}

	@GetMapping(value = "/get-my-assigned-datasets/{id}")
	public ResponseEntity<APIResponse> getMyAssignedDatasets(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.getAllAssigned(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(new ObjectId(currentUser.getId())), campaignService, datasetService, DatasetResponseModifier.baseModifier()).toResponseEntity();
	}

	@PostMapping(value = "/add-validator/{id}")
	public ResponseEntity<APIResponse> addUserToCampaign(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId userId) {
		return apiUtils.addMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(userId), campaignService, userService, CampaignResponseModifier.fullModifier()).toResponseEntity();
	}
	
	@GetMapping(value = "/get-paged-annotation-validations/{id}", produces = "application/json")
	public ResponseEntity<APIResponse> datasetProgress(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam ObjectId datasetId) {
		
		try {
			CampaignContainer cc = campaignService.getContainer(currentUser, new SimpleObjectIdentifier(id));
	    	if (cc == null) {
	    		return APIResponse.notFound(CampaignContainer.class).toResponseEntity();
	    	}
	    	
			return APIResponse.result(cc.getDatasetProgress(datasetId)).toResponseEntity();
	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}

}
