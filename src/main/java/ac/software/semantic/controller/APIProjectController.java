package ac.software.semantic.controller;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
import ac.software.semantic.model.Database;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.payload.request.DatasetUpdateRequest;
import ac.software.semantic.payload.request.ProjectUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.modifier.DatasetResponseModifier;
import ac.software.semantic.payload.response.modifier.ProjectResponseModifier;
import ac.software.semantic.repository.core.ProjectDocumentRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ProjectService;
import ac.software.semantic.service.UserService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.lookup.ProjectLookupProperties;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Project API")

@RestController
@RequestMapping("/api/project")
public class APIProjectController {
    
	@Autowired
    @Qualifier("database")
    private Database database;
	
	@Autowired
    private ProjectService projectService;

	@Autowired
    private UserService userService;

	@Autowired
	private APIUtils apiUtils;

	@GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                    @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, projectService, apiUtils.pageable(page, size))).toResponseEntity();
	}
	
	@GetMapping(value = "/get-all-joined")
	public ResponseEntity<APIResponse> getAllJoined(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                        @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
    	ProjectLookupProperties lp = new ProjectLookupProperties();
		lp.setJoinedUserId(new ObjectId(currentUser.getId()));

		return APIResponse.result(apiUtils.getAll(lp, projectService, apiUtils.pageable(page, size))).toResponseEntity();
	}
	
	@GetMapping(value = "/get-all-other-public")
	public ResponseEntity<APIResponse> getAllOtherPublic(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                             @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
		
    	ProjectLookupProperties lp = new ProjectLookupProperties();
		lp.setPublik(true);
		lp.setUserIdNot(new ObjectId(currentUser.getId()));

		return APIResponse.result(apiUtils.getAll(lp, projectService, apiUtils.pageable(page, size))).toResponseEntity();
	}

	@PostMapping(value = "/new")
	public ResponseEntity<APIResponse> cnew(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody ProjectUpdateRequest ur)  {
		return apiUtils.cnew(currentUser, ur, projectService).toResponseEntity();
	}

	@GetMapping(value = "/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false) Boolean details)  {
		
    	ProjectResponseModifier rm = new ProjectResponseModifier();

    	if (details != null && details) {
    		rm.setJoinedUsers(ResponseFieldType.EXPAND);
    	}    		
    		
    	return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), projectService, rm).toResponseEntity();
	}	
	
	@DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), projectService).toResponseEntity();
	}
	
    @PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody ProjectUpdateRequest ur)  {
//    	idService.remove(ur.getIdentifier());
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, projectService).toResponseEntity();
	}    

	@PostMapping(value = "/join/{id}")
	public ResponseEntity<APIResponse> join(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.addMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(new ObjectId(currentUser.getId())), projectService, userService).toResponseEntity();
	}
	
	@PostMapping(value = "/unjoin/{id}")
	public ResponseEntity<APIResponse> unjoin(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false) ObjectId userId) {
		return apiUtils.removeMember(currentUser, new SimpleObjectIdentifier(id), new SimpleObjectIdentifier(userId != null ? userId : new ObjectId(currentUser.getId())), projectService, userService).toResponseEntity();
	}
	
	@PostMapping(value = "/get-members/{id}")
	public ResponseEntity<APIResponse> getMembers(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		return apiUtils.getMembers(currentUser, new SimpleObjectIdentifier(id), projectService, userService, null).toResponseEntity();
	}
	
    @GetMapping(value = "/exists-identifier",
            produces = "application/json")
	public ResponseEntity<APIResponse> existsIdentifier(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true)  @RequestParam String identifier)  {
		
    	ProjectDocument ur = new ProjectDocument(database);
    	ur.setIdentifier(identifier);
    	
    	return apiUtils.identifierConflict(ur, IdentifierType.IDENTIFIER, projectService).toResponseEntity();
	}
}
