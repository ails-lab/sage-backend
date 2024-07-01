package ac.software.semantic.controller;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import ac.software.semantic.payload.request.UserUpdateRequest;
import ac.software.semantic.payload.notification.NotificationObject;
import ac.software.semantic.payload.request.InviteUserRequest;
import ac.software.semantic.payload.request.LoginRequest;
import ac.software.semantic.payload.request.PasswordResetRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.UserResponse;
import ac.software.semantic.payload.response.modifier.UserResponseModifier;
import ac.software.semantic.repository.core.UserRepository;
import ac.software.semantic.repository.core.UserRoleRepository;
import ac.software.semantic.service.FolderService;
import ac.software.semantic.service.TokenService;
import ac.software.semantic.service.UserRoleService;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.ActionToken;
import ac.software.semantic.model.User;
import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.UserRoleDefault;
import ac.software.semantic.model.constants.notification.NotificationChannel;
import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.TokenType;
import ac.software.semantic.model.constants.type.UserRoleType;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.JwtAuthenticationResponse;
import ac.software.semantic.security.JwtTokenProvider;
import ac.software.semantic.security.SelectRoleResponse;
import ac.software.semantic.security.SigninSucessResponse;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.UserService;
import ac.software.semantic.service.UserService.UserContainer;
import ac.software.semantic.service.UserSessionService;
import ac.software.semantic.service.WebSocketService;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.exception.ContainerNotFoundException;
import ac.software.semantic.service.lookup.UserLookupProperties;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;


@RestController
@RequestMapping("/api/users")
@Tag(name = "User API", description = "User Management API Methods")
public class APIUserController {
	
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private UserRepository userRepository;

	@Autowired
	private UserRoleRepository userRoleRepository;

	@Autowired
	private UserSessionService userSessionService;
	
	@Autowired
	private UserService userService;

	@Autowired
	private FolderService folderService;

	@Autowired
	private TokenService tokenService;
	
	@Autowired
	private APIUtils apiUtils;

	@Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private JwtTokenProvider tokenProvider;

	@Autowired
	private UserRoleService roleService;

    @Autowired
    private WebSocketService webSocketService;
    
	@GetMapping(value = "/get")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		return apiUtils.get(currentUser, new SimpleObjectIdentifier(new ObjectId(currentUser.getId())), userService).toResponseEntity();
	}
	
	@GetMapping(value = "/details/{id}")
	public ResponseEntity<APIResponse> getUserInfo(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) {
		UserResponseModifier rm = new UserResponseModifier();
		
//		rm.setUserRole(ResponseFieldType.EXPAND);
		rm.setDatasetCount(ResponseFieldType.EXPAND);
		rm.setInOtherDatabases(ResponseFieldType.EXPAND);
		rm.setAnnotationEditAcceptCount(ResponseFieldType.EXPAND);
		rm.setAnnotationEditRejectCount(ResponseFieldType.EXPAND);
		rm.setAnnotationEditAddCount(ResponseFieldType.EXPAND);

		return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), userService, rm).toResponseEntity();
 
	}
	
	@GetMapping(value = "/get-all")
	public ResponseEntity<?> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			                        @RequestParam(required = false) UserRoleType role,			
			                        @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size) {
		
		UserLookupProperties lp = null;
		if (role != null) {
			lp = new UserLookupProperties();
			lp.setUserRoleType(role);
		}
		
		return APIResponse.result(apiUtils.getAll(lp, userService, apiUtils.pageable(page, size))).toResponseEntity();
	}
	
	@PostMapping(value = "/new")
	public ResponseEntity<APIResponse> cnew(@RequestBody UserUpdateRequest ur) {
		ur.setRole(UserRoleType.VALIDATOR);

		return apiUtils.cnew(null, ur, userService).toResponseEntity();
	}
	
	@PostMapping(value = "/update")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody UserUpdateRequest ur)  {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(new ObjectId(currentUser.getId())), ur, userService).toResponseEntity();
	}
	
	@Operation(summary = "Get current logged in user details")
	@GetMapping(value = "/me")
	public ResponseEntity<APIResponse> getCurrentUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) throws Exception {
		
//		System.out.println("CC " + currentUser);
		if (currentUser != null) {
		
			UserResponse ur = new UserResponse();
			ur.setId(currentUser.getId());
			ur.setEmail(currentUser.getUsername());
			ur.setName(currentUser.getName());
			ur.setRole(currentUser.getRole());
			ur.setRoles(currentUser.getRoles());
			
			UserRoleDefault urd = currentUser.getUserRoleDefault();
			if (urd != null) {
				ur.setDefaultProjectId(urd.getDefaultProjectId().toString());
			}
	
			return APIResponse.result(ur).toResponseEntity();
		} else {
			return APIResponse.notFound().toResponseEntity();
		}
	}

	@PostMapping(value = "/update-roles/{id}")
	public ResponseEntity<APIResponse> updateRoles(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody List<UserRoleType> newRoles) {
		try {
			Optional<User> userOpt = userRepository.findById(id);
			
			if (!userOpt.isPresent()) {
				return APIResponse.FailureResponse("User not found.", HttpStatus.NOT_FOUND).toResponseEntity();
			}
			
			User user = userOpt.get();
			
			Optional<UserRole> roleOpt = userRoleRepository.findByDatabaseIdAndUserId(database.getId(), user.getId());
			
			UserRole role;
			if (roleOpt.isPresent()) {
				role = roleOpt.get();
			} else {
				role = new UserRole();
				role.setDatabaseId(database.getId());
				role.setUserId(user.getId());
			}
			
			role.setRole(newRoles);
			
			userRoleRepository.save(role);
			
			return APIResponse.SuccessResponse("User updated sucessfully.", HttpStatus.OK).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	  
	}

	@Operation(summary = "Request For Token to Reset User Password")
	@PostMapping(value = "/invite-user-request")
	public ResponseEntity<APIResponse> inviteUserRequest(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody InviteUserRequest request) {
		try {
			UserContainer oc = (UserContainer)apiUtils.exists(currentUser, new SimpleObjectIdentifier(new ObjectId(currentUser.getId())), userService);
			
			ActionToken token = tokenService.createSignUpToken(oc, request);
			tokenService.sendMail(request.getEmail(), token);
			
			return APIResponse.ok().toResponseEntity();
		
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}			

	}

	@Operation(summary = "Request For Token to Reset User Password")
	@PostMapping(value = "/reset-password-request")
	public ResponseEntity<APIResponse> resetPasswordRequest(@RequestParam("email") String email) {
		try {
			User user = userService.getUserByEmail(email);
			if (user == null) {
				return APIResponse.notFound().toResponseEntity();
			}
			
			ActionToken token = tokenService.createPasswordResetToken(user);
			tokenService.sendMail(email, token);
			
			return APIResponse.ok().toResponseEntity();
		}
		catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}

	@Operation(summary = "Save new password (will succeed if token is OK.")
	@PostMapping(value = "/save-password")
	public ResponseEntity<APIResponse> savePassword(@RequestBody PasswordResetRequest request) {
		try {
			userService.changeUserPasswordFromToken(request.getNewPassword(), request.getToken());

			return  APIResponse.ok().toResponseEntity();
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest(ex).toResponseEntity();
		}
	}
	
	@PostMapping("/signin")
    public ResponseEntity<APIResponse> signin(@Valid @RequestBody LoginRequest loginRequest, HttpServletRequest request) {

		try {
			if (loginRequest.getRole() == null) {
		        
				Authentication authentication = authenticationManager.authenticate(
		                new UsernamePasswordAuthenticationToken(
		                        loginRequest.getEmail(),
		                        loginRequest.getPassword()));
		        
		       	UserPrincipal userDetails = (UserPrincipal) authentication.getPrincipal();
	        	
		       	List<UserRoleType> roles = roleService.getLoginRolesForUser(new ObjectId(userDetails.getId()));
		       	
		       	if (roles.size() == 1) {
		       		authentication = authenticationManager.authenticate(
		                       new UsernamePasswordAuthenticationToken(
		                               loginRequest.getEmail() + "~~~" + roles.get(0),
		                               loginRequest.getPassword()));
	
		       		loginRequest.setRole(roles.get(0));
		       		
					return APIResponse.result(signin(loginRequest, request, authentication)).toResponseEntity();

		       		
		       	} else {
		        	return APIResponse.selectSigninRole(new SelectRoleResponse(roles)).toResponseEntity();
		        }
		       	
			} else {
				
				Authentication authentication = authenticationManager.authenticate(
						new UsernamePasswordAuthenticationToken(
		                        loginRequest.getEmail() + "~~~" + loginRequest.getRole(),
		                        loginRequest.getPassword()));
				
				return APIResponse.result(signin(loginRequest, request, authentication)).toResponseEntity();

			}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
        
    }	
	
	private SigninSucessResponse signin(LoginRequest loginRequest, HttpServletRequest request, Authentication authentication) throws Exception {
   		
		SecurityContextHolder.getContext().setAuthentication(authentication);
   		UserPrincipal userDetails = (UserPrincipal) authentication.getPrincipal();
   		
   		Date date = userSessionService.login(new ObjectId(userDetails.getId()), request.getRemoteAddr()).getUserSession().getLogin();
		String jwt = tokenProvider.generateToken(authentication, date);

		if (!userDetails.allowMultiLogin()) {
			NotificationObject no = new NotificationObject(NotificationType.login, "NEW_LOGIN", (String)null);
			webSocketService.send(NotificationChannel.login, userDetails, no);
		}
		
		SigninSucessResponse response = new SigninSucessResponse();
		response.setToken(new JwtAuthenticationResponse(jwt));
		response.setRole(loginRequest.getRole());
		
		folderService.checkPaths(userDetails);
		
		return response;
	}

	@PostMapping(value = "/signout")
	public ResponseEntity<APIResponse> logout(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			boolean loggeOut = userSessionService.logout(new ObjectId(currentUser.getId()));
			
			if (loggeOut) {
				return APIResponse.SuccessResponse("User sucessfully logged out.", HttpStatus.OK).toResponseEntity();
			} else {
				return APIResponse.SuccessResponse("User not found.", HttpStatus.NOT_FOUND).toResponseEntity();
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}
	
    @GetMapping(value = "/exists-email",
            produces = "application/json")
	public ResponseEntity<APIResponse> existsEmail(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true)  @RequestParam String email)  {
		
    	User ur = new User(database);
    	ur.setEmail(email);
    	
    	return apiUtils.identifierConflict(ur, IdentifierType.EMAIL, userService).toResponseEntity();
	}
	
}
