package ac.software.semantic.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import ac.software.semantic.payload.*;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.UserRepository;
import ac.software.semantic.repository.UserRoleRepository;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.TokenPasswordResetService;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.User;
import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.constants.UserRoleType;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.UserService;
import ac.software.semantic.service.UserSessionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
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
	private DatasetRepository datasetRepository;

	@Autowired
	private UserSessionService userSessionService;

	@Autowired
	private AnnotationEditRepository annotationEditRepository;

	
	@Autowired
	private UserService userService;

	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfiguration;
    
	@Autowired
	private TokenPasswordResetService tokenPasswordResetService;
	
	@Autowired
	private ModelMapper modelMapper;
	

//	private static ObjectMapper mapper = new ObjectMapper();

//    @RequestMapping(value = "/users/signin",
//    		        method = RequestMethod.POST,
//    		        consumes = "application/json",
//    		        produces = "application/json")
//	public ResponseEntity<String> signin(HttpServletRequest request, @RequestBody ObjectNode json)  {
//
//
//    	ObjectNode result = mapper.createObjectNode();
//    	
//		String username = json.get("username").asText();
//		String password = json.get("password").asText();
//		
//		System.out.println(username + " " + password);
//		
//		User user = userRepository.findOneByUsername(username);
//		
//		if (user == null) {
//			result.put("error", "Invalid username or password.");
//			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result.toString());
//		}
//		
//		if (!passwordEncoder.matches(password, user.getBCryptPassword())) {
//			result.put("error", "Invalid username or password.");
//			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result.toString());
//		}
//		
//		
//		signin(request.getSession(), user);
//		
//		result.put("success", "User " + username + " signed in.");
//		
//		return ResponseEntity.status(HttpStatus.OK).body(result.toString());
//	}
	@Operation(summary = "Old API endpoint, backwards-compatible, not sure if works or what it does")
	@GetMapping("/me")
//    @PreAuthorize("hasRole('USER')")
	public UserSummary getCurrentUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) throws Exception {

		if (currentUser != null) {
//			System.out.println("not null");
			checkPaths(currentUser);
			
			UserSummary us = new UserSummary(currentUser.getId(), currentUser.getUsername(), currentUser.getType());
//			us.setValidationAssigner(roleService.isPublicEditor(new ObjectId(currentUser.getId())));
			
			return us;
		}
		
		return null;
	}

	private void checkPaths(UserPrincipal currentUser) throws Exception {
		File df = new File(fileSystemConfiguration.getDataFolder());
		if (!df.exists()) {
			throw new Exception("Data folder " + df.getAbsolutePath() + " does not exits");
		}

		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser));
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "mappings");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "mappings/executions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "mappings/uploaded-files");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "annotations");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "annotations/executions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "annotations/manual");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "vocabularizers");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "vocabularizers/executions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "datasets");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "datasets/distributions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "embeddings");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "embeddings/executions");
		df.mkdir();
		
	}

	@Operation(summary = "Create a new user")
	@ApiResponses(value = {
		@ApiResponse(responseCode = "201", description = "User Created",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = NewUserSummary.class))}),
		@ApiResponse(responseCode = "400", description = "User Creation failed",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = ErrorResponse.class))})
	})
	@PostMapping("/create")
	public ResponseEntity<?> createUserV2(@Parameter(required = true,
		schema = @Schema(implementation = NewUserRequest.class))  @RequestBody NewUserRequest userRequest) {
		try {
			NewUserSummary newUser = userService.createUser(userRequest.getEmail(), userRequest.getPassword(), userRequest.getName(), UserRoleType.VALIDATOR);
			if (newUser != null) {
				return ResponseEntity.ok(newUser);
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Email already exists in database"));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}	
	}

	@Operation(summary = "Get User Details")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "User Details Successfully Rendered",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = UserResponse.class))}),
			@ApiResponse(responseCode = "500", description = "Error",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	@GetMapping("/get")
	public ResponseEntity<?> getUserDetails(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			Optional<User> userOpt = userRepository.findById(currentUser.getId());
			
			if (!userOpt.isPresent()) {
				return new ResponseEntity<>(APIResponse.FailureResponse(), HttpStatus.NOT_FOUND);
			}
	
			return ResponseEntity.ok(modelMapper.user2UserResponse(userOpt.get()));
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	   
	}
	
	@GetMapping("/get-all")
	public ResponseEntity<?> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			List<User> users = userRepository.findByDatabaseId(database.getId());
			
			List<UserResponse> res = new ArrayList<>();
			
			for (User user : users) {
				UserResponse ur = modelMapper.user2UserResponse(user);
				res.add(ur);
			}
			
			return ResponseEntity.ok(res);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	  
	}

	@GetMapping("/get-info/{id}")
	public ResponseEntity<?> getUserInfo(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id) {
		try {
			Optional<User> userOpt = userRepository.findById(id);
			
			if (!userOpt.isPresent()) {
				return new ResponseEntity<>(APIResponse.FailureResponse(), HttpStatus.NOT_FOUND);
			}
			
			User user = userOpt.get();
			
			Optional<UserRole> roleOpt = userRoleRepository.findByDatabaseIdAndUserId(database.getId(), user.getId());
			
			List<UserRoleType> roles = null;
			if (roleOpt.isPresent()) {
				roles = roleOpt.get().getRole();
			}
			
			UserResponse ur = modelMapper.user2UserResponse(user, roles);
			ur.setDatasetCount(new Long(datasetRepository.findByUserIdAndDatabaseId(user.getId(), database.getId()).size()));
			ur.setInOtherDatabases(user.getDatabaseId().size() > 1);
			
			List<Dataset> datasets = datasetRepository.findByDatabaseId(database.getId());
			
			List<String> datasetUuids = datasets.stream().map(e -> e.getUuid()).collect(Collectors.toList());
			
			ur.setAnnotationEditAcceptCount(annotationEditRepository.countAcceptedByUserIdAndDatasetsUuid(user.getId(), datasetUuids));
			ur.setAnnotationEditRejectCount(annotationEditRepository.countRejectedByUserIdAndDatasetsUuid(user.getId(), datasetUuids));
			ur.setAnnotationEditAddCount(annotationEditRepository.countAddedByUserIdAndDatasetsUuid(user.getId(), datasetUuids));
			
			return ResponseEntity.ok(ur);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	  
	}
	
	@PostMapping("/update-roles/{id}")
	public ResponseEntity<?> updateRoles(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody List<UserRoleType> newRoles) {
		try {
			Optional<User> userOpt = userRepository.findById(id);
			
			if (!userOpt.isPresent()) {
				return new ResponseEntity<>(APIResponse.FailureResponse("User not found."), HttpStatus.NOT_FOUND);
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
			
			return new ResponseEntity<>(APIResponse.SuccessResponse("User updated sucessfully."), HttpStatus.OK);
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse("User update failed."), HttpStatus.INTERNAL_SERVER_ERROR);
		}	  
	}
	
//	@PostMapping("/admin-create")
//	public ResponseEntity<?> adminCreateUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @RequestBody NewUserRequest userRequest) {
//		try {
//			NewUserSummary newUser = userService.createUser(userRequest.getEmail(), userRequest.getPassword(), userRequest.getName(), UserType.VALIDATOR);
//			if (newUser != null) {
//				return ResponseEntity.ok(newUser);
//			}
//			else {
//				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Email already exists in database"));
//			}
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
//		}	
//	}
	
	public enum UserDetailsUpdateOptions {
		EMAIL,
		NAME,
//		JOBDESCRIPTION,
		PASSWORD,
		PUBLIC
	}

	@Operation(summary = "Change User Details")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "User Details Successfully Changed",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = UserSummary.class))}),
			@ApiResponse(responseCode = "500", description = "Error",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	@PutMapping("/me")
	public ResponseEntity<?> changeUserDetails(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
											   @RequestBody ChangeUserDetailsRequest request) {
		try {
			User res = userService.updateUser(currentUser.getId(), request.getTarget(), request);
			
			if (res != null) {
				return ResponseEntity.ok(modelMapper.user2UserResponse(res));
			}
			else {
				return ResponseEntity.status(500).body(null);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}

	@Operation(summary = "Request For Token to Reset User Password")
	@PostMapping("/resetPasswordRequest")
	public ResponseEntity<?> resetPasswordRequest(@RequestParam("email") String email) {
		try {
			User user = userService.getUserByEmail(email);
			if (user == null) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
			}
			String tkn = tokenPasswordResetService.createToken(user);
			tokenPasswordResetService.sendMail(user, tkn);
			return ResponseEntity.ok("{}");
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
//		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("{}");
	}

	@Operation(summary = "Save new password (will succeed if token is OK.")
	@PostMapping("/savePassword")
	public ResponseEntity<?> savePassword(@RequestBody PasswordResetRequest request) {
		try {
			boolean success = userService.changeUserPasswordFromToken(request.getNewPassword(), request.getToken());
			if (success) {
				return  ResponseEntity.ok("{}");
			} else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}

	}
	
	@PostMapping("/logout")
	public ResponseEntity<?> changeUserDetails(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			boolean loggeOut = userSessionService.logout(new ObjectId(currentUser.getId()));
			
			if (loggeOut) {
				return new ResponseEntity<>(APIResponse.SuccessResponse("User sucessfully logged out."), HttpStatus.OK);
			} else {
				return new ResponseEntity<>(APIResponse.SuccessResponse("User not found."), HttpStatus.NOT_FOUND);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}


}
