package ac.software.semantic.controller;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Optional;


import ac.software.semantic.payload.*;
import ac.software.semantic.service.TokenPasswordResetService;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.security.core.annotation.AuthenticationPrincipal;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.UserType;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.model.User;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.UserService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

//import javax.xml.ws.Response;

@RestController
@RequestMapping("/api/users")
@Tag(name = "User API", description = "User Management API Methods")
public class APIUserController {

	@Autowired
	private UserService userService;

	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfiguration;
    
	@Autowired
	private TokenPasswordResetService tokenPasswordResetService;

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
		}

		return new UserSummary(currentUser.getId(), currentUser.getUsername(), currentUser.getType());
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
			NewUserSummary newUser = userService.createUser(userRequest.getEmail(), userRequest.getPassword(), userRequest.getName(), UserType.VALIDATOR);
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

	
	@Operation(summary = "Get Validator list of current logged in user (editor)", security = @SecurityRequirement(name = "bearerAuth"))
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "Validator List Rendered",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = UsersResponse.class))}),
		@ApiResponse(responseCode = "500", description = "Error",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = ErrorResponse.class))})
	})
	@GetMapping("/getValidatorsOfEditor")
	public ResponseEntity<?> getValidatorsOfLoggedInEditor(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			if (currentUser.getType() != UserType.EDITOR) {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Logged in user is not editor!"));
			}
			List<NewUserSummary> validatorList = userService.getValidatorsOfEditor(currentUser.getId());
			if (validatorList != null) {
				return ResponseEntity.ok(new UsersResponse(validatorList));
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}

	@Operation(summary = "Get List Of Public Editors")
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "Editor List Rendered",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = PublicEditorsResponse.class))}),
		@ApiResponse(responseCode = "500", description = "Error",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = ErrorResponse.class))})
	})
	@GetMapping("/getPublicEditors")
	public ResponseEntity<?> getPublicEditors() {
		try {
			List<EditorItem> publicEditors = userService.getPublicEditors();
			if (publicEditors != null) {
				return ResponseEntity.ok(new PublicEditorsResponse(publicEditors));
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}

	@Operation(summary = "Validator makes himself available for a certain editor", security = @SecurityRequirement(name = "bearerAuth"))
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "Operation succeeded",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = ErrorResponse.class))}),
		@ApiResponse(responseCode = "500", description = "Operation failed. Server Error",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = ErrorResponse.class))})
	})
	@PostMapping("/joinEditor")
	public ResponseEntity<?> assignValidatorToEditor(
		@Parameter(description = "The userId of editor that validator has applied for", example = "132") @RequestParam String editorId,
		@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
			try {
				boolean success = userService.addValidatorToEditor(editorId, currentUser.getId());
				if (success) {
					return ResponseEntity.ok(new ErrorResponse("Validator successfully added to editor"));
				}
				else {
					return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
				}				
			}
			catch (Exception e) {
				e.printStackTrace();
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
			}
	}

	@Operation(summary = "Editor removes a validatorId from his validatorList", security = @SecurityRequirement(name = "bearerAuth"),
				description = "This call also removes access entries concerning that specific editor - validator combination")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "Operation succeeded",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))}),
			@ApiResponse(responseCode = "500", description = "Operation failed. Server Error",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	@PostMapping("/removeValidator")
	public ResponseEntity<?> removeValidatorFromEditor(
			@Parameter(description = "The userId of the validator that will be removed", example = "1234") @RequestParam String validatorId,
			@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			boolean success = userService.removeValidatorFromEditor(validatorId, currentUser.getId());
			if (success) {
				return ResponseEntity.ok(new ErrorResponse("Validator successfully removed from editor"));
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}

	@Operation(summary = "Get editors of logged in validator", security = @SecurityRequirement(name = "bearerAuth"))
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "Editor List Rendered",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = PublicEditorsResponse.class))}),
			@ApiResponse(responseCode = "500", description = "Error",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	@GetMapping("/getEditorsOfValidator")
	public ResponseEntity<?> getEditorsOfValidator(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			List<EditorItem> editors = userService.getEditorsOfValidator(currentUser.getId());
			return ResponseEntity.ok(new PublicEditorsResponse(editors));
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
							schema = @Schema(implementation = UserDetails.class))}),
			@ApiResponse(responseCode = "500", description = "Error",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	@GetMapping("/get")
	public ResponseEntity<?> getUserDetails(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		try {
			UserDetails res = userService.getUserById(currentUser.getId());
			if (res != null) {
				return ResponseEntity.ok(res);
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

	public enum UserDetailsUpdateOptions {
		EMAIL,
		NAME,
		JOBDESCRIPTION,
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
			UserDetails res = userService.updateUser(currentUser.getId(), request.getTarget(), request);
			if (res != null) {
				return ResponseEntity.ok(res);
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


}
