package ac.software.semantic.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.model.constants.UserRoleType;
import ac.software.semantic.payload.LoginRequest;
import ac.software.semantic.security.JwtAuthenticationResponse;
import ac.software.semantic.security.JwtTokenProvider;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.UserRoleService;
import ac.software.semantic.service.UserService;
import ac.software.semantic.service.UserSessionService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Authorization API")
@RestController
public class APIAuthController {

	@Autowired
    AuthenticationManager authenticationManager;

    @Autowired
    JwtTokenProvider tokenProvider;

	@Autowired
	private UserRoleService roleService;

	@Autowired
	private UserSessionService userSessionService;

	
	@PostMapping("/api/auth/signin")
    public ResponseEntity<?> signin(@Valid @RequestBody LoginRequest loginRequest, HttpServletRequest request) {

		if (loginRequest.getRole() == null) {
	        
			Authentication authentication = authenticationManager.authenticate(
	                new UsernamePasswordAuthenticationToken(
	                        loginRequest.getEmail(),
	                        loginRequest.getPassword()
	                )
	        );
	        
	       	UserPrincipal userDetails = (UserPrincipal) authentication.getPrincipal();
        	
	       	List<UserRoleType> roles = roleService.getLoginRolesForUser(new ObjectId(userDetails.getId()));
	       	if (roles.size() == 1) {
	       		authentication = authenticationManager.authenticate(
	                       new UsernamePasswordAuthenticationToken(
	                               loginRequest.getEmail() + "~~~" + roles.get(0),
	                               loginRequest.getPassword()));
	       		SecurityContextHolder.getContext().setAuthentication(authentication);


	       		userSessionService.login(new ObjectId(userDetails.getId()), request.getRemoteAddr());
				String jwt = tokenProvider.generateToken(authentication);
	       		return ResponseEntity.ok(new JwtAuthenticationResponse(jwt));
	       		
	       	} else {
	        	Map<String, List<UserRoleType>> map = new HashMap<>();
	        	map.put("roles", roles);
	        	return ResponseEntity.ok(map);
	        }
	       	
		} else {
			
			Authentication authentication = authenticationManager.authenticate(
					new UsernamePasswordAuthenticationToken(
	                        loginRequest.getEmail() + "~~~" + loginRequest.getRole(),
	                        loginRequest.getPassword()
	                )
	        );
			
       		SecurityContextHolder.getContext().setAuthentication(authentication);
       		UserPrincipal userDetails = (UserPrincipal) authentication.getPrincipal();
       		userSessionService.login(new ObjectId(userDetails.getId()), request.getRemoteAddr());

			String jwt = tokenProvider.generateToken(authentication);

       		return ResponseEntity.ok(new JwtAuthenticationResponse(jwt));
		}
        
    }	
	
}
