package ac.software.semantic.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.payload.LoginRequest;
import ac.software.semantic.security.JwtAuthenticationResponse;
import ac.software.semantic.security.JwtTokenProvider;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Authorization API")
@RestController
public class AuthController {

	@Autowired
    AuthenticationManager authenticationManager;

    @Autowired
    JwtTokenProvider tokenProvider;

    
	@PostMapping("/api/auth/signin")
    public ResponseEntity<?> signin(@Valid @RequestBody LoginRequest loginRequest) {

        Authentication authentication = authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(
                        loginRequest.getEmail(),
                        loginRequest.getPassword()
                )
        );

        SecurityContextHolder.getContext().setAuthentication(authentication);

        String jwt = tokenProvider.generateToken(authentication);
        
        
        return ResponseEntity.ok(new JwtAuthenticationResponse(jwt));
    }	
	
}
