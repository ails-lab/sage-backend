package ac.software.semantic.security;

import ac.software.semantic.model.constants.type.UserRoleType;

public class SigninSucessResponse {
    private JwtAuthenticationResponse token;
    private UserRoleType role;
    
	public JwtAuthenticationResponse getToken() {
		return token;
	}
	
	public void setToken(JwtAuthenticationResponse token) {
		this.token = token;
	}
	
	public UserRoleType getRole() {
		return role;
	}
	
	public void setRole(UserRoleType role) {
		this.role = role;
	}
    
    
}

