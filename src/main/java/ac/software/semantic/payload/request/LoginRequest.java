package ac.software.semantic.payload.request;


import javax.validation.constraints.NotBlank;

import ac.software.semantic.model.constants.type.UserRoleType;

public class LoginRequest {
    @NotBlank
    private String email;

    @NotBlank
    private String password;
    
    private UserRoleType role; 

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

	public UserRoleType getRole() {
		return role;
	}

	public void setRole(UserRoleType role) {
		this.role = role;
	}
}

