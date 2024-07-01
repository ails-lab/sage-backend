package ac.software.semantic.payload.request;
import ac.software.semantic.model.constants.type.UserRoleType;

public class UserUpdateRequest implements UpdateRequest {

    private String email;

    private String password;

    private String name;
    
    private String oldPassword;
    
    private UserRoleType role;
    
    private String token;
    
    public UserUpdateRequest() { 
    	
    }
    
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

	public UserRoleType getRole() {
		return role;
	}

	public void setRole(UserRoleType role) {
		this.role = role;
	}

	public String getOldPassword() {
		return oldPassword;
	}

	public void setOldPassword(String oldPassword) {
		this.oldPassword = oldPassword;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}


}