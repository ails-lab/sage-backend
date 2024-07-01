package ac.software.semantic.payload.request;

public class UserRequest {
    private String username;
    private String password;
    
//    private UserType type;

    public UserRequest(String username, String password) {
        this.username = username;
        this.password = password;
//        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

//	public UserType getType() {
//		return type;
//	}
//
//	public void setType(UserType type) {
//		this.type = type;
//	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
