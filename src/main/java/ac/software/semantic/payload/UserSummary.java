package ac.software.semantic.payload;

import ac.software.semantic.model.constants.UserRoleType;

public class UserSummary {
    private String id;
    private String username;
    
    private UserRoleType type;
    
//    private boolean validationAssigner;

    public UserSummary(String id, String username, UserRoleType type) {
        this.id = id;
        this.username = username;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

	public UserRoleType getType() {
		return type;
	}

	public void setType(UserRoleType type) {
		this.type = type;
	}

//	public boolean isValidationAssigner() {
//		return validationAssigner;
//	}
//
//	public void setValidationAssigner(boolean validationAssigner) {
//		this.validationAssigner = validationAssigner;
//	}

}
