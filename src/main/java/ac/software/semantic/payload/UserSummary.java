package ac.software.semantic.payload;

import ac.software.semantic.model.UserType;

public class UserSummary {
    private String id;
    private String username;
    
    private UserType type;

    public UserSummary(String id, String username, UserType type) {
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

	public UserType getType() {
		return type;
	}

	public void setType(UserType type) {
		this.type = type;
	}

}
