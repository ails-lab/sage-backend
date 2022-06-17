package ac.software.semantic.payload;

import ac.software.semantic.model.UserType;
import ac.software.semantic.model.User;

public class NewUserSummary {
    private String id;
    private String email;
    private String name;
    private String jobDescription;
    private UserType type;

    public NewUserSummary(String id, String email, UserType type, String name, String jobDescription) {
        this.id = id;
        this.email = email;
        this.type = type;
        this.name = name;
        this.jobDescription = jobDescription;
    }

    public NewUserSummary(User user) {
        this.id = user.getId().toString();
        this.email = user.getEmail();
        this.type = user.getType();
        this.name = user.getName();
        this.jobDescription = user.getJobDescription();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

	public UserType getType() {
		return type;
	}

	public void setType(UserType type) {
		this.type = type;
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobDescription() {
        return jobDescription;
    }

    public void setJobDescription(String jobDescription) {
        this.jobDescription = jobDescription;
    }

}
