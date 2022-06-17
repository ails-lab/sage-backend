package ac.software.semantic.payload;

import ac.software.semantic.model.User;
import ac.software.semantic.model.UserType;

public class UserDetails {

    private String email;
    private UserType userType;
    private String name;
    private String jobDescription;
    private boolean isPublic;

    public UserDetails(User user) {
        this.email = user.getEmail();
        this.userType = user.getType();
        this.name = user.getName();
        this.jobDescription = user.getJobDescription();
        this.isPublic = user.getIsPublic();
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public UserType getUserType() {
        return userType;
    }

    public void setUserType(UserType userType) {
        this.userType = userType;
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

    public boolean isPublic() {
        return isPublic;
    }

    public void setPublic(boolean aPublic) {
        isPublic = aPublic;
    }
}
