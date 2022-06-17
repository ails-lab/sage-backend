package ac.software.semantic.payload;

import ac.software.semantic.model.User;

public class EditorItem {
    private String id;
    private String jobDescription;

    public EditorItem (String userId, String jobDescription) {
        this.id = userId;
        this.jobDescription = jobDescription;
    }

    public EditorItem (User user) {
        this.id = user.getId().toString();
        this.jobDescription = user.getJobDescription();
    }

    public String getId() {
        return this.id;
    }

    public void setId(String userId) {
        this.id = userId;
    }

    public String getJobDescription() {
        return this.jobDescription;
    }

    public void setJobDescription(String jobDescription) {
        this.jobDescription = jobDescription;
    }
}