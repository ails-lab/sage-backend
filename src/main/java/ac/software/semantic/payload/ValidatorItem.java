package ac.software.semantic.payload;

public class ValidatorItem {
    private int userId;
    private String jobDescription;

    public ValidatorItem (int userId, String jobDescription) {
        this.userId = userId;
        this.jobDescription = jobDescription;
    }

    public int getUserId() {
        return this.userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }


    public String getJobDescription() {
        return this.jobDescription;
    }

    public void setJobDescription(String jobDescription) {
        this.jobDescription = jobDescription;
    }
}