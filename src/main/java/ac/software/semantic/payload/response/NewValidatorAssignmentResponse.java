package ac.software.semantic.payload.response;

public class NewValidatorAssignmentResponse {
    private Boolean success;
    
    public NewValidatorAssignmentResponse(Boolean success){
        this.success = success;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}