package ac.software.semantic.payload;

public class ProgressResponseValidationModal {

    private int totalAnnotations;
    private int totalValidations;
    private int totalAdded;

    public ProgressResponseValidationModal (ProgressResponse res) {
        this.totalAnnotations = res.getTotalAnnotations();
        this.totalAdded = res.getTotalAdded();
        this.totalValidations = res.getTotalValidations();
    }

    public int getTotalAnnotations() {
        return totalAnnotations;
    }

    public void setTotalAnnotations(int totalAnnotations) {
        this.totalAnnotations = totalAnnotations;
    }

    public int getTotalValidations() {
        return totalValidations;
    }

    public void setTotalValidations(int totalValidations) {
        this.totalValidations = totalValidations;
    }

    public int getTotalAdded() {
        return totalAdded;
    }

    public void setTotalAdded(int totalAdded) {
        this.totalAdded = totalAdded;
    }
}
