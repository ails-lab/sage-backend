package ac.software.semantic.payload;

public class DatasetProgressResponse {
    private String validationId;
    private String propertyName;
    private String asProperty;
    private double progress;
    private int totalAdded;
    private int totalAnnotations;
    private int totalValidations;
    private int totalAccepted;
    private int totalRejected;
    private int totalNeutral;

    public DatasetProgressResponse() {
    }

    public int getTotalAccepted() {
        return totalAccepted;
    }

    public void setTotalAccepted(int totalAccepted) {
        this.totalAccepted = totalAccepted;
    }

    public int getTotalRejected() {
        return totalRejected;
    }

    public void setTotalRejected(int totalRejected) {
        this.totalRejected = totalRejected;
    }

    public int getTotalNeutral() {
        return totalNeutral;
    }

    public void setTotalNeutral(int totalNeutral) {
        this.totalNeutral = totalNeutral;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getValidationId() {
        return validationId;
    }

    public double getProgress() {
        return progress;
    }

    public String getAsProperty() {
        return asProperty;
    }

    public void setAsProperty(String asProperty) {
        this.asProperty = asProperty;
    }

    public void setProgress(double progress) {
        this.progress = progress;
    }

    public void setValidationId(String validationId) {
        this.validationId = validationId;
    }

    public int getTotalAdded() {
        return totalAdded;
    }

    public void setTotalAdded(int totalAdded) {
        this.totalAdded = totalAdded;
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
}
