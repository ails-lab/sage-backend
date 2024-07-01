package ac.software.semantic.payload.response;

public class ProgressResponse {
    private int totalAnnotations;
    private int totalValidations;
    private int totalAdded;
    private int totalAccepted;
    private int totalRejected;
    private int totalNeutral;
    private double progress;

    public ProgressResponse() {
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

	public double getProgress() {
		return progress;
	}

	public void setProgress(double progress) {
		this.progress = progress;
	}
}
