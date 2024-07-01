package ac.software.semantic.model;

public class ValidationResult {

	private boolean conforms;
	
	private String report;

	public ValidationResult(boolean conforms,  String report) {
		this.conforms = conforms;
		this.report = report;
	}
	
	public boolean isConforms() {
		return conforms;
	}

	public void setConforms(boolean conforms) {
		this.conforms = conforms;
	}

	public String getReport() {
		return report;
	}

	public void setReport(String report) {
		this.report = report;
	}
}
