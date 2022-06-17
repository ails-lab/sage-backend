package ac.software.semantic.payload;

import java.util.List;

public class VocabularizerRequest {

	private String datasetUri;
	private List<String> onProperty;
	private String name;
	private String separator;
	
	public VocabularizerRequest() {}
	
	public String getDatasetUri() {
		return datasetUri;
	}
	public void setDatasetUri(String datasetUri) {
		this.datasetUri = datasetUri;
	}
	public List<String> getOnProperty() {
		return onProperty;
	}
	public void setOnProperty(List<String> onProperty) {
		this.onProperty = onProperty;
	}
	public String getSeparator() {
		return separator;
	}
	public void setSeparator(String separator) {
		this.separator = separator;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
