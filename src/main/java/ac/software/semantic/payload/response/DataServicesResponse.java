package ac.software.semantic.payload.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataServicesResponse {

	private List<DataServiceResponse> annotators;
	private List<DataServiceResponse> embedders;
	private List<DataServiceResponse> clusterers;

	public DataServicesResponse() { }

	public List<DataServiceResponse> getAnnotators() {
		return annotators;
	}

	public void setAnnotators(List<DataServiceResponse> annotators) {
		this.annotators = annotators;
	}

	public List<DataServiceResponse> getEmbedders() {
		return embedders;
	}

	public void setEmbedders(List<DataServiceResponse> embedders) {
		this.embedders = embedders;
	}

	public List<DataServiceResponse> getClusterers() {
		return clusterers;
	}

	public void setClusterers(List<DataServiceResponse> clusterers) {
		this.clusterers = clusterers;
	}
}
