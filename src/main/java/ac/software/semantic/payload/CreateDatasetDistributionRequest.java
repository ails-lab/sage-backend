package ac.software.semantic.payload;

import java.util.List;

import ac.software.semantic.model.constants.SerializationType;

public class CreateDatasetDistributionRequest {

	private List<String> classes;
	private List<SerializationType> serializations;
	
	private String compress;
	
	CreateDatasetDistributionRequest() {
		
	}

	public List<String> getClasses() {
		return classes;
	}

	public void setClasses(List<String> classes) {
		this.classes = classes;
	}

	public List<SerializationType> getSerializations() {
		return serializations;
	}

	public void setSerializations(List<SerializationType> serializations) {
		this.serializations = serializations;
	}

	public String getCompress() {
		return compress;
	}

	public void setCompress(String compress) {
		this.compress = compress;
	}
	
	
}
