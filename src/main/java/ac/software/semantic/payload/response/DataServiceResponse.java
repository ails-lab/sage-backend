package ac.software.semantic.payload.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.DataServiceVariant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataServiceResponse {

	private String identifier;
	private String title;
	private List<DataServiceParameter> parameters;
	private List<String> asProperties;
	private List<DataServiceVariant> variants;
	private String description;
	
	private List<String> tags;
	
	private DataServiceType type;
	
	public DataServiceResponse() {}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getIdentifier() {
		return identifier;
	}
	
	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}

	public List<String> getAsProperties() {
		return asProperties;
	}

	public void setAsProperties(List<String> asProperties) {
		this.asProperties = asProperties;
	}

	public List<DataServiceVariant> getVariants() {
		return variants;
	}

	public void setVariants(List<DataServiceVariant> variants) {
		this.variants = variants;
	}

	public DataServiceType getType() {
		return type;
	}

	public void setType(DataServiceType type) {
		this.type = type;
	}

	public List<DataServiceParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameter> parameters) {
		this.parameters = parameters;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}	
}
