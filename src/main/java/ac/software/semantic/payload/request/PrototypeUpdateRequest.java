package ac.software.semantic.payload.request;

import java.util.List;

import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.type.PrototypeType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PrototypeUpdateRequest implements MultipartFileUpdateRequest {

	private String name;
	private String url;
	
	private String description;
	
	private PrototypeType type;
	
	@JsonIgnore
	private MultipartFile file;
	
	private List<DataServiceParameter> parameters;
	private List<String> dependencies;
	
	private List<DataServiceParameter> fields;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public MultipartFile getFile() {
		return file;
	}

	public void setFile(MultipartFile file) {
		this.file = file;
	}

	public PrototypeType getType() {
		return type;
	}

	public void setType(PrototypeType type) {
		this.type = type;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public List<DataServiceParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameter> parameters) {
		this.parameters = parameters;
	}

	public List<String> getDependencies() {
		return dependencies;
	}

	public void setDependencies(List<String> dependencies) {
		this.dependencies = dependencies;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<DataServiceParameter> getFields() {
		return fields;
	}

	public void setFields(List<DataServiceParameter> fields) {
		this.fields = fields;
	}

}
