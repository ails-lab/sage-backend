package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ExtendedParameter;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TemplateResponse {

    private String id;
    
    private String name;
    
    private List<ExtendedParameter> parameters;
    
    public TemplateResponse() {
    }

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<ExtendedParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<ExtendedParameter> parameters) {
		this.parameters = parameters;
	}

}
