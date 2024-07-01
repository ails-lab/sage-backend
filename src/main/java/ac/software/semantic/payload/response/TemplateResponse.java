package ac.software.semantic.payload.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ExtendedParameter;
import ac.software.semantic.model.ParameterBinding;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TemplateResponse {

    private String id;
    
    private String name;
    
    private List<? extends ParameterBinding> parameters;
    
    private String description;
    
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

	public List<? extends ParameterBinding> getParameters() {
		return parameters;
	}

	public void setParameters(List<? extends ParameterBinding> parameters) {
		this.parameters = parameters;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

}
