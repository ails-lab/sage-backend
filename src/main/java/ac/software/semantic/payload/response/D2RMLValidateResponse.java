package ac.software.semantic.payload.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class D2RMLValidateResponse {
    
	private List<DataServiceParameter> parameters;
	private List<String> dependencies;

    public D2RMLValidateResponse() {
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


}