package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class D2RMLValidateResponse {
    
	private List<String> parameters;

    public D2RMLValidateResponse() {
    }

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(List<String> parameters) {
		this.parameters = parameters;
	}


}