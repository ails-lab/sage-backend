package ac.software.semantic.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PreprocessInstruction {

	private String function;
	private List<DataServiceParameterValue> parameters;
	
	private String modifier;
	
	public PreprocessInstruction() {
		
	}

	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}

	public List<DataServiceParameterValue> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameterValue> parameters) {
		this.parameters = parameters;
	}

	public String getModifier() {
		return modifier;
	}

	public void setModifier(String modifier) {
		this.modifier = modifier;
	}
	
}
