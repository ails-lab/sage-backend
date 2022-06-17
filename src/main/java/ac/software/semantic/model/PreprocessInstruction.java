package ac.software.semantic.model;

import java.util.List;

public class PreprocessInstruction {

	private String function;
	private List<DataServiceParameterValue> parameters;
	
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
	
}
