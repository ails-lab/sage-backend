package ac.software.semantic.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConditionInstruction {
	
	private List<PreprocessInstruction> conditions;
	
	private String scope;

	public List<PreprocessInstruction> getConditions() {
		return conditions;
	}

	public void setConditions(List<PreprocessInstruction> conditions) {
		this.conditions = conditions;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}
	   

}
