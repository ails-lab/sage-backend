package ac.software.semantic.model.expr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LogicResult {
	
	private Logic logic;
	private Double value;
	private int rule;
	
	@JsonIgnore
	private Boolean stop;
	
	public LogicResult(Logic logic, Double value, Boolean stop) {
		this.logic = logic;
		this.value = value;
		this.stop = stop;
	}

	public Logic getLogic() {
		return logic;
	}

	public void setLogic(Logic logic) {
		this.logic = logic;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public Boolean getStop() {
		return stop;
	}

	public void setStop(Boolean stop) {
		this.stop = stop;
	}

	public int getRule() {
		return rule;
	}

	public void setRule(int rule) {
		this.rule = rule;
	}

}
