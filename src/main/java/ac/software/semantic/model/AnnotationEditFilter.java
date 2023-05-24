package ac.software.semantic.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.FilterValidationType;

public class AnnotationEditFilter {

	private FilterValidationType action;
	private String selectExpression;
	
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String newValue;
	
	public AnnotationEditFilter() {
		
	}
	
	public String getSelectExpression() {
		return selectExpression;
	}

	public void setSelectExpression(String expression) {
		this.selectExpression = expression;
	}

	public FilterValidationType getAction() {
		return action;
	}

	public void setAction(FilterValidationType action) {
		this.action = action;
	}

	public String getNewValue() {
		return newValue;
	}

	public void setNewValue(String newValue) {
		this.newValue = newValue;
	}

	
}
