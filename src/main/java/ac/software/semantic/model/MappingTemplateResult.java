package ac.software.semantic.model;

import java.util.List;

public class MappingTemplateResult {
	private String template;
	private List<ParameterBinding> bindings;
	
	public MappingTemplateResult() { }
	
	public String getTemplate() {
		return template;
	}
	
	public void setTemplate(String template) {
		this.template = template;
	}
	
	public List<ParameterBinding> getBindings() {
		return bindings;
	}
	
	public void setBindings(List<ParameterBinding> bindings) {
		this.bindings = bindings;
	}
	
	public String toString() {
		return "MTR " + template + " : " + bindings; 
	}
}