package ac.software.semantic.model.expr;

import java.util.List;

public class Filter {

	private String name;
	private List<Object> parameters;
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public List<Object> getParameters() {
		return parameters;
	}

	public void setParameters(List<Object> parameters) {
		this.parameters = parameters;
	}
}
