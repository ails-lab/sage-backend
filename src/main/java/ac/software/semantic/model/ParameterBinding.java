package ac.software.semantic.model;

public class ParameterBinding {

	private String name;
	private String value;
	
	public ParameterBinding() { }
	
	public ParameterBinding(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
