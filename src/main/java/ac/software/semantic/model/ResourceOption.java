package ac.software.semantic.model;

public class ResourceOption {
	
	private ResourceOptionType type;
	private Object value;
	
	public ResourceOption () {
	}

	public ResourceOptionType getType() {
		return type;
	}

	public void setType(ResourceOptionType type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
}
