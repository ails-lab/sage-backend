package ac.software.semantic.model;

import ac.software.semantic.model.constants.type.ResourceOptionType;

public class ResourceOption<T> {
	
	private ResourceOptionType type;
	private T value;
	
	public ResourceOption () {
	}

	public ResourceOptionType getType() {
		return type;
	}

	public void setType(ResourceOptionType type) {
		this.type = type;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}
}
