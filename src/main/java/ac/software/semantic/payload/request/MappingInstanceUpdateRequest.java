package ac.software.semantic.payload.request;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ParameterBinding;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MappingInstanceUpdateRequest implements UpdateRequest {

	private List<ParameterBinding> bindings;
	
	private String identifier;
	
	private boolean active;
	
	public MappingInstanceUpdateRequest() { }

	public List<ParameterBinding> getBindings() {
		return bindings;
	}

	public void setBindings(List<ParameterBinding> bindings) {
		this.bindings = bindings;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	
}