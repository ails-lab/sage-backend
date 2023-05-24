package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataServiceParameter {

	private String name;
	private String type;
	private boolean required;
	
	private List<String> values;
	
	private String defaultValue;
	
	public DataServiceParameter(String name, String type) {
		this.name = name;
		this.type = type;
		this.required = false;
		
//		values = new ArrayList<>(); 
	}
	
	public void addValues(String v) {
		if (values == null) {
			this.values = new ArrayList<>();
		}
		values.add(v);
	}
	
	public List<String> getValues() {
		return values;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}
}
