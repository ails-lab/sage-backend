package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

public class DataServiceParameter {

	private String name;
	private String type;
	
	private List<String> values;
	
	private String defaultValue;
	
	public DataServiceParameter(String name, String type) {
		this.name = name;
		this.type = type;
		
		values = new ArrayList<>(); 
	}
	
	public void addValues(String v) {
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
}
