package ac.software.semantic.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.d2rml.model.map.ValueMap.TermMapType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataServiceParameterValue {

	private String name;
	private String value;
	
	private TermMapType type;
	
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

	public TermMapType getType() {
		return type;
	}

	public void setType(TermMapType type) {
		this.type = type;
	}

}
