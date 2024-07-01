package ac.software.semantic.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExtendedParameter extends ParameterBinding {

	private String description;
	private String datatype;
	private List<String> format;
	
	private Boolean required;
	private Boolean hidden;
	
	private Boolean inherited;
	
	public ExtendedParameter() { 
	}


	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Boolean getInherited() {
		return inherited;
	}

	public void setInherited(Boolean inherited) {
		this.inherited = inherited;
	}


	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public Boolean getRequired() {
		if (hidden == null) {
			return true;
		}
		return required;
	}


	public void setRequired(Boolean required) {
		this.required = required;
	}


	public Boolean getHidden() {
		if (hidden == null) {
			return false;
		}
		return hidden;
	}


	public void setHidden(Boolean hidden) {
		this.hidden = hidden;
	}


	public List<String> getFormat() {
		return format;
	}


	public void setFormat(List<String> format) {
		this.format = format;
	}



}
