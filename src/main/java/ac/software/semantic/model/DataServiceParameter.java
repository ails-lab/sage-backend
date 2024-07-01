package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataServiceParameter {

	private String name;
	private String type; // type for ui
	private String datatype;
	private List<String> format;
	private boolean required;
	private boolean hidden;
	private List<DescribedValue> examples;
	
	private String description;
	
	private List<String> values;
	
	private String defaultValue;
	
	public DataServiceParameter() {
		
	}

	public DataServiceParameter(String name) {
		this(name, null);
	}
	
	public DataServiceParameter(String name, String datatype) {
		this.name = name;
		this.datatype = datatype;
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

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
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

	public boolean isHidden() {
		return hidden;
	}

	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}
	
	public static List<DataServiceParameterValue> getShowedParameters(boolean owner, List<DataServiceParameter> paramDefs, List<DataServiceParameterValue> parameters) {
		if (parameters == null) {
			return parameters;
		} else {
			List<DataServiceParameterValue> res = new ArrayList<>();
			
			loop:
			for (DataServiceParameterValue p : parameters) {
				if (paramDefs == null || owner) {
					res.add(p);
				} else {
					for (DataServiceParameter pd : paramDefs) {
						if (pd.getName().equals(p.getName())) {
							if (!pd.isHidden()) {
								res.add(p);
							} else {
								DataServiceParameterValue np = new DataServiceParameterValue();
								np.setName(p.getName());
								np.setValue("**********");
								
								res.add(np);
							}
							continue loop;
						}
					}
					res.add(p);
				}
			}
			
			return res;
		}
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<String> getFormat() {
		return format;
	}

	public void setFormat(List<String> format) {
		this.format = format;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<DescribedValue> getExamples() {
		return examples;
	}

	public void setExamples(List<DescribedValue> examples) {
		this.examples = examples;
	}

}
