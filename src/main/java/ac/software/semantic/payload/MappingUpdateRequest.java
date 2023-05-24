package ac.software.semantic.payload;

import java.util.List;

public class MappingUpdateRequest {

	private String name; 
//	private String d2rml;
	
	private String templateId;
	
	private List<String> parameters;

	public MappingUpdateRequest() { }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

//	public String getD2rml() {
//		return d2rml;
//	}
//
//	public void setD2rml(String d2rml) {
//		this.d2rml = d2rml;
//	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(List<String> parameters) {
		this.parameters = parameters;
	}

	public String getTemplateId() {
		return templateId;
	}

	public void setTemplateId(String templateId) {
		this.templateId = templateId;
	}
	
}