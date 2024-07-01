package ac.software.semantic.model;

import java.util.List;

public class CatalogTemplateResult {

//	{ "dataset": { "template": "mint", "bindings" : [ { "name": "TITLE", "value": "museu 5820" } ], "mappings": [ { "mapping": { "name": "mint dataset", "bindings" : [ { "name": "DATASET_ID", "value": "5820" }  ] } ] }{ "dataset": { "template": "mint", "bindings" : [ { "name": "TITLE", "value": "museu 5820" } ], "mappings": [ { "mapping": { "name": "mint dataset", "bindings" : [ { "name": "DATASET_ID", "value": "5820" }  ] } ] }
	
	private String template;
	private String name;
	private List<ParameterBinding> bindings;
	private List<MappingTemplateResult> mappings;
	

	
	public CatalogTemplateResult() { }

	public List<MappingTemplateResult> getMappings() {
		return mappings;
	}

	public void setMappings(List<MappingTemplateResult> mappings) {
		this.mappings = mappings;
	}

	public List<ParameterBinding> getBindings() {
		return bindings;
	}

	public void setBindings(List<ParameterBinding> bindings) {
		this.bindings = bindings;
	}

	public String getTemplate() {
		return template;
	}

	public void setTemplate(String template) {
		this.template = template;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
