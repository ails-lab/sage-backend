package ac.software.semantic.payload.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TemplatesResponse {

    private List<TemplateResponse> datasetTemplates;
    
    private List<TemplateResponse> catalogTemplates;
    
    private List<TemplateResponse> mappingSampleTemplates;
    
    public TemplatesResponse() {
    }
    
    public TemplatesResponse(List<TemplateResponse> catalog, List<TemplateResponse> dataset, List<TemplateResponse> mappingSampleTemplates) {
    	this.catalogTemplates = catalog;
    	this.datasetTemplates = dataset;
    	this.mappingSampleTemplates = mappingSampleTemplates; 
    }
    
	public List<TemplateResponse> getDatasetTemplates() {
		return datasetTemplates;
	}

	public void setDatasetTemplates(List<TemplateResponse> dataset) {
		this.datasetTemplates = dataset;
	}

	public List<TemplateResponse> getCatalogTemplates() {
		return catalogTemplates;
	}

	public void setCatalogTemplates(List<TemplateResponse> catalog) {
		this.catalogTemplates = catalog;
	}

	public List<TemplateResponse> getMappingSampleTemplates() {
		return mappingSampleTemplates;
	}

	public void setMappingSampleTemplates(List<TemplateResponse> mappingSampleTemplates) {
		this.mappingSampleTemplates = mappingSampleTemplates;
	}


}
