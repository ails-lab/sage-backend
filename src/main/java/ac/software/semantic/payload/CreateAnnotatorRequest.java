package ac.software.semantic.payload;

import java.util.List;


import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.PreprocessInstruction;

public class CreateAnnotatorRequest {

	private String datasetUri; 
	private List<String> onProperty;
	private String asProperty;
	private String annotator;
	private String thesaurus;
	
	private String variant;
	
	private List<DataServiceParameterValue> parameters;
	private List<PreprocessInstruction> preprocess;
	
	public String getDatasetUri() {
		return datasetUri;
	}
	public void setDatasetUri(String datasetUri) {
		this.datasetUri = datasetUri;
	}
	public List<String> getOnProperty() {
		return onProperty;
	}
	public void setOnProperty(List<String> onProperty) {
		this.onProperty = onProperty;
	}
	public String getAsProperty() {
		return asProperty;
	}
	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}
	public String getAnnotator() {
		return annotator;
	}
	public void setAnnotator(String annotator) {
		this.annotator = annotator;
	}
	public String getThesaurus() {
		return thesaurus;
	}
	public void setThesaurus(String thesaurus) {
		this.thesaurus = thesaurus;
	}
	public List<DataServiceParameterValue> getParameters() {
		return parameters;
	}
	public void setParameters(List<DataServiceParameterValue> parameters) {
		this.parameters = parameters;
	}
	public List<PreprocessInstruction> getPreprocess() {
		return preprocess;
	}
	public void setPreprocess(List<PreprocessInstruction> preprocess) {
		this.preprocess = preprocess;
	}
	public String getVariant() {
		return variant;
	}
	public void setVariant(String variant) {
		this.variant = variant;
	}

}
