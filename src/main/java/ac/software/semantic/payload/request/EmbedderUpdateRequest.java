package ac.software.semantic.payload.request;


import java.util.List;

import ac.software.semantic.model.index.ClassIndexElement;

public class EmbedderUpdateRequest implements UpdateRequest {

	private String datasetUri; 
	private ClassIndexElement indexElement;
	private String embedder;
	private String variant;
	
	private String onClass;
	
	private List<String> keys;
	
	public String getDatasetUri() {
		return datasetUri;
	}
	
	public void setDatasetUri(String datasetUri) {
		this.datasetUri = datasetUri;
	}

	public ClassIndexElement getIndexElement() {
		return indexElement;
	}

	public void setIndexElement(ClassIndexElement indexElement) {
		this.indexElement = indexElement;
	}

	public String getEmbedder() {
		return embedder;
	}

	public void setEmbedder(String embedder) {
		this.embedder = embedder;
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

	public String getVariant() {
		return variant;
	}

	public void setVariant(String variant) {
		this.variant = variant;
	}

	public List<String> getKeys() {
		return keys;
	}

	public void setKeys(List<String> keys) {
		this.keys = keys;
	}
	
}
