package ac.software.semantic.payload.response;

import java.util.List;

import org.apache.jena.rdf.model.Property;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.index.IndexElementSelector;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class EmbedderIndexElementResponse {
	private String embedderId; 
	
	private List<IndexElementSelector> selectors;
	
	public EmbedderIndexElementResponse() {		
	}

	public EmbedderIndexElementResponse(String embedderId) {
		this.embedderId = embedderId;
	}

	public List<IndexElementSelector> getSelectors() {
		return selectors;
	}

	public void setSelectors(List<IndexElementSelector> selectors) {
		this.selectors = selectors;
	}

	public String getEmbedderId() {
		return embedderId;
	}

	public void setEmbedderId(String embedderId) {
		this.embedderId = embedderId;
	}
}