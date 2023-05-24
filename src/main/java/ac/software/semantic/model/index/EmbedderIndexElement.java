package ac.software.semantic.model.index;

import java.util.List;

import org.apache.jena.rdf.model.Property;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class EmbedderIndexElement {
	private ObjectId embedderId; 
	
	private List<IndexElementSelector> selectors;
	
	public EmbedderIndexElement() {		
	}

	public EmbedderIndexElement(ObjectId embedderId) {
		this.embedderId = embedderId;
	}

	public List<IndexElementSelector> getSelectors() {
		return selectors;
	}

	public void setSelectors(List<IndexElementSelector> selectors) {
		this.selectors = selectors;
	}

	public ObjectId getEmbedderId() {
		return embedderId;
	}

	public void setEmbedderId(ObjectId embedderId) {
		this.embedderId = embedderId;
	}
}