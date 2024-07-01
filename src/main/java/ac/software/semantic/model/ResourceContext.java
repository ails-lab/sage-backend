package ac.software.semantic.model;

import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface ResourceContext {

	public ObjectId getId();
	
	public String getName();
	
	public List<VocabularyEntityDescriptor> getUriDescriptors();
	
	public String getSparqlEndpoint();
	
	public String getSparqlQueryForResource(String resource);
	
	public boolean isSparql();
	
	@JsonIgnore
	public Map<String, String> getPrefixMap();
}
