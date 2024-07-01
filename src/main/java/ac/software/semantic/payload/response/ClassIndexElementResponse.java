package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.index.PropertyIndexElement;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClassIndexElementResponse {
	private String clazz;
	private List<PropertyIndexElementResponse> properties;
	
	private List<EmbedderIndexElementResponse> embedders;
	
	public ClassIndexElementResponse() {
		
	}

	public ClassIndexElementResponse(String clazz) {
		this.clazz = clazz;
		properties = new ArrayList<>();
	}
	
	public ClassIndexElementResponse(Resource clazz) {
		this.clazz = clazz.toString();
		properties = new ArrayList<>();
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	public void setProperties(List<PropertyIndexElementResponse> properties) {
		this.properties = properties;
	}

	public List<PropertyIndexElementResponse> getProperties() {
		return properties;
	}

	public void addProperty(Property property) {
		properties.add(new PropertyIndexElementResponse(property));
	}

	public void addProperty(Property property, ClassIndexElementResponse ie) {
		properties.add(new PropertyIndexElementResponse(property, ie));
	}

	public List<EmbedderIndexElementResponse> getEmbedders() {
		return embedders;
	}

	public void setEmbedders(List<EmbedderIndexElementResponse> embedders) {
		this.embedders = embedders;
	}

}