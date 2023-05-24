package ac.software.semantic.model.index;

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

import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.vocs.SOAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClassIndexElement {
	private String clazz;
	private List<PropertyIndexElement> properties;
	
	private List<EmbedderIndexElement> embedders;
	
	public ClassIndexElement() {
		
	}

	public ClassIndexElement(String clazz) {
		this.clazz = clazz;
		properties = new ArrayList<>();
	}
	
	public ClassIndexElement(Resource clazz) {
		this.clazz = clazz.toString();
		properties = new ArrayList<>();
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	public void setProperties(List<PropertyIndexElement> properties) {
		this.properties = properties;
	}

	public List<PropertyIndexElement> getProperties() {
		return properties;
	}

	public void addProperty(Property property) {
		properties.add(new PropertyIndexElement(property));
	}

	public void addProperty(Property property, ClassIndexElement ie) {
		properties.add(new PropertyIndexElement(property, ie));
	}
	

	
	public String topElementsListSPARQL(String graph) {
		return "SELECT ?s FROM <" + graph + "> WHERE { ?s a <" + clazz + "> } ";
	}
	
	
	public static void main(String[] args) {
////		class/<http://www.w3.org/ns/regorg#RegisteredOrganization> ;
////			property/<https://lod.stirdata.eu/model/mainActivity> 
////			property/<https://lod.stirdata.eu/model/jurisdiction> 
////			property/<http://www.w3.org/ns/regorg#hasRegisteredSite> 
////				class/<http://www.w3.org/ns/org#Site>
////					property/<http://www.w3.org/ns/org#siteAddress> ;
////						class/<http://www.w3.org/ns/locn#Address> ;
////							property/<http://www.w3.org/ns/locn#postCode> ;
////							property/<http://www.w3.org/ns/locn#locatorDesignator> ;
//		
//		Model model = ModelFactory.createDefaultModel();
//		
//		IndexElement ie0 = new IndexElement(model.createResource("http://www.w3.org/ns/regorg#RegisteredOrganization"));
//		IndexElement ie1 = new IndexElement(model.createResource("http://www.w3.org/ns/org#Site"));
//		IndexElement ie2 = new IndexElement(model.createResource("http://www.w3.org/ns/locn#Address"));
//		
//		ie0.addProperty(model.createProperty("http://www.w3.org/ns/regorg#orgActivity"));
//		ie0.getProperties().get(0).setIndex(0);
//		ie0.addProperty(model.createProperty("https://lod.stirdata.eu/model/jurisdiction"));
//		ie0.getProperties().get(1).setIndex(1);
//		ie0.addProperty(model.createProperty("http://www.w3.org/ns/org#hasRegisteredSite"), ie1);
//		
//		ie1.addProperty(model.createProperty("http://www.w3.org/ns/org#siteAddress"), ie2);
//		
//		ie2.addProperty(model.createProperty("http://www.w3.org/ns/locn#postCode"));
//		ie2.getProperties().get(0).setIndex(3);
//		ie2.addProperty(model.createProperty("http://www.w3.org/ns/locn#locatorDesignator"));
//		ie2.getProperties().get(1).setIndex(4);
//		
//		List<String> lang = new ArrayList<>();
//		lang.add("el");
//		lang.add("en");
//		ie2.getProperties().get(1).setLanguage(lang);
//
//		System.out.println(ie0.topElementsListSPARQL("graph"));
//
//		SPARQLStructure ss = ie0.toSPARQL();
//		System.out.println(ss.whereClause);
//		System.out.println(ss.getKeys());
//		System.out.println(ss.construct("graph", model.createResource()));
////		String sparql = ss.construct(SEMAVocabulary.getDatasetAsResource("1c1686f9-b2c6-40cf-85ba-5b1272fd0a31").toString());
////		Query query = QueryFactory.create(sparql);
//				
////		System.out.println(query);
//		
////		System.out.println(ss.getPaths());
	
	}

	public List<EmbedderIndexElement> getEmbedders() {
		return embedders;
	}

	public void setEmbedders(List<EmbedderIndexElement> embedders) {
		this.embedders = embedders;
	}

}