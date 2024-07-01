package ac.software.semantic.model.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.EmbedderDocument;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClassIndexElement {
	private String clazz;
	private List<PropertyIndexElement> properties;
	
	private List<EmbedderIndexElement> embedders;
	
	private String group;
	
	private Boolean groupBy;
	
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
	
	public boolean updateRequiredParameters(Map<Integer, DataServiceParameter> indexMap) {
		boolean res = false;
		for (PropertyIndexElement p : properties) {
			res = res | p.updateRequiredParameters(indexMap);
		}
		
		return res;
	}

	
	public String topElementsListSPARQL(String fromClause) {
		return "SELECT ?s " + fromClause + " WHERE { ?s a <" + clazz + "> } ";
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

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public void indexToPropertiesMap(Map<Integer, List<String>> map) {

		if (properties != null) {
			for (PropertyIndexElement prop : properties) {
				prop.indexToPropertiesMap(map);
			}
		}
	}

	public Boolean getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(Boolean groupBy) {
		this.groupBy = groupBy;
	}
}