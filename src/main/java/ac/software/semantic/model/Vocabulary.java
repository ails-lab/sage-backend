package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.VocabularyType;
import edu.ntua.isci.ac.lod.vocabularies.OWLVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Vocabularies")
public class Vocabulary implements ResourceContext {
   @Id
   private ObjectId id;

   private String name;
   
   private List<VocabularyEntityDescriptor> uriDescriptors;
   
   private String specification;
   
   private String sparqlEndpoint;
   
   private String labelQuery;
   private String descriptionQuery;

   private List<ObjectId> databaseId;
   
   private VocabularyType type;
	
   @Transient
   @JsonIgnore
   private Map<String, String> prefixMap;
   
   @Transient
   @JsonIgnore
   private List<String> classes;

   @Transient
   @JsonIgnore
   private List<String> properties;
   
   public Vocabulary() {
	   prefixMap = new HashMap<>();
   }

   	public ObjectId getId() {
   		return id;
   	}
	   
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSpecification() {
		return specification;
	}

	public void setSpecification(String specification) {
		this.specification = specification;
	}
	
	@Override
	public Map<String, String> getPrefixMap() {
		return prefixMap;
	}
	
	public void load() {
		if (uriDescriptors != null) {
			for (VocabularyEntityDescriptor ved : uriDescriptors) {
				prefixMap.put(ved.getNamespace(), ved.getPrefix());
			}
		}
		
		if (specification != null) {
			Model model = ModelFactory.createDefaultModel();
			
			try {
				RDFDataMgr.read(model, specification);
			} catch (Exception ex) {
				System.out.println("Error loading " + specification);
				ex.printStackTrace();
			}
	
			String sparql; 
	
	//		if (type == VocabularyType.RDFS) {
	//			sparql = "SELECT ?class { ?class a <" + RDFSVocabulary.Class + "> } ORDER BY ?class ";
	//		} else {
	//			sparql = "SELECT ?class { ?class a <" + OWLVocabulary.Class + "> FILTER isIRI(?class) } ORDER BY ?class ";
	//		}
			
			sparql = "SELECT ?class { ?class a ?type . VALUES ?type { <" + RDFSVocabulary.Class + "> <" + OWLVocabulary.Class + "> } FILTER isIRI(?class) } ORDER BY ?class ";

			classes = new ArrayList<>();

			try (QueryExecution qe = QueryExecutionFactory.create(sparql, model)) {
				ResultSet rs = qe.execSelect();
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					classes.add(sol.get("class").toString());
				}
			}
			
			if (classes.size() == 0) {
				classes = null;
			}
			
	//		if (type == VocabularyType.RDFS) {
	//			sparql = "SELECT ?prop { ?prop a <" + RDFVocabulary.Property + "> } ORDER BY ?prop ";
	//		} else {
	//			sparql = "SELECT ?prop { ?prop a <" + OWLVocabulary.ObjectProperty + "> } ORDER BY ?prop ";
	//		}
			sparql = "SELECT ?prop { ?prop a ?type . VALUES ?type { <" + RDFVocabulary.Property + "> <" + OWLVocabulary.ObjectProperty + "> } FILTER isIRI(?prop) } ORDER BY ?prop ";
			
			properties = new ArrayList<>();
			
			try (QueryExecution qe = QueryExecutionFactory.create(sparql, model)) {
				ResultSet rs = qe.execSelect();
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
	//				System.out.println(sol.get("prop"));
					properties.add(sol.get("prop").toString());
				}
			}
			
			if (properties.size() == 0) {
				properties = null;
			}

		}
	}

	public List<ObjectId> getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(List<ObjectId> databaseId) {
		this.databaseId = databaseId;
	}
	
	public List<String> getClasses() {
		return classes;
	}

	public List<String> getProperties() {
		return properties;
	}

	public VocabularyType getType() {
		return type;
	}

	public void setType(VocabularyType type) {
		this.type = type;
	}

	@Override
	public List<VocabularyEntityDescriptor> getUriDescriptors() {
		return uriDescriptors;
	}

	public void setUriDescriptors(List<VocabularyEntityDescriptor> uriDescriptors) {
		this.uriDescriptors = uriDescriptors;
	}

	public String getSparqlEndpoint() {
		return sparqlEndpoint;
	}

	public void setSparqlEndpoint(String sparqlEndpoint) {
		this.sparqlEndpoint = sparqlEndpoint;
	}

	@Override
	public String getSparqlQueryForResource(String resource) {
		if (labelQuery == null) {
			return null;
		}
		
		String body = labelQuery + " . ";
		
		if (descriptionQuery != null) {
			body += descriptionQuery + " . ";
		}
		
		body = "CONSTRUCT { <{@@RESOURCE@@}> <http://www.w3.org/2000/01/rdf-schema#label> ?label .  <{@@RESOURCE@@}> <http://purl.org/dc/terms/description> ?description } WHERE { " + body + " }";
		return body.replaceAll("\\{@@RESOURCE@@\\}", resource);
	}

	public String getLabelQuery() {
		return labelQuery;
	}

	public void setLabelQuery(String labelQuery) {
		this.labelQuery = labelQuery;
	}

	public String getDescriptionQuery() {
		return descriptionQuery;
	}

	public void setDescriptionQuery(String descriptionQuery) {
		this.descriptionQuery = descriptionQuery;
	}

	@Override
	public boolean isSparql() {
		return sparqlEndpoint != null;
	}

}
