package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.VocabularyType;
import edu.ntua.isci.ac.lod.vocabularies.OWLVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Vocabularies")
public class Vocabulary {
   @Id
   private ObjectId id;

   private String name;
   
   private String namespace;
   
   private String prefix;
   
   private String specification;

   private List<ObjectId> databaseId;
   
   private VocabularyType type;
	
   @Transient
   private List<String> classes;

   @Transient
   private List<String> properties;
   
   public Vocabulary() {
	   classes = new ArrayList<>();
	   properties = new ArrayList<>();
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

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getSpecification() {
		return specification;
	}

	public void setSpecification(String specification) {
		this.specification = specification;
	}
	
	public void load() {
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
		
		try (QueryExecution qe = QueryExecutionFactory.create(sparql, model)) {
			ResultSet rs = qe.execSelect();
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				classes.add(sol.get("class").toString());
			}
		}
		
//		if (type == VocabularyType.RDFS) {
//			sparql = "SELECT ?prop { ?prop a <" + RDFVocabulary.Property + "> } ORDER BY ?prop ";
//		} else {
//			sparql = "SELECT ?prop { ?prop a <" + OWLVocabulary.ObjectProperty + "> } ORDER BY ?prop ";
//		}
		sparql = "SELECT ?prop { ?prop a ?type . VALUES ?type { <" + RDFVocabulary.Property + "> <" + OWLVocabulary.ObjectProperty + "> } FILTER isIRI(?prop) } ORDER BY ?prop ";
		
		try (QueryExecution qe = QueryExecutionFactory.create(sparql, model)) {
			ResultSet rs = qe.execSelect();
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
//				System.out.println(sol.get("prop"));
				properties.add(sol.get("prop").toString());
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

}
