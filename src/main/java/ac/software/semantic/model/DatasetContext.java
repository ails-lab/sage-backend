package ac.software.semantic.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.SchemaService;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasetContext implements ResourceContext {
	
	private String name;
	
	private ObjectId id;
	
	private List<VocabularyEntityDescriptor> uriDescriptors;
	
	@Transient
	@JsonIgnore
	private DatasetContainer datasetContainer;
	
	@Transient
	@JsonIgnore
	private SchemaService schemaService;
	
	public DatasetContext(SchemaService schemaService) {
		this.schemaService = schemaService;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<VocabularyEntityDescriptor> getUriDescriptors() {
		return uriDescriptors;
	}

	public void setUriDescriptors(List<VocabularyEntityDescriptor> uriDescriptors) {
		this.uriDescriptors = uriDescriptors;
	}

	public DatasetContainer getDatasetContainer() {
		return datasetContainer;
	}

	public void setDatasetContainer(DatasetContainer datasetContainer) {
		this.datasetContainer = datasetContainer;
		this.id = datasetContainer.getObject().getId();
	}

	@JsonIgnore
	@Override
	public String getSparqlEndpoint() {
		return datasetContainer.getDatasetTripleStoreVirtuosoConfiguration().getSparqlEndpoint();
	}

	@Override
	public String getSparqlQueryForResource(String resource) {
		for (VocabularyEntityDescriptor s : uriDescriptors) {
			if (resource.startsWith(s.getNamespace())) {
		
				DatasetCatalog dcg = schemaService.asCatalog(datasetContainer.getObject());
				
				return  
						"CONSTRUCT { " + 
		 			    "  ?resource <" + RDFSVocabulary.label + "> ?label . " +
						"  ?resource <" + DCTVocabulary.description + "> ?description } " +
						schemaService.buildFromClause(dcg) +
						"WHERE { " +
						"  VALUES ?resource { <" + resource + "> } " +
						"  ?resource <" + RDFSVocabulary.label + ">|<" + SKOSVocabulary.prefLabel + ">|<" + DCTVocabulary.title + "> ?label . " +
				        "  OPTIONAL { ?resource <" + SKOSVocabulary.scopeNote + ">|<" + DCTVocabulary.description + "> ?description } }"; 
			}
		}
		
		return null;
	}

	@JsonIgnore
	@Override
	public boolean isSparql() {
		return true;
	}

	public ObjectId getId() {
		return id;
	}

	@Override
	public Map<String, String> getPrefixMap() {
		return new HashMap<>();
	}

}