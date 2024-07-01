package ac.software.semantic.vocs;

import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

import java.util.List;

import org.apache.jena.rdf.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ClustererDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.constants.type.SerializationType;

@Service
public class SEMRVocabulary extends Vocabulary {
	
	@Autowired
    @Qualifier("database")
	private Database database;

    @Value("${dataset.version:null}")
    private Integer version;
    
    @Value("${backend.server}")
    private String server;

    public Resource getItemBaseResource() {
    	return model.createResource(database.getResourcePrefix() + "item/");
    }
    
    public Resource getDefaultGroupResource() {
    	return model.createResource(database.getResourcePrefix() + "group/default");
    }
    
	public Resource getContentGraphResource(Dataset dataset) {
		return model.createResource(database.getResourcePrefix() + "graph/content");
	}

//	public Resource getAccessGraphResource() {
//		return model.createResource(database.getResourcePrefix() + "graph/access");
//	}

	public Resource getAnnotationGraphResource() {
		return model.createResource(database.getResourcePrefix() + "graph/annotation");
	}
	
//    public Resource getDatasetAsResource(String uuid) {
//    	return model.createResource(database.getResourcePrefix() + "dataset/" + uuid);
//    }
    
    public Resource getDatasetContentAsResource(Dataset dataset) {
    	return model.createResource(database.getResourcePrefix() + "dataset/" + dataset.getUuid());
    }
    
    public Resource getDatasetMetadataAsResource(Dataset dataset) {
		if (version == null) {
			return getContentGraphResource(dataset);
		} else {
			return model.createResource(database.getResourcePrefix() + "dataset-metadata/" + dataset.getUuid());
		}
    }
    
    public Resource getDatasetEmbeddingsAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "dataset-embeddings/" + uuid);
    }

    public Resource getDatasetAnnotationsAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "dataset-annotations/" + uuid);
    }
    
    public Resource getMappingAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "mapping/" + uuid);
    }

    public Resource getMappingItemAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "mapping/" + uuid + "/");
    }
    
    public Resource getAnnotatorAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "annotator/" + uuid);
    }

    public Resource getClustererAsResource(ClustererDocument cdoc) {
    	return model.createResource(database.getResourcePrefix() + "clusterer/" + cdoc.getUuid());
    }

    public Resource getAnnotatorAsResource(AnnotatorDocument adoc) {
    	return model.createResource(database.getResourcePrefix() + "annotator/" + adoc.getUuid());
    }

    public Resource getEmbedderAsResource(EmbedderDocument edoc) {
    	return model.createResource(database.getResourcePrefix() + "embedder/" + edoc.getUuid());
    }

    public Resource getAnnotationAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "annotation/" + uuid);
    }

    public Resource getClusterAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "cluster/" + uuid);
    }

    public boolean isAnnotator(String uri) {
    	return uri.startsWith(database.getResourcePrefix() + "annotator/");
    }

    public boolean isAnnotationValidator(String uri) {
    	return uri.startsWith(database.getResourcePrefix() + "annotationvalidator/");
    }

    public Resource getDistributionAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "distribution/" + uuid);
    }

    public Resource getDataServiceAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "data-service/" + uuid);
    }

//    public Resource getUserAsResource(String uuid) {
//    	return model.createResource(database.getResourcePrefix() + "user/" + uuid);
//    }
//
//    public Resource getGroupAsResource(String uuid) {
//    	return model.createResource(database.getResourcePrefix() + "group/" + uuid);
//    }
    
    public Resource getAnnotationSetAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "annotationset/" + uuid);
    }

    public Resource getAnnotationValidatorAsResource(AnnotationValidation av) {
    	return model.createResource(database.getResourcePrefix() + "annotationvalidator/" + av.getUuid());
    }

    public Resource getTermAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "term/" + uuid);
    }


    public String getUuidFromResourceUri(String uri) {
    	if (uri.startsWith(database.getResourcePrefix() + "dataset/")) {
    		return uri.substring((database.getResourcePrefix() + "dataset/").length());
    	} else if (uri.startsWith(database.getResourcePrefix() + "annotation/")) {
    		return uri.substring((database.getResourcePrefix() + "annotation/").length());
    	} else if (uri.startsWith(database.getResourcePrefix() + "annotator/")) {
    		return uri.substring((database.getResourcePrefix() + "annotator/").length());
    	} else if (uri.startsWith(database.getResourcePrefix() + "user/")) {
    		return uri.substring((database.getResourcePrefix() + "user/").length());
    	} else if (uri.startsWith(database.getResourcePrefix() + "group/")) {
    		return uri.substring((database.getResourcePrefix() + "group/").length());
    	} 
    	return null;
    }
    
    public Resource getContentSparqlEnpoint(String identifier) {
    	return model.createResource(server + "/api/content/" + identifier + "/sparql");
    }

    public Resource getContentSparqlEnpoint(String identifier, List<Integer> groups) {
    	String gr = "";
    	if (groups != null) {
	    	for (int i : groups) {
	    		if (gr.length() > 0) {
	    			gr += "_";
	    		}
	    		
	    		gr += i;
	    	}
	    	
	    	if (gr.length() > 0) {
	    		gr = "_" + gr;
	    	}
    	}
    	
    	return model.createResource(server + "/api/content/" + identifier + gr + "/sparql");
    }
    
    public Resource getContentDistribution(String identifier, String distributionIdentifier, SerializationType serialization) {
    	return model.createResource(server + "/api/content/" + identifier + "/distribution/" + distributionIdentifier + "/" + serialization.toString().toLowerCase());
    }
    
    public Resource getContentAnnotations(String identifier) {
    	return model.createResource(server + "/api/content/" + identifier + "/annotations");
    }

}

