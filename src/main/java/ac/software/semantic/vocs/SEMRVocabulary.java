package ac.software.semantic.vocs;

import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

import org.apache.jena.rdf.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.constants.SerializationType;

@Service
public class SEMRVocabulary extends Vocabulary {
//	private static String SEMR_NAMESPACE = "http://sw.islab.ntua.gr/semaspace/resource/";
	
	@Autowired
    @Qualifier("database")
	Database database;

    @Value("${backend.server}")
    private String server;
    
	// database.getResourcePrefix() + "item" reserved for data items

    public Resource getItemBaseResource() {
    	return model.createResource(database.getResourcePrefix() + "item/");
    }
    
    public Resource getDefaultGroupResource() {
    	return model.createResource(database.getResourcePrefix() + "group/default");
    }
    
	public Resource getContentGraphResource() {
		return model.createResource(database.getResourcePrefix() + "graph/content");
	}

	public Resource getAccessGraphResource() {
		return model.createResource(database.getResourcePrefix() + "graph/access");
	}

	public Resource getAnnotationGraphResource() {
		return model.createResource(database.getResourcePrefix() + "graph/annotation");
	}
	
    public Resource getDatasetAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "dataset/" + uuid);
    }

    public Resource getDatasetEmbeddingsAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "dataset-embeddings/" + uuid);
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

    public Resource getEmbedderAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "embedder/" + uuid);
    }

    public Resource getAnnotationAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "annotation/" + uuid);
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

    public Resource getUserAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "user/" + uuid);
    }

    public Resource getGroupAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "group/" + uuid);
    }
    
    public Resource getAnnotationSetAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "annotationset/" + uuid);
    }

    public Resource getAnnotationValidatorAsResource(String uuid) {
    	return model.createResource(database.getResourcePrefix() + "annotationvalidator/" + uuid);
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

    public Resource getContentDistribution(String identifier, SerializationType serialization) {
    	return model.createResource(server + "/api/content/" + identifier + "/distribution/" + serialization.toString().toLowerCase());
    }
    
    public Resource getContentAnnotations(String identifier) {
    	return model.createResource(server + "/api/content/" + identifier + "/annotations");
    }

}

