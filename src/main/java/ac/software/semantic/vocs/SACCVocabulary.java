package ac.software.semantic.vocs;

import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class SACCVocabulary extends Vocabulary {
    
    public static String SACC_NAMESPACE = "http://sw.islab.ntua.gr/semaspace/access/";
    
	public static Resource User =  model.createResource(SACC_NAMESPACE + "User");
	public static Resource Group = model.createResource(SACC_NAMESPACE + "Group");
	public static Resource PublicGroup = model.createResource(SACC_NAMESPACE + "PublicGroup");
	
    public static Property member = model.createProperty(SACC_NAMESPACE + "member");
    public static Property dataset = model.createProperty(SACC_NAMESPACE + "dataset");

    
}
