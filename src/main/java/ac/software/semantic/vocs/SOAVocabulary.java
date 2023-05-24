package ac.software.semantic.vocs;

import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.springframework.stereotype.Service;

public class SOAVocabulary extends Vocabulary {
	public static String SOA_NAMESPACE = "http://sw.islab.ntua.gr/annotation/";
	
	public static Property confidence = model.createProperty(SOA_NAMESPACE + "confidence");
	public static Property destination = model.createProperty(SOA_NAMESPACE + "destination");
	public static Property rdfPath = model.createProperty(SOA_NAMESPACE + "rdfPath");
	public static Property recommendation = model.createProperty(SOA_NAMESPACE + "recommendation");
	public static Property property = model.createProperty(SOA_NAMESPACE + "property");
	
    public static Property onProperty = model.createProperty(SOA_NAMESPACE + "onProperty");
    public static Property onValue = model.createProperty(SOA_NAMESPACE + "onValue");
    public static Property start = model.createProperty(SOA_NAMESPACE + "start");
    public static Property end = model.createProperty(SOA_NAMESPACE + "end");
    public static Property score = model.createProperty(SOA_NAMESPACE + "score");    
    
    public static Property hasReview = model.createProperty(SOA_NAMESPACE + "hasReview");
    public static Property hasValidation = model.createProperty(SOA_NAMESPACE + "hasValidation");
    public static Property action = model.createProperty(SOA_NAMESPACE + "action");
    
    public static Resource Delete  = model.createProperty(SOA_NAMESPACE + "Delete");
    public static Resource Add = model.createProperty(SOA_NAMESPACE + "Add");
    public static Resource Approve = model.createProperty(SOA_NAMESPACE + "Approve");
    
    public static Resource Accept = model.createProperty(SOA_NAMESPACE + "Accept");
    public static Resource Reject = model.createProperty(SOA_NAMESPACE + "Reject");
    
//    public static Resource accepted = model.createProperty(SOA_NAMESPACE + "accepted");
//    public static Resource rejected = model.createProperty(SOA_NAMESPACE + "rejected");
//    public static Resource unvalidated = model.createProperty(SOA_NAMESPACE + "unvalidated");
    
//    public static Resource ValidationState = model.createProperty(SOA_NAMESPACE + "ValidationState");
    public static Resource Validation = model.createProperty(SOA_NAMESPACE + "Validation");
    public static Resource RDFPathSelector = model.createProperty(SOA_NAMESPACE + "RDFPathSelector");
    public static Resource RDFPropertySelector = model.createProperty(SOA_NAMESPACE + "RDFPropertySelector");
    public static Resource Literal = model.createProperty(SOA_NAMESPACE + "Literal");
    
    public static Property literal = model.createProperty(SOA_NAMESPACE + "literal");
    public static Property iri = model.createProperty(SOA_NAMESPACE + "iri");
    
    public static String getPrefix() {
    	return SOA_NAMESPACE;
    }
    
}

