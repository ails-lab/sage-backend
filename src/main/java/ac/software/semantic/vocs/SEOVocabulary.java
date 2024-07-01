package ac.software.semantic.vocs;

import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class SEOVocabulary extends Vocabulary {
	public static String SEO_NAMESPACE = "http://sw.islab.ntua.gr/semaspace/ontology/";
	
    public static Property term = model.createProperty(SEO_NAMESPACE + "term");
    public static Property place = model.createProperty(SEO_NAMESPACE + "place");
    public static Property time = model.createProperty(SEO_NAMESPACE + "time");
}

