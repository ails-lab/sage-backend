package ac.software.semantic.model.constants.type;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import edu.ntua.isci.ac.lod.vocabularies.FormatsVocabulary;

public enum SerializationType {
	
	CSV,
	TTL,
	NT,
	RDF_XML,
	JSONLD,
	;

	public static SerializationType get(String type) {
		if (type.equalsIgnoreCase("CSV")) {
			return CSV;
		} else if (type.equalsIgnoreCase("TTL")) {
			return TTL;
		} else if (type.equalsIgnoreCase("NT")) {
			return NT;
		} else if (type.equalsIgnoreCase("RDF/XML")) {
			return RDF_XML;
		} else if (type.equalsIgnoreCase("JSON-LD")) {
			return JSONLD;
		} 
		
		return null;
	}
	
	public static Resource toFormats(SerializationType st, ExportVocabulary vocabulary) {
		if (vocabulary == ExportVocabulary.eu_contolled_vocabularies) {
			if (st == TTL) {
				return new ResourceImpl("http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE");
			} else if (st == NT) {
				return new ResourceImpl("http://publications.europa.eu/resource/authority/file-type/RDF_N_TRIPLES");
			} else if (st == RDF_XML) {				
				return new ResourceImpl("http://publications.europa.eu/resource/authority/file-type/RDF_XML");
			}			
		} else if (vocabulary == ExportVocabulary.w3c) { 
			if (st == TTL) {
				return FormatsVocabulary.Turtle;
			} else if (st == NT) {
				return FormatsVocabulary.N_Triples;
			} else if (st == RDF_XML) {				
				return FormatsVocabulary.RDF_XML;
			}
		}
		
		return null;
	}
	
	public static String toMediaType(SerializationType st) {
		if (st == TTL) {
			return "text/turtle";
		} else if (st == NT) {
			return "application/n-triples";
		} else if (st == RDF_XML) {
			return "application/rdf+xml";
		}
		
		return null;
	}
	
	
}
