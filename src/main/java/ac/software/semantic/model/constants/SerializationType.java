package ac.software.semantic.model.constants;

import org.apache.jena.rdf.model.Resource;

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
	
	public static Resource toFormats(SerializationType st) {
		if (st == TTL) {
			return FormatsVocabulary.Turtle;
		} else if (st == NT) {
			return FormatsVocabulary.N_Triples;
		}
		
		return null;
	}
	
}
