package ac.software.semantic.model;

import java.nio.charset.Charset;

import org.apache.jena.riot.RDFFormat;
import org.springframework.http.MediaType;

public class RDFMediaType {
	
	public static final String TEXT_TURTLE_VALUE = "text/turtle";
	public static final MediaType TEXT_TURTLE = new MediaType("text","turtle", Charset.forName("UTF-8"));
	
	public static final String APPLICATION_TRIG_VALUE = "application/trig";
	public static final MediaType APPLICATION_TRIG = new MediaType("application","trig", Charset.forName("UTF-8"));

	public static final String APPLICATION_N_TRIPLES_VALUE = "application/n-triples";
	public static final MediaType APPLICATION_N_TRIPLES = new MediaType("application","n-triples", Charset.forName("UTF-8"));

	public static final String APPLICATION_RDF_XML_VALUE = "application/rdf+xml";
	public static final MediaType APPLICATION_RDF_XML = new MediaType("application","rdf+xml", Charset.forName("UTF-8"));

	public static final String APPLICATION_JSONLD_VALUE = "application/jsonld";
	public static final MediaType APPLICATION_JSONLD = new MediaType("application","jsonld", Charset.forName("UTF-8"));

	public static MediaType getMediaType(String contentType) {
		
		if (contentType.equals(APPLICATION_JSONLD_VALUE)) {
			return APPLICATION_JSONLD;
		} else if (contentType.equals(TEXT_TURTLE_VALUE)) {
			return TEXT_TURTLE;
		} else if (contentType.equals(APPLICATION_TRIG_VALUE)) {
			return APPLICATION_TRIG;
		} else if (contentType.equals(APPLICATION_N_TRIPLES_VALUE)) {
			return APPLICATION_N_TRIPLES;
		} else if (contentType.equals(APPLICATION_RDF_XML_VALUE)) {
			return APPLICATION_RDF_XML;
		}
		
		return null;
	}
	
	public static RDFFormat getFormat(String contentType) {
		
		if (contentType.equals(APPLICATION_JSONLD_VALUE)) {
			return RDFFormat.JSONLD;
		} else if (contentType.equals(TEXT_TURTLE_VALUE)) {
			return RDFFormat.TTL;
		} else if (contentType.equals(APPLICATION_TRIG_VALUE)) {
			return RDFFormat.TRIG;
		} else if (contentType.equals(APPLICATION_N_TRIPLES_VALUE)) {
			return RDFFormat.NTRIPLES;
		} else if (contentType.equals(APPLICATION_RDF_XML_VALUE)) {
			return RDFFormat.RDFXML;
		}
		
		return null;
	}
}
