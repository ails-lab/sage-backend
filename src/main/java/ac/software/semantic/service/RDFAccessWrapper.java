package ac.software.semantic.service;

import java.io.File;

import org.apache.jena.rdf.model.RDFNode;

import edu.ntua.isci.ac.common.db.QueryResult;
import edu.ntua.isci.ac.common.db.rdf.RDFLibrary;

public interface RDFAccessWrapper extends AutoCloseable {

	public void load(File f) throws Exception;
	
	public static RDFAccessWrapper create(RDFLibrary rdfLibrary) {
		if (rdfLibrary == RDFLibrary.RDF4J) {
			return new RDF4JAccessWrapper();
		} else if (rdfLibrary == RDFLibrary.JENA) {
			return new JenaAccessWrapper();
		}
		
		return null;
	}
	
	public void execQuery(String query);
	
	public boolean hasNext();
	
	public void next();
	
	public RDFNode get(String name);
}
