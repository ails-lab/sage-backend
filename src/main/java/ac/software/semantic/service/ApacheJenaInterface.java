package ac.software.semantic.service;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;

public class ApacheJenaInterface implements SparqlEndpointInterface {

	private QueryExecution qe; 
	private ResultSet rs;
	
	@Override
	public void executeSelect(String location, String sparql) {
		System.out.println(QueryFactory.create(sparql));
		
		QueryExecution qe = QueryExecutionFactory.sparqlService(location, QueryFactory.create(sparql));
		
		rs = qe.execSelect();
	}
	
	@Override
	public boolean hasNext() {
		return rs.hasNext();
	}
	
	@Override
	public void next() {
//		BindingSet bindingSet = result.next();
//        Value valueOfX = bindingSet.getValue("x");
//        Value valueOfY = bindingSet.getValue("y");
		
	}
	
	@Override
	public void close() throws Exception {
		qe.close();
	}
}
