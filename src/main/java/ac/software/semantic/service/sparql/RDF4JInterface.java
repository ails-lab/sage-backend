package ac.software.semantic.service.sparql;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

public class RDF4JInterface implements SparqlEndpointInterface {

	private RepositoryConnection conn;
	private TupleQueryResult result; 
	
	@Override
	public void executeSelect(String location, String sparql) {
		Repository repo = new SPARQLRepository(location);
	
		conn = repo.getConnection();
		
		result = conn.prepareTupleQuery(sparql).evaluate();
	}
	
	@Override
	public boolean hasNext() {
		return result.hasNext();
	}
	
	@Override
	public void next() {
		BindingSet bindingSet = result.next();
        Value valueOfX = bindingSet.getValue("x");
        Value valueOfY = bindingSet.getValue("y");
		
	}
	
	@Override
	public void close() {
		if (result != null) {
			result.close();
		}
		
		if (conn != null) {
			conn.close();
		}
	}
	
	
}
