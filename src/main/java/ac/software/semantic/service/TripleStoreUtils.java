package ac.software.semantic.service;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;

import ac.software.semantic.model.TripleStoreConfiguration;

public class TripleStoreUtils {

	public static int count(TripleStoreConfiguration vc, String sparql) {
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
   			ResultSet rs = qe.execSelect();
   			if (rs.hasNext()) {
   				QuerySolution qs = rs.next();
   				return qs.get("count").asLiteral().getInt();
   			}
    	}
		
		return 0;
	}
}
