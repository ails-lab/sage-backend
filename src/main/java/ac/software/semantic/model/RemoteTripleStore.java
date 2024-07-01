package ac.software.semantic.model;

import java.util.Date;
import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RemoteTripleStore {

	private String sparqlEndpoint;
	private List<String> namedGraph;
	
	private String lastUpdatedSparqlQuery;
	
	public RemoteTripleStore() {}
	
	public String getSparqlEndpoint() {
		return sparqlEndpoint;
	}
	
	public void setSparqlEndpoint(String endpoint) {
		this.sparqlEndpoint = endpoint;
	}
	
	public List<String> getNamedGraph() {
		return namedGraph;
	}
	
	public void setNamedGraph(List<String> namedGraph) {
		this.namedGraph = namedGraph;
	}
	
	public static String buildFromClause(RemoteTripleStore rts) {
		String from = "";
		if (rts.getNamedGraph() != null) {
			for (String ng : rts.getNamedGraph()) {
				from += "FROM <" + ng + "> ";
			}
		}
		
		return from;
	}

	public String getLastUpdatedSparqlQuery() {
		return lastUpdatedSparqlQuery;
	}

	public void setLastUpdatedSparqlQuery(String lastUpdatedSparqlQuery) {
		this.lastUpdatedSparqlQuery = lastUpdatedSparqlQuery;
	}

	public String buildLastUpdatedSparqlQuery() {
		if (lastUpdatedSparqlQuery != null) {
			String sparql = "SELECT * ";
			if (namedGraph != null) {
				for (String g : namedGraph) {
					sparql += " FROM <" + g + "> ";
				}
			}
			sparql += " WHERE { " + lastUpdatedSparqlQuery + " }";
			
			return sparql;
		} else {
			return null;
		}
	}
	
	public Date findLastUpdated() {
		if (lastUpdatedSparqlQuery != null) {
			String updateQuery = buildLastUpdatedSparqlQuery();
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(sparqlEndpoint, QueryFactory.create(updateQuery, Syntax.syntaxARQ))) {
				ResultSet rs = qe.execSelect();
				
				if (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					Literal literal = sol.get("date").asLiteral();
					
			        if (XSDDatatype.XSDdateTime.equals(literal.getDatatype())) {
			            XSDDateTime dateTime = (XSDDateTime) XSDDatatype.XSDdateTime.parse(literal.getLexicalForm());
			            
			            return dateTime.asCalendar().getTime();
			        } else if (XSDDatatype.XSDdate.equals(literal.getDatatype())) {
			            XSDDateTime dateTime = (XSDDateTime) XSDDatatype.XSDdate.parse(literal.getLexicalForm());
			            
			            return dateTime.asCalendar().getTime();
			        } else if (XSDDatatype.XSDdateTimeStamp.equals(literal.getDatatype())) {
			            XSDDateTime dateTime = (XSDDateTime) XSDDatatype.XSDdateTimeStamp.parse(literal.getLexicalForm());
			            
			            return dateTime.asCalendar().getTime();
			        }					
				}
			} 
		}
		
		return null;
	}
}