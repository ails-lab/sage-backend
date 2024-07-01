//package ac.software.semantic.service;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.jena.query.QueryExecution;
//import org.apache.jena.query.QueryExecutionFactory;
//import org.apache.jena.query.QueryFactory;
//import org.apache.jena.query.QuerySolution;
//import org.apache.jena.query.ResultSet;
//import org.apache.jena.query.Syntax;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.stereotype.Service;
//
//import ac.software.semantic.config.ConfigurationContainer;
//import ac.software.semantic.model.AlignmentDescriptor;
//import ac.software.semantic.model.GraphDescriptor;
//import ac.software.semantic.model.TripleStoreConfiguration;
//import ac.software.semantic.vocs.SEMRVocabulary;
//import ac.software.semantic.vocs.SEMAVocabulary;
//
//@Service
//public class CollectionsService {
//
//    @Autowired
//    @Qualifier("triplestore-configurations")
//    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfiguration;
//    
//	@Autowired
//	private SEMRVocabulary resourceVocabulary;
//
//	public GraphDescriptor getCollectionGraphUrlByIdentifier(String virtuoso, String identifier) {
//
//		GraphDescriptor res = null;
//		String sparql = 
//				"SELECT ?url WHERE { " +
//		        " GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
//		            " ?url <http://purl.org/dc/elements/1.1/identifier> <" + identifier + "> } }";
//
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getByName(virtuoso).getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//			ResultSet rs = qe.execSelect();
//		
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//				res = new GraphDescriptor(sol.get("?url").toString(), identifier, false, false);
//			}
//		}
//		
//		return res;
//
//	}
//	
//	public List<AlignmentDescriptor> getAlignmentGraphUrlsByType(String virtuoso, String type) {
//		List<AlignmentDescriptor> res = new ArrayList<>();
//		String sparql = 
//				"SELECT ?a ?aid ?s ?sid ?t ?tid WHERE { " +
//		        " GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
//		            " ?a a <" + SEMAVocabulary.Alignment + "> . " +
//		            " ?a <" + SEMAVocabulary.source + "> ?s . " +
//		            " ?a <" + SEMAVocabulary.target + "> ?t . " +
//		            " ?a <http://purl.org/dc/elements/1.1/identifier> ?aid . " +
//		            " ?s a <" + type + "> . " +
//		            " ?s <http://purl.org/dc/elements/1.1/identifier> ?sid . " +
//		            " ?t a <" + type + "> . " +
//		            " ?t <http://purl.org/dc/elements/1.1/identifier> ?tid . " +
//		            " } }";
//
////		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
//		
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getByName(virtuoso).getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//			ResultSet rs = qe.execSelect();
//		
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//				
////				System.out.println(sol);
//				
//				res.add(new AlignmentDescriptor(
//				         new GraphDescriptor(sol.get("?a").toString(), sol.get("?aid").toString(), false, false),
//				         new GraphDescriptor(sol.get("?s").toString(), sol.get("?sid").toString(), false, false),
//				         new GraphDescriptor(sol.get("?t").toString(), sol.get("?tid").toString(), false, false)));
//			}
//				
//		}
//		
//		return res;
//	}
//
//}
