package ac.software.semantic.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.config.VocabulariesMap;
import ac.software.semantic.config.VocabularyInfo;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.PrefixEndpoint;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;

@Service
public class NamesService {

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
	@Autowired
	private SEMRVocabulary resourceVocabulary;

	public Collection<PrefixEndpoint> getPrefixEndpoints() {
		Set<PrefixEndpoint> res = new HashSet<>();
		
		String sparql =  
				"SELECT ?dataset ?endpoint ?prefix FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { " 
				+ "   ?dataset a ?tt . VALUES ?tt { <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . "
				+ "   ?dataset <" + VOIDVocabulary.sparqlEndpoint + "> ?endpoint . "
				+ "   ?dataset <" + SEMAVocabulary.clazz + "> ?clazz . "
				+ "   ?clazz a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
				+ "   ?clazz <" + SEMAVocabulary.prefix + "> ?prefix  ."
				+ "} ";

//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
//			System.out.println(vc.getSparqlEndpoint());
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
				ResultSet rs = qe.execSelect();

				while (rs.hasNext()) {
					QuerySolution qs = rs.next();
					String prefix = qs.get("prefix").toString();
					String endpoint = qs.get("endpoint").toString();
	
					res.add(new PrefixEndpoint(prefix, endpoint));
				}
			}
		}
		
		return res;
	}
	
	public void createDatasetsMap(VocabulariesMap vm, TripleStoreConfiguration vc, boolean legacyUris) {
		String sparql = legacyUris ? 
//				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { "  
				"SELECT ?d ?endpoint ?prefix FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { "  
				+ "   ?d a ?tt . VALUES ?tt { <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/endpoint> ?endpoint . } "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/class> ?cp . "
				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
				+ "              ?cp <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix } ."
//				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?dp . "
//				+ "              ?dp <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . "
//				+ "              ?dp <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProp . } "
				+ "} " :
					
//				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { " 
				"SELECT ?d ?endpoint ?prefix FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { " 
				+ "   ?d a ?tt . VALUES ?tt { <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.endpoint + "> ?endpoint . } "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.clazz + "> ?cp . "
				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
				+ "              ?cp <" + SEMAVocabulary.prefix + "> ?prefix } ."
//				+ "   OPTIONAL { ?d <" + SEMAVocabulary.dataProperty + "> ?dp . "
//				+ "              ?dp <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . "
//				+ "              ?dp <" + SEMAVocabulary.uri + "> ?labelProp . } " +
				+ "} ";
					

//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
			ResultSet rs = qe.execSelect();
//
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
//				System.out.println(qs);
//
				String graph = qs.get("d").toString();
				RDFNode prefix = qs.get("prefix");

				if (prefix != null) { // SHOULD NOT BE NULL!!

					RDFNode endpoint = qs.get("endpoint");

					VocabularyInfo vi;
					if (endpoint == null) {
						vi = new VocabularyInfo(graph);
					} else {
						vi = new VocabularyInfo(graph, endpoint.toString());
					}
					vi.setVirtuoso(vc);
					
					vm.addMap(graph, prefix.toString(), vi);
				}
			}
		}
	}

	public Map<String, VocabularyInfo> createVocabulariesMap(TripleStoreConfiguration vc, boolean legacyUris) {
		String sparql = legacyUris ? 
//				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp " + "WHERE { " + "GRAPH <"
				"SELECT ?d ?endpoint ?prefix " + "WHERE { " + "GRAPH <"				
				+ resourceVocabulary.getContentGraphResource() + "> { " + "   ?d a <" + SEMAVocabulary.VocabularyCollection + "> . "
//				+ "   ?d <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/endpoint> ?endpoint . } "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/class> ?cp . "
				+ "              ?cp <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix } ."
//				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?dp . "
//				+ "              ?dp <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . "
//				+ "              ?dp <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProp . } " 
				+ "} }" :
					
//				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp " + "WHERE { " + "GRAPH <"
				"SELECT ?d ?endpoint ?prefix " + "WHERE { " + "GRAPH <"
				+ resourceVocabulary.getContentGraphResource() + "> { " + "   ?d a <" + SEMAVocabulary.VocabularyCollection + "> . "
//				+ "   ?d <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.endpoint + "> ?endpoint . } "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.clazz + "> ?cp . "
				+ "              ?cp <" + SEMAVocabulary.prefix + "> ?prefix } ."
//				+ "   OPTIONAL { ?d <" + SEMAVocabulary.dataProperty + "> ?dp . "
//				+ "              ?dp <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . "
//				+ "              ?dp <" + SEMAVocabulary.uri + "> ?labelProp . } "
				+ "} }";
					

		Map<String, VocabularyInfo> map = new HashMap<>();

//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
			ResultSet rs = qe.execSelect();
//
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
//				System.out.println(qs);
//
				String graph = qs.get("d").toString();
				RDFNode prefix = qs.get("prefix");

				if (prefix != null) { // SHOULD NOT BE NULL!!
//					String identifier = qs.get("identifier").toString();

					RDFNode endpoint = qs.get("endpoint");

					VocabularyInfo vi = map.get(prefix.toString());
					if (vi == null) {
						if (endpoint == null) {
							vi = new VocabularyInfo(graph);
						} else {
							vi = new VocabularyInfo(graph, endpoint.toString());
						}
						vi.setVirtuoso(vc);
						
						map.put(prefix.toString(), vi);
					}
				}
			}
		}

//		System.out.println("END");

		return map;
	}

	public void createDatasetsMap(VocabulariesMap vm, TripleStoreConfiguration vc, String graph, boolean legacyUris) {
		String sparql = legacyUris ? 
//				"SELECT ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { "  
				"SELECT ?endpoint ?prefix FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { "  
				+ "   <" + graph + "> a ?t . "
				+ "   OPTIONAL { <" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/endpoint> ?endpoint . } "
				+ "   OPTIONAL { <" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/class> ?cp . "
				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
				+ "              ?cp <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix } ."
//				+ "   OPTIONAL { <" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?dp . "
//				+ "              ?dp <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . "
//				+ "              ?dp <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProp . }" 
				+ "} " :
					
//				"SELECT ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { " 
				"SELECT ?endpoint ?prefix FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { " 
				+ "   <" + graph + "> a ?t . "
				+ "   OPTIONAL { <" + graph + "> <" + SEMAVocabulary.endpoint + "> ?endpoint . } "
				+ "   OPTIONAL { <" + graph + "> <" + SEMAVocabulary.clazz + "> ?cp . "
				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
				+ "              ?cp <" + SEMAVocabulary.prefix + "> ?prefix } ."
//				+ "   OPTIONAL { <" + graph + "> <" + SEMAVocabulary.dataProperty + "> ?dp . "
//				+ "              ?dp <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . "
//				+ "              ?dp <" + SEMAVocabulary.uri + "> ?labelProp . } "
				+ "} ";
					

//		Map<String, VocabularyInfo> map = new HashMap<>();

//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
			ResultSet rs = qe.execSelect();
//
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
//				System.out.println(qs);
//
				RDFNode prefix = qs.get("prefix");

				if (prefix != null) { // SHOULD NOT BE NULL!!
//					String identifier = qs.get("identifier").toString();

					RDFNode endpoint = qs.get("endpoint");

					VocabularyInfo vi;
					if (endpoint == null) {
						vi = new VocabularyInfo(graph);
					} else {
						vi = new VocabularyInfo(graph, endpoint.toString());
					}
					vi.setVirtuoso(vc);
					
					vm.addMap(graph, prefix.toString(), vi);
				}
			}
		}
	}
	
//	public static Map<String, VocabularyInfo> createDatasetsMap(TripleStoreConfiguration vc, boolean legacyUris) {
//		String sparql = legacyUris ? 
//				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { "  
//				+ "   ?d a ?tt . VALUES ?tt { <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . "
//				+ "   ?d <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
//				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/endpoint> ?endpoint . } "
//				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/class> ?cp . "
//				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
//				+ "              ?cp <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix } ."
//				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?dp . "
//				+ "              ?dp <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . "
//				+ "              ?dp <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProp . } } " :
//					
//				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { " 
//				+ "   ?d a ?tt . VALUES ?tt { <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . "
//				+ "   ?d <" + DCTVocabulary.identifier + "> ?identifier . "
//				+ "   OPTIONAL { ?d <" + SEMAVocabulary.endpoint + "> ?endpoint . } "
//				+ "   OPTIONAL { ?d <" + SEMAVocabulary.clazz + "> ?cp . "
//				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
//				+ "              ?cp <" + SEMAVocabulary.prefix + "> ?prefix } ."
//				+ "   OPTIONAL { ?d <" + SEMAVocabulary.dataProperty + "> ?dp . "
//				+ "              ?dp <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . "
//				+ "              ?dp <" + SEMAVocabulary.uri + "> ?labelProp . } } ";
//					
//
//		Map<String, VocabularyInfo> map = new HashMap<>();
//
////		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
//
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//			ResultSet rs = qe.execSelect();
////
//			while (rs.hasNext()) {
//				QuerySolution qs = rs.next();
////				System.out.println(qs);
////
//				String graph = qs.get("d").toString();
//				RDFNode prefix = qs.get("prefix");
//
//				if (prefix != null) { // SHOULD NOT BE NULL!!
////					String identifier = qs.get("identifier").toString();
//
//					RDFNode endpoint = qs.get("endpoint");
//
//					VocabularyInfo vi = map.get(prefix.toString());
//					if (vi == null) {
//						if (endpoint == null) {
//							vi = new VocabularyInfo(graph);
//						} else {
//							vi = new VocabularyInfo(graph, endpoint.toString());
//						}
//						vi.setVirtuoso(vc);
//						
//						map.put(prefix.toString(), vi);
//					}
//				}
//			}
//		}
//
//		return map;
//	}
//
//	public static Map<String, VocabularyInfo> createDatasetsMap(TripleStoreConfiguration vc, String graph, boolean legacyUris) {
//		String sparql = legacyUris ? 
//				"SELECT ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { "  
//				+ "   <" + graph + "> <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
//				+ "   OPTIONAL { <" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/endpoint> ?endpoint . } "
//				+ "   OPTIONAL { <" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/class> ?cp . "
//				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
//				+ "              ?cp <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix } ."
//				+ "   OPTIONAL { <" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?dp . "
//				+ "              ?dp <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . "
//				+ "              ?dp <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProp . } } " :
//					
//				"SELECT ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { " 
//				+ "   <" + graph + "> <" + DCTVocabulary.identifier + "> ?identifier . "
//				+ "   OPTIONAL { <" + graph + "> <" + SEMAVocabulary.endpoint + "> ?endpoint . } "
//				+ "   OPTIONAL { <" + graph + "> <" + SEMAVocabulary.clazz + "> ?cp . "
//				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
//				+ "              ?cp <" + SEMAVocabulary.prefix + "> ?prefix } ."
//				+ "   OPTIONAL { <" + graph + "> <" + SEMAVocabulary.dataProperty + "> ?dp . "
//				+ "              ?dp <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . "
//				+ "              ?dp <" + SEMAVocabulary.uri + "> ?labelProp . } } ";
//					
//
//		Map<String, VocabularyInfo> map = new HashMap<>();
//
////		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
//
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//			ResultSet rs = qe.execSelect();
////
//			while (rs.hasNext()) {
//				QuerySolution qs = rs.next();
////				System.out.println(qs);
////
//				RDFNode prefix = qs.get("prefix");
//
//				if (prefix != null) { // SHOULD NOT BE NULL!!
////					String identifier = qs.get("identifier").toString();
//
//					RDFNode endpoint = qs.get("endpoint");
//
//					VocabularyInfo vi = map.get(prefix.toString());
//					if (vi == null) {
//						if (endpoint == null) {
//							vi = new VocabularyInfo(graph);
//						} else {
//							vi = new VocabularyInfo(graph, endpoint.toString());
//						}
//						vi.setVirtuoso(vc);
//						
//						map.put(prefix.toString(), vi);
//					}
//				}
//			}
//		}
//
//		return map;
//	}
	
}