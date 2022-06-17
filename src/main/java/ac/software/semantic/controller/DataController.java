package ac.software.semantic.controller;

import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.config.VocabulariesBean;
import ac.software.semantic.config.VocabularyInfo;
import ac.software.semantic.model.VirtuosoConfiguration;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.semaspace.query.Searcher;
import edu.ntua.isci.ac.semaspace.query.URIDescriptor;
import io.swagger.v3.oas.annotations.tags.Tag;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

@Tag(name = "Data functions")
@RestController
@RequestMapping("/api/f/resources")
public class DataController {

	@Autowired
	@Qualifier("virtuoso-configuration")
	private Map<String,VirtuosoConfiguration> virtuosoConfiguration;
	
	@Autowired
	@Qualifier("vocabularies")
	private VocabulariesBean vocs;

	@Autowired
	@Qualifier("labels-cache")
	private Cache labelsCache;

	@Autowired
	@Qualifier("prefixes")
	private Set<URIDescriptor> prefixes;
	
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;
	
//    @RequestMapping(value = "/collections",
//    		        produces = "application/json")
//    public String collections(@RequestParam String type) throws Exception {
//    	String sparql = "CONSTRUCT { ?url a ?t . " +
//                " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . " +
//                " ?url <http://purl.org/dc/elements/1.1/creator> ?creator }" +
//                "FROM <http://sw.islab.ntua.gr/apollonis/core/graph> " +
//                "WHERE { ?url a <" + type + "> . ?url a ?t ." +
//                " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ." +
//    	        " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }";
//    	
//		QueryExecution qe = QueryExecutionFactory.sparqlService(config.getURL(), QueryFactory.create(sparql, Syntax.syntaxARQ));
//		Model model = qe.execConstruct();
//		
//		Writer sw = new StringWriter();
//		
//		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//
////		System.out.println(sw);
//		
//        return sw.toString();
//    }

////	@CrossOrigin(origins = "http://kimon.image.ece.ntua.gr:3000")
//    @RequestMapping(value = "/collections/search",
//    		        produces = "application/json")
//	public String searchCollections(@RequestParam("time") Optional<String> time , 
//			@RequestParam("endTime") Optional<String> endTime, 
//			@RequestParam("place") Optional<String> place, 
//			@RequestParam("type") Optional<String> type)  {
//
//		Writer sw = new StringWriter();
//		
//		try (QueryExecution qe = ApollonisWrapper.searchQuery(time.isPresent() ? time.get() : null, 
//				                                              endTime.isPresent() ? endTime.get(): null, 
//				                                              place.isPresent() ? place.get() : null, 
//				                                              type.isPresent() ? type.get() : null, 
//				                                              true)) {
//			Model model = qe.execConstruct();
//			
//			RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//		}
//		
//		return sw.toString();
//	}

////	@CrossOrigin(origins = "http://kimon.image.ece.ntua.gr:3000")
//    @RequestMapping(value = "/data-collection-entries",
//    		        produces = "application/json")
//    public String dataCollectionEntries(@RequestParam String collection) throws Exception {
//		
//    	String sparql = "CONSTRUCT { ?url <http://purl.org/dc/terms/source> ?source } " +
//                "WHERE { " +
//                " GRAPH <http://sw.islab.ntua.gr/apollonis/core/graph> { " +
//                "   <" + collection + "> <http://sw.islab.ntua.gr/apollonis/ms/class> [ " +
//                "      a <http://sw.islab.ntua.gr/apollonis/ms/CollectionResource> ; " +
//                "      <http://sw.islab.ntua.gr/apollonis/ms/uri> ?type ] } " + 
//                " GRAPH <" + collection + "> { " +
//                "   ?url a ?type ." +
//                "   OPTIONAL { ?url <http://purl.org/dc/terms/source> ?source } } " +
//                " }";
//
//		Writer sw = new StringWriter();
//
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(config.getURL(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//			Model model = qe.execConstruct();
//			
//			RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//		}
//        return sw.toString();
//    }	

//	@RequestMapping(value = "/label", produces = "application/json")
//	public String label(@RequestParam String resource)  {
//
//		Writer sw = new StringWriter();
//		
//		try {
//			if (!(resource.startsWith("http://") || resource.startsWith("https://"))) {
//				return sw.toString();
//			}
//		
//			int f = Math.max(resource.lastIndexOf("/"), resource.lastIndexOf("#"));
//			
//			String prefix = resource.substring(0, f + 1);
//
//			String sparql = 
//				"CONSTRUCT { " + "  <" + resource + "> <http://www.w3.org/2000/01/rdf-schema#label> ?label } " +
//				"WHERE { " +
//				"GRAPH <http://sw.islab.ntua.gr/semaspace/resource/graph/content> { " +
//				"   ?g <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?p . " +
//				"   ?p <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " +
//				"   ?p <http://sw.islab.ntua.gr/apollonis/ms/uri> ?uri } " +
//				"GRAPH ?g { " + 
//				"  <" + resource + "> ?uri ?label }" + "}";
//
////    	System.out.println(sparql);
//
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(),
//					QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//				Model model = qe.execConstruct();
//	
//				RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//		
//		return sw.toString();
//	}
	
	@GetMapping(value = "/label", produces = "application/json")
	public String label(@RequestParam String resource)  {


		Element e = labelsCache.get(resource);
		if (e != null) {
			return (String)e.getObjectValue();
		}
		
//		System.out.println(">>>> " + resource);
		Writer sw = new StringWriter();
		
		try {
			if (!(resource.startsWith("http://") || resource.startsWith("https://"))) {
				return sw.toString();
			}
		
			URIDescriptor prefix = Searcher.findPrefix(resource, prefixes);
			if (prefix == null) {
				return sw.toString();
			}
			
			VocabularyInfo vi = vocs.getMap().get(prefix.getPrefix());
			
			if (vi == null) {
				return sw.toString();
			}

			String sparql; 

			if (!vi.isRemote()) {
				sparql = legacyUris ?
					"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
					"WHERE { " +
					"GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
					"   <" + vi.getGraph() + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?p . " +
					"   ?p <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " +
					"   ?p <http://sw.islab.ntua.gr/apollonis/ms/uri> ?uri } " +
					"GRAPH <" + vi.getGraph() + "> { " + 
					"  <" + resource + "> ?uri ?label }" + "}" :
						
					"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
					"WHERE { " +
					"GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
					"   <" + vi.getGraph() + "> <" + SEMAVocabulary.dataProperty + "> ?p . " +
					"   ?p <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . " +
					"   ?p <" + SEMAVocabulary.uri + "> ?uri } " +
					"GRAPH <" + vi.getGraph() + "> { " + 
					"  <" + resource + "> ?uri ?label }" + "}";
						
			} else {
				sparql = legacyUris ? 
						"CONSTRUCT { " + "  <" + resource + "> <http://www.w3.org/2000/01/rdf-schema#label> ?label } " +
						"WHERE { " +
						"GRAPH <http://sw.islab.ntua.gr/semaspace/resource/graph/content> { " +
						"   <" + vi.getGraph() + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?p . " +
						"   ?p <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " +
						"   ?p <http://sw.islab.ntua.gr/apollonis/ms/uri> ?uri } " +
						"SERVICE <" + vi.getEndpoint() + "> { " + 
						"  <" + resource + "> ?uri ?label }" + "}" :
						
						"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
						"WHERE { " +
						"GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
						"   <" + vi.getGraph() + "> <" + SEMAVocabulary.dataProperty + "> ?p . " +
						"   ?p <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . " +
						"   ?p <" + SEMAVocabulary.uri + "> ?uri } " +
						"SERVICE <" + vi.getEndpoint() + "> { " + 
						"  <" + resource + "> ?uri ?label }" + "}";				
			}
			
//			System.out.println(resource);
//			System.out.println(prefix);
//			System.out.println(vi.getGraph() + " " + vi.getEndpoint());
//			System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vi.getVirtuoso().getSparqlEndpoint(),
					QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
				Model model = qe.execConstruct();
	
				RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
			}
			
			labelsCache.put(new Element(resource, sw.toString()));
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return sw.toString();
	}	

//    public static void main(String[] s) {
//    	String sparql = "CONSTRUCT { ?url a <http://sw.islab.ntua.gr/apollonis/ms/DataCollection> . " +
//                " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . " +
//                " ?url <http://purl.org/dc/elements/1.1/descriptor> ?descr }" +
//     "FROM <http://sw.islab.ntua.gr/apollonis/core/graph> " +
//     "WHERE { ?url a <http://sw.islab.ntua.gr/apollonis/ms/DataCollection> ." +
//            " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ." +
//    	    " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/descriptor> ?descr } }";
//
//		//IteratorSet is = new RDFJenaConnection(config.getURL()).executeQuery(sparql);
//		
//		QueryExecution qe = QueryExecutionFactory.sparqlService("http://kimon.image.ntua.gr:8890/sparql", QueryFactory.create(sparql, Syntax.syntaxARQ));
//		Model model = qe.execConstruct();
//		
//		Writer sw = new StringWriter();
//		
//		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//		
//		System.out.println(sw.toString());
//    }
}
