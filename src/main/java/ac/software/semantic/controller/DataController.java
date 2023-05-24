package ac.software.semantic.controller;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;
import java.util.SortedMap;

import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.hsqldb.lib.StringInputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.config.VocabulariesBean;
import ac.software.semantic.config.VocabularyInfo;
import ac.software.semantic.model.TripleStoreConfiguration;
import edu.ntua.isci.ac.common.utils.SimpleTrie;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSXLVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.semaspace.query.URIDescriptor;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

@Tag(name = "Data functions")
@RestController
@RequestMapping("/api/f/resources")
public class DataController {

	@Autowired
	@Qualifier("triplestore-configurations")
	private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfiguration;
	
	@Autowired
	@Qualifier("vocabularies")
	private VocabulariesBean vocs;

	@Autowired
	@Qualifier("labels-cache")
	private Cache labelsCache;

	@Autowired
	@Qualifier("prefixes")
	private SimpleTrie<URIDescriptor> prefixesTrie;
	
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
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
//				"GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
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
	
    
	public URIDescriptor findPrefix(String url) {
		
		Map.Entry<String, URIDescriptor> res = prefixesTrie.getLongestPrefix(url);
		if (res != null) {
			return res.getValue();
		}
		
		return null;
		
//		SortedMap<String, URIDescriptor> headMap = prefixesTrie.headMap(url);
//		
//		if (headMap.size() > 0) {
//			String key = headMap.lastKey();
//			if (url.startsWith(key)) {
//				return prefixesTrie.get(key);
//			}
//		}
////		
//		return null;
	}
	

	private String wraptoArray(String s) {
		if (s.length() == 0) {
			return "[]";
		} else {
			return s;
		}
	}
    
	@GetMapping(value = "/label", produces = "application/json")
	public String label(@RequestParam String resource)  {

		Element e = labelsCache.get(resource);
		if (e != null) {
			return wraptoArray((String)e.getObjectValue());
		}
		
//		System.out.println(">>>RES " + resource);
		Writer sw = new StringWriter();
		
		try {
			if (!(resource.startsWith("http://") || resource.startsWith("https://"))) {
				return wraptoArray(sw.toString());
			}
		
			String sparql = null;
			String endpoint = null;
			
			// temp hard coded >> should move to mongo configuration
			
			if (resource.startsWith("http://www.wikidata.org/entity/")) {
				sparql = "CONSTRUCT { <" + resource + "> <" + RDFSVocabulary.label + "> ?label . " + 
						              "<" + resource + "> <" + DCTVocabulary.description + "> ?description } " +
				"WHERE { " +
				" <" + resource + "> <" + RDFSVocabulary.label + "> ?label . " +
				" OPTIONAL { <" + resource + "> <http://schema.org/description> ?description . FILTER (lang(?description) = \"en\") } " +
				"}";
				
				endpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";

			} else if (resource.startsWith("http://vocab.getty.edu/")) {
					sparql = "CONSTRUCT { <" + resource + "> <" + RDFSVocabulary.label + "> ?label  } " +
					"WHERE { " +
					" <" + resource + "> <" + SKOSXLVocabulary.prefLabel + "> [ " +
					" <" + SKOSXLVocabulary.literalForm + "> ?label ; " +
					" <http://vocab.getty.edu/ontology#displayOrder> \"1\"^^<http://www.w3.org/2001/XMLSchema#positiveInteger>  ] " +
					"}";
					
					endpoint = "http://vocab.getty.edu/sparql";

			} else if (resource.startsWith("https://sws.geonames.org/")) {
				Model m = ModelFactory.createDefaultModel();
				try {
//					System.out.println("Reading " + resource);
					m.read(resource);

					boolean geoPath = false; // true is not well because geonames does not respond to all calls
					
					if (!geoPath) {
						sparql = 
								"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
								"WHERE { " +
								"  <" + resource + "> <http://www.geonames.org/ontology#name> ?label . " +
								"}";		
	
						try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), m)) {
							Model model = qe.execConstruct();
				
							RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
						}

					} else {
						sparql = 
								"SELECT * " +
								"WHERE { " +
								"  <" + resource + "> <http://www.geonames.org/ontology#name> ?label . " +
								"  OPTIONAL { <" + resource + "> <http://www.geonames.org/ontology#parentCountry> ?country } " +
								"  OPTIONAL { <" + resource + "> <http://www.geonames.org/ontology#parentADM1> ?parent1 } " +
								"  OPTIONAL { <" + resource + "> <http://www.geonames.org/ontology#parentADM2> ?parent2 } " +
								"  OPTIONAL { <" + resource + "> <http://www.geonames.org/ontology#parentADM3> ?parent3 } " + 
								"  OPTIONAL { <" + resource + "> <http://www.geonames.org/ontology#parentADM4> ?parent4 } " +
								"}";		
						
	
						try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), m)) {
							ResultSet rs = qe.execSelect();
							
							if (rs.hasNext()) {
								QuerySolution sol = rs.next();
								
								Model nextModel = geonamesIter(sol, new String[] { "parent4", "parent3", "parent2", "parent1", "country" }, 0);
								
								String label = sol.get("label").toString();
	
								StmtIterator labelIter = nextModel.listStatements(null, RDFSVocabulary.label, (RDFNode)null);
								StmtIterator descrIter = nextModel.listStatements(null, DCTVocabulary.description, (RDFNode)null);
	
								String nextLabel = null;
								String nextDescr = null;
								
								if (labelIter.hasNext()) {
									nextLabel = labelIter.next().getObject().toString();
								}
								
								if (descrIter.hasNext()) {
									nextDescr = descrIter.next().getObject().toString();
								}
								
								Model newModel = ModelFactory.createDefaultModel();
								newModel.add(newModel.createResource(resource), RDFSVocabulary.label, newModel.createLiteral(label));
								String descr = "";
								if (nextDescr != null) {
									descr = nextDescr + " > ";
								} 
								if (nextLabel != null) {
									descr = descr + nextLabel;
								}
								
								if (descr.length() > 0) {
									newModel.add(newModel.createResource(resource), DCTVocabulary.description, descr);
								}
								
								RDFDataMgr.write(sw, newModel, RDFFormat.JSONLD_EXPAND_PRETTY);
								labelsCache.put(new Element(resource, sw.toString()));
								
							}
						}
					}					
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				
				return wraptoArray(sw.toString());
				
			} else {
				URIDescriptor prefix = findPrefix(resource);
				if (prefix == null) {
					
					Model m = ModelFactory.createDefaultModel();
					try {
						m.read(resource);
						
						sparql = 
								"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
								"WHERE { " +
								"  <" + resource + "> <" + RDFSVocabulary.label + ">|<" + SKOSVocabulary.prefLabel + "> ?label }";		
						
						try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), m)) {
							Model model = qe.execConstruct();
				
							RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
						}
						
						labelsCache.put(new Element(resource, sw.toString()));
						
					} catch (Exception ex) {
//						System.out.println(ex);
					}
					
					return wraptoArray(sw.toString());
				}
				
				VocabularyInfo vi = vocs.getMap().get(prefix.getPrefix());
				
				if (vi == null) {
					return wraptoArray(sw.toString());
				}
	
				if (!vi.isRemote()) {
					sparql = legacyUris ?
						"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
						"WHERE { " +
						"GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
						"   <" + vi.getGraph() + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?p . " +
						"   ?p <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " +
						"   ?p <http://sw.islab.ntua.gr/apollonis/ms/uri> ?uri } " +
						"GRAPH <" + vi.getGraph() + "> { " + 
						"  <" + resource + "> ?uri ?label }" + "}" 
						:
						"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
						"WHERE { " +
						"GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
						"   <" + vi.getGraph() + "> <" + SEMAVocabulary.dataProperty + "> ?p . " +
						"   ?p <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . " +
						"   ?p <" + SEMAVocabulary.uri + "> ?uri } " +
						"GRAPH <" + vi.getGraph() + "> { " + 
						"  <" + resource + "> ?uri ?label }" + "}";
							
				} else {
					sparql = legacyUris ? 
							"CONSTRUCT { " + "  <" + resource + "> <http://www.w3.org/2000/01/rdf-schema#label> ?label } " +
							"WHERE { " +
							"GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
							"   <" + vi.getGraph() + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?p . " +
							"   ?p <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " +
							"   ?p <http://sw.islab.ntua.gr/apollonis/ms/uri> ?uri } " +
							"SERVICE <" + vi.getEndpoint() + "> { " + 
							"  <" + resource + "> ?uri ?label }" + "}" :
							
							"CONSTRUCT { " + "  <" + resource + "> <" + RDFSVocabulary.label + "> ?label } " +
							"WHERE { " +
							"GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
							"   <" + vi.getGraph() + "> <" + SEMAVocabulary.dataProperty + "> ?p . " +
							"   ?p <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . " +
							"   ?p <" + SEMAVocabulary.uri + "> ?uri } " +
							"SERVICE <" + vi.getEndpoint() + "> { " + 
							"  <" + resource + "> ?uri ?label }" + "}";				
				}
				
				endpoint = vi.getVirtuoso().getSparqlEndpoint();
			}
			
//			System.out.println(resource);
//			System.out.println(prefix);
//			System.out.println(vi.getGraph() + " " + vi.getEndpoint());
//			System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
				Model model = qe.execConstruct();
	
				RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
			}
			
			labelsCache.put(new Element(resource, sw.toString()));
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return wraptoArray(sw.toString());
	}	

	private Model geonamesIter(QuerySolution sol, String[] props, int index) {
		if (index > props.length - 1) {
			return ModelFactory.createDefaultModel();
		}
		
		RDFNode prop = sol.get(props[index]);
		
		if (prop != null) {
			Element e = labelsCache.get(prop.toString());
			if (e != null) {
				Model tmodel = ModelFactory.createDefaultModel();
				try (StringReader sr = new StringReader((String)e.getObjectValue())) {
					RDFDataMgr.read(tmodel, sr, null, Lang.JSONLD);
				}

				return tmodel;
			}

			Model tmodel = ModelFactory.createDefaultModel();
//			System.out.println("Reading " + prop.toString());
			tmodel.read(prop.toString());
				
			String sparql = 
					"SELECT * " +
					"WHERE { " +
					"  <" + prop.toString() + "> <http://www.geonames.org/ontology#name> ?label . " +
					"}";		

			
			String label = null;
			try (QueryExecution tqe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), tmodel)) {
				ResultSet trs = tqe.execSelect();

				if (trs.hasNext()) {
					label = trs.next().get("label").toString();
				}
			}
			
//			System.out.println("Label " + label);
			
			Model nextModel = geonamesIter(sol, props, index + 1);
			
			if (nextModel.size() == 0) {

				Model newModel = ModelFactory.createDefaultModel();
				newModel.add(prop.asResource(), RDFSVocabulary.label, newModel.createLiteral(label));

				try (Writer tsw = new StringWriter()) {
					RDFDataMgr.write(tsw, newModel, RDFFormat.JSONLD_EXPAND_PRETTY);
					labelsCache.put(new Element(prop.toString(), tsw.toString()));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				return newModel;
			} else {
				StmtIterator labelIter = nextModel.listStatements(null, RDFSVocabulary.label, (RDFNode)null);
				StmtIterator descrIter = nextModel.listStatements(null, DCTVocabulary.description, (RDFNode)null);

				String nextLabel = null;
				String nextDescr = null;
				
				if (labelIter.hasNext()) {
					nextLabel = labelIter.next().getObject().toString();
				}
				
				if (descrIter.hasNext()) {
					nextDescr = descrIter.next().getObject().toString();
				}
				
//				System.out.println("Next label " + nextLabel);
//				System.out.println("Next description " + nextDescr);
				
				Model newModel = ModelFactory.createDefaultModel();
				newModel.add(prop.asResource(), RDFSVocabulary.label, newModel.createLiteral(label));
				
				String descr = "";
				if (nextDescr != null) {
					descr = nextDescr + " > ";
				} 
				if (nextLabel != null) {
					descr = descr + nextLabel;
				}
				
				if (descr.length() > 0) {
					newModel.add(prop.asResource(), DCTVocabulary.description, newModel.createLiteral(descr));
				}
				
				
				
				
				try (Writer tsw = new StringWriter()) {
					RDFDataMgr.write(tsw, newModel, RDFFormat.JSONLD_EXPAND_PRETTY);
					labelsCache.put(new Element(prop.toString(), tsw.toString()));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				return newModel;
			}
			
		} else {
			return geonamesIter(sol, props, index + 1);
		}
	
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
