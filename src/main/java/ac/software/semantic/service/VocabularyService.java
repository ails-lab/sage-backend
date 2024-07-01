package ac.software.semantic.service;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.ResourceContext;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;


@Service
public class VocabularyService {

	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;
	
	@Autowired
	@Qualifier("label-jsonld-context")
    private Map<String, Object> labelContext;

	@Autowired
	@Qualifier("labels-cache")
	private Cache labelsCache;
	
	@Autowired
    private APIUtils apiUtils;

	public String prefixize(String resource) {
		return vocc.prefixize(resource);
	}
	
	public String onPathStringListAsPrettyString(List<String> path) {
		String s = "";
		
//		boolean beforeClass = false;
		for (int i = 0; i < path.size(); ) {
//			if (!beforeClass && i > 0) {
//				s += "/" ;
//			}
			if (i > 0) {
				s += " / " ;
			}
			if (path.get(i).equals(RDFSVocabulary.Class.toString())) {
				String p = path.get(++i);
				String pretty = prefixize(p);
				if (pretty.equals(p)) {
					pretty = "<" + pretty + ">";
				}
				s += pretty;
//				s += "[" + prefixize(path.get(++i)) + "]";
//				beforeClass = true;
			} else {
				String p = path.get(i);
				String pretty = prefixize(p);
				if (pretty.equals(p)) {
					pretty = "<" + pretty + ">";
				}
				s += pretty;
//				s += "<" + prefixize(path.get(i)) + ">";
//				beforeClass = false;
			}
			i++;
		}
	
		return s.trim();
	}
	
	public List<Object> getLabel(String resource, List<ResourceContext> ai, boolean trySystemVocabularies) {
		Element e = labelsCache.get(resource);
		if (e != null) {
			return (List<Object>)e.getObjectValue();
		}
		
//		System.out.println(">>>RES " + resource);
		List<Object> res = null;
		
		try {
			if (!(resource.startsWith("http://") || resource.startsWith("https://"))) {
				return new ArrayList<>();
			}

			List<ResourceContext> voc = null;
			
			if (trySystemVocabularies) {
				voc = (List<ResourceContext>)(List<?>)vocc.resolve(resource);
			}
			
			if (voc == null) {
				voc = new ArrayList<>();
				if (ai != null) {
					voc.addAll(ai);
				}
			}
			
			for (ResourceContext rc : voc) {
				String iendpoint = null;
				String sparql = null;
	
				iendpoint = rc.getSparqlEndpoint();
				sparql = rc.getSparqlQueryForResource(resource);
	
				Map<String, Object> jn = resolve(resource, iendpoint, sparql);
				if (jn != null) {
					res = (List<Object>)jn.get("@graph");
					break;
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		if (res == null) {
			res = new ArrayList<>();
		}

		labelsCache.put(new Element(resource, res));
		
		return res;
	}
	
	public List<Object> getLabel(String resource, VocabularyContainer<ResourceContext> vcont) {
		Element e = labelsCache.get(resource);
		if (e != null) {
			return (List<Object>)e.getObjectValue();
		}
		
//		System.out.println(">>>RES " + resource);
		List<Object> res = null;
		
		try {
			if (!(resource.startsWith("http://") || resource.startsWith("https://"))) {
				return new ArrayList<>();
			}

			List<ResourceContext> voc = vcont.resolve(resource);
			
			for (ResourceContext rc : voc) {

				String iendpoint = null;
				String sparql = null;
	
				iendpoint = rc.getSparqlEndpoint();
				sparql = rc.getSparqlQueryForResource(resource);
				
//				System.out.println(iendpoint);
//				System.out.println(sparql);
	
				Map<String, Object> jn = resolve(resource, iendpoint, sparql);
				if (jn != null) {
					res = (List<Object>)jn.get("@graph");
					break;
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		if (res == null) {
			res = new ArrayList<>();
		}

		labelsCache.put(new Element(resource, res));
	
		return res;
	}
	
	
	private Map<String, Object> resolve(String resource, String iendpoint, String sparql) {
		Map<String, Object> jn = null;
//		jn.put("@graph", new ArrayList<>());
		
		boolean resolved = false;
		if (iendpoint != null && sparql != null) {

//			System.out.println(iendpoint);
//			System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(iendpoint, QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
				Model model = qe.execConstruct();

		    	jn = apiUtils.jsonLDFrame(model, (Map)labelContext.get("label-jsonld-context"));
		    	resolved = true;
			} catch (Exception ex) {
//				ex.printStackTrace();
			}
		}
			
		if (!resolved) {
			Model m = ModelFactory.createDefaultModel();
			
			if (sparql == null) { // try a default sparql
				sparql = 
					"CONSTRUCT { " + 
					  "  ?resource <" + RDFSVocabulary.label + "> ?label . " +
					  "  ?resource <" + DCTVocabulary.description + "> ?description } " +
					"WHERE { " +
					"  VALUES ?resource { <" + resource + "> } " +
					"  ?resource <" + RDFSVocabulary.label + ">|<" + SKOSVocabulary.prefLabel + ">|<" + DCTVocabulary.title + "> ?label . " +
			        "  OPTIONAL { ?resource <" + SKOSVocabulary.scopeNote + ">|<" + DCTVocabulary.description + "> ?description } }";
			}
			
			try {
				m.read(resource);
				
				try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), m)) {
					Model model = qe.execConstruct();
		
			    	jn = apiUtils.jsonLDFrame(model, (Map)labelContext.get("label-jsonld-context"));
			    	resolved = true;
				}
			} catch (Exception ex) {
//				System.out.println(ex);
			}
		}
		
		return jn;
	}
	
	
	// do not use 
	private List<Object> getGeonamesLabel(String resource) {
		Map<String, Object> jn = new HashMap<>();
		jn.put("@graph", new ArrayList<>());
		
		try {

			String sparql = null;

			if (resource.startsWith("https://sws.geonames.org/")) {
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

//							RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
					    	jn = apiUtils.jsonLDFrame(model, (Map)labelContext.get("label-jsonld-context"));
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
								
//								RDFDataMgr.write(sw, newModel, RDFFormat.JSONLD_EXPAND_PRETTY);
						    	jn = apiUtils.jsonLDFrame(newModel, (Map)labelContext.get("label-jsonld-context"));

								labelsCache.put(new Element(resource, jn.get("@graph")));
								
							}
						}
					}					
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				
				return (List<Object>)jn.get("@graph");
			}
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return (List<Object>)jn.get("@graph");
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

}