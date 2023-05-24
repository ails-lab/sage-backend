package ac.software.semantic.service;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.PathElementType;
import ac.software.semantic.payload.ClassStructureResponse;
import ac.software.semantic.repository.DatasetRepository;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.lod.vocabularies.DCAMVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCATVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;

@Service
public class SchemaService {

	@Autowired
	DatasetRepository datasetRepository;
	
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;
    
	@Autowired
	private SEMRVocabulary resourceVocabulary;


    public DatasetCatalog asCatalog(String uuid) {
    	Optional<Dataset> dsOpt = datasetRepository.findByUuid(uuid);
    	
    	if (!dsOpt.isPresent()) {
    		return null;
    	}
    	
    	Dataset dataset = dsOpt.get();
    	
    	DatasetCatalog dc = new DatasetCatalog(dataset);
    	
    	if (dataset.getDatasets() != null) {
	    	for (ObjectId id : dataset.getDatasets()) {
	    		Optional<Dataset> dssOpt = datasetRepository.findById(id);
	    		if (dssOpt.isPresent()) {
	    			dc.addMember(dssOpt.get());
	    		}
	    	}
    	}
    	
    	return dc;
    }
    
	public String buildFromClause(DatasetCatalog dcg) {
		String from = "FROM <" + resourceVocabulary.getDatasetAsResource(dcg.getDataset().getUuid()) + "> ";
		for (Dataset dataset : dcg.getMembers()) {
			from += "FROM <" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + "> ";
		}
		
		return from;
	}
	
	public String buildUsingClause(DatasetCatalog dcg) {
		String from = "USING <" + resourceVocabulary.getDatasetAsResource(dcg.getDataset().getUuid()) + "> ";
		for (Dataset dataset : dcg.getMembers()) {
			from += "USING <" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + "> ";
		}
		
		return from;
	}

	private void explore(TripleStoreConfiguration vc, List<PathElement> path, String datasetUri, Model outModel, boolean ranges, String fromClause) {
		
//		System.out.println("EXPLORING " + path.size());
		String spath = "";
		String endVar = "";
		
		String modelPath = "BIND(<" + datasetUri + "> AS ?c0) . ";
		
		int pathLength = 0;
		int modelLength = 1;
		for (int i = 0; i < path.size(); i++) {
			PathElement pe = path.get(i);
			if (pe.isClass()) {
				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
				
				modelPath += "?c" + (modelLength-1) +  " <" + VOIDVocabulary.classPartition + "> ?c" + modelLength + " . ?c" + modelLength +  " <" + VOIDVocabulary.clazz + "> " + "<" + pe.getUri() + "> ." ;
				modelLength++;
			} else {
				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
				endVar = "c" + (pathLength + 1);
			    pathLength++;

			    modelPath += "?c" + (modelLength-1) +  " <" + VOIDVocabulary.propertyPartition + "> ?c" + modelLength + " . ?c" + modelLength +  " <" + VOIDVocabulary.property + "> " + "<" + pe.getUri() + "> ." ;
			    modelLength++;
			}
		}
		
		modelPath = "SELECT ?c" + (modelLength - 1) + " WHERE { " + modelPath + " } ";
				
		Resource subject = null;
		try (QueryExecution lqe = QueryExecutionFactory.create(modelPath, outModel)) {
			ResultSet qrs = lqe.execSelect();
			while (qrs.hasNext()) {
				QuerySolution qqs = qrs.next();
				subject = qqs.get("?c" + (modelLength - 1)).asResource();
			}
		}
		
//		System.out.println("EXPLORING " + spath);
		if (pathLength == 0) {
			endVar = "c0";
////			spath = "?entry  ?prop ?z . FILTER NOT EXISTS { ?k1 ?k2 ?entry }" +
////					" OPTIONAL { " +
////		            "  ?entry  ?prop ?b  " + 
////		            "  FILTER isBlank(?b) } ";
////			spath = "?c0 ?prop ?z . ?c0 a ?type . FILTER isIRI(?c0) " +
//			spath = spath + "?" + endVar + " ?prop ?z . " + // " FILTER (isIRI(?c0))" + // FILTER is not necessary here and may causes virtuoso timeout
//					" OPTIONAL { " +
//		            "  ?" + endVar + " ?prop ?b  " + 
//		            "  FILTER isBlank(?b) " +
//		            "} ";
//		} else {
////			spath = "?entry ?p1 ?p2 . FILTER NOT EXISTS { ?k1 ?k2 ?entry } " +
////					"?entry " + spath + " ?z0 . ?z0 ?prop ?z . " +
//// 			        " OPTIONAL { " +
////                    "  ?z0  ?prop ?b  " + 
////                    "  FILTER isBlank(?b) } ";
////			spath = "?c0 ?p1 ?p2 . FILTER (isIRI(?c0)) " +
//			spath = //"FILTER (isIRI(?c0)) " + // is FILTER necessary here?
////					spath + " ?" + endVar + " ?prop ?z . ?" + endVar + " a ?type . " +
//                    spath + " ?" + endVar + " ?prop ?z . " +
// 			        " OPTIONAL { " +
//                    "  ?" + endVar + " ?prop ?b  " + 
//                    "  FILTER isBlank(?b) " +
//			        "} ";			
		}
		
		spath = spath + "?" + endVar + " ?prop ?z . ";
		

		Map<Resource, List<Resource>> rangeMap = new HashMap<>();
		
		if (ranges) {
			// Get properties ranges 
			String sparql = 
			    	"SELECT ?prop ?type " +
			        fromClause +
			        "WHERE { " +
			    	     spath +
	 				"    ?z a ?type  " +
	                " } " +
			 		"GROUP BY ?prop ?type";
			
			Query query = QueryFactory.create(sparql);

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), query)) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					Resource prop = sol.getResource("prop");
					if (prop.getURI().equals(RDFVocabulary.type.toString())) {
						continue;
					}
	
					List<Resource> list = rangeMap.get(prop);
					if (list == null) {
						list = new ArrayList<>();
						rangeMap.put(prop, list);
					}
					list.add(sol.getResource("type"));
				}
			}
		}
		
		// Get properties 
		String sparql = 
		    	"SELECT ?prop (count(?z) AS ?countz) (count(DISTINCT ?z) AS ?distinctCountz) (count(?b) AS ?countb) " +
		        fromClause +
		        "WHERE { " +
		    	     spath +
 				"    OPTIONAL { " +
	            "      ?" + endVar + " ?prop ?b  " + 
	            "      FILTER isBlank(?b) } " +
                "} " +
		 		"GROUP BY ?prop";

		
		Query query = QueryFactory.create(sparql);
		System.out.println("Getting properties");
		System.out.println(query);
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), query)) {
			
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				Resource prop = sol.getResource("prop");
				if (prop.toString().equals(RDFVocabulary.type.toString())) {
					continue;
				}

				Resource partition = outModel.createResource();
				outModel.add(subject, VOIDVocabulary.propertyPartition, partition);
				partition.addProperty(RDFVocabulary.type, VOIDVocabulary.Dataset);
				partition.addProperty(VOIDVocabulary.property, prop);
				partition.addProperty(VOIDVocabulary.triples, sol.get("countz"));
				partition.addProperty(VOIDVocabulary.distinctObjects, sol.get("distinctCountz"));
				
				List<Resource> list = rangeMap.get(prop);
				if (list != null) {
					for (Resource r : list) {
						partition.addProperty(DCAMVocabulary.rangeIncludes, r);
					}
				}
				
				
				List<PathElement> newPath = new ArrayList<>();
				newPath.addAll(path);
				newPath.add(PathElement.createPropertyPathElement(prop.toString()));

				int blank = sol.get("countb").asLiteral().getInt();
				if (blank > 0) {
					explore(vc, newPath, datasetUri, outModel, ranges, fromClause);
				}
				
			}
		}
	}

	private List<Resource> getTopClasses(TripleStoreConfiguration vc, String datasetUri, Model outModel, String fromClause) {
//		String lsparql =
//			"SELECT  ?clazz (count(?x) as ?count)" +
//	        "WHERE { " +
//	        "  GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
//	        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//	    	"	 <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/class" : SEMAVocabulary.clazz) + "> ?t . " +
//	    	"    ?t  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> ?clazz . } " +
//            "  GRAPH <" + datasetUri + "> { " +
//            "    ?x a ?clazz } " +
//	    	"  } " +
//	    	"GROUP BY ?clazz ";

		String lsparql =
				"SELECT  ?clazz (count(?x) as ?count)" +
				fromClause + 
		        "WHERE { " +
	            "    ?x a ?clazz . FILTER isIRI(?x)  " +
		    	"}  " +
		    	"GROUP BY ?clazz ";

		System.out.println("Getting top classes");
//		System.out.println(vc.getSparqlEndpoint());
		System.out.println(QueryFactory.create(lsparql));
		List<Resource> result = new ArrayList<>();

		Resource subject = outModel.createResource(datasetUri);
		outModel.add(subject, RDFVocabulary.type, VOIDVocabulary.Dataset);

		try (QueryExecution lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql))) {
			ResultSet rs =  lqe.execSelect();
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
				Resource clazz = qs.get("clazz").asResource();

				result.add(clazz);
				
				Resource partition = outModel.createResource();
				outModel.add(subject, VOIDVocabulary.classPartition, partition);
				partition.addProperty(RDFVocabulary.type, VOIDVocabulary.Dataset);
				partition.addProperty(VOIDVocabulary.clazz, clazz);
				partition.addProperty(VOIDVocabulary.entities, qs.get("count").asLiteral());
			}
		}
		
		return result;
	}
	
    public Model buildSchema(String datasetUri, boolean ranges)  {

    	Model model = ModelFactory.createDefaultModel();
    	
    	String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

    	DatasetCatalog dcg = asCatalog(datasetUuid);
    	
    	if (dcg == null) {
    		return model;
    	}
    	
    	// !!! ASSUME ALL DATASETS ARE IN THE SAME TRIPLE STORE !!! NOT TRUE IN GENERAL
		TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		String fromClause = buildFromClause(dcg);
		
		for (Resource node : getTopClasses(vc, datasetUri, model, fromClause)) {

			ArrayList<PathElement> path = new ArrayList<>();
			path.add(PathElement.createClassPathElement(node.getURI()));

			explore(vc, path, datasetUri, model, ranges, fromClause);
		}
		
		model = Vocabulary.standarizeNamespaces(model);

		return model;
    }
    
    public Model readSchema(String datasetUri) {
    	return readSchema(datasetUri, null);
    }
    	
    public Model readSchema(String datasetUri, String voc) {
    	
    	String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);
    	
    	Optional<Dataset> dopt = datasetRepository.findByUuid(datasetUuid);
    	if (!dopt.isPresent()) {
    		return null;
    	}
    	
		TripleStoreConfiguration vc = dopt.get().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		if (vc == null) { // not published
			return null;
		}
		
		String query = null;
		
		String dctProperties = "";
		for (Property prop : Vocabulary.getProperties(DCTVocabulary.class)) {
			dctProperties += "<" + prop + "> "; 
		}

		String dcatProperties = "";
		for (Property prop : Vocabulary.getProperties(DCATVocabulary.class)) {
			if (prop != DCATVocabulary.distribution) {
				dcatProperties += "<" + prop + "> ";
			}
		}
		

		if (voc == null) {
			query =
				"CONSTRUCT { " + 
				"    <" + datasetUri + "> a ?type . " +
		        "    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . ?c1 ?c11 ?c12 . " + 
				"    <" + datasetUri + "> ?dct ?dctvalue . " + 
				"    <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 . " +
				"    ?service ?service11 ?service12 . " +
				"} WHERE  { GRAPH <" + resourceVocabulary.getContentGraphResource() + "> {" +
				"    <" + datasetUri + "> a ?type . VALUES ?type { <" + VOIDVocabulary.Dataset + "> <" + DCATVocabulary.Dataset + "> }" +
				"    OPTIONAL { <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . " + 
				"      <" + datasetUri + "> (<" + VOIDVocabulary.classPartition + ">|<" + VOIDVocabulary.propertyPartition + ">)+ ?c1 . ?c1 ?c11 ?c12 . } " +
				"    OPTIONAL { <" + datasetUri + "> ?dct ?dctvalue. VALUES ?dct { " + dctProperties + " } }  " +
				"    OPTIONAL { <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 .  " +
				"       OPTIONAL { ?distr <" + DCATVocabulary.accessService + "> ?service . ?service a <" + DCATVocabulary.DataService + "> . ?service ?service11 ?service12 . } } " +
				"} }";
		} else if (voc.equals(DCATVocabulary.PREFIX)) {
			query =
				"CONSTRUCT { " + 
				"    <" + datasetUri + "> a ?type . " +
				"    <" + datasetUri + "> ?dct ?dctvalue . " + 
				"    <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 . " +
				"    ?service ?service11 ?service12 . " +
				"} WHERE  { GRAPH <" + resourceVocabulary.getContentGraphResource() + "> {" +
				"    <" + datasetUri + "> a ?type . VALUES ?type { <" + DCATVocabulary.Dataset + "> }" +
				"    OPTIONAL { <" + datasetUri + "> ?dct ?dctvalue. VALUES ?dct { " + dctProperties + " " + dcatProperties + " } }  " +
				"    OPTIONAL { <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 .  " +
				"    OPTIONAL { ?distr <" + DCATVocabulary.accessService + "> ?service . ?service a <" + DCATVocabulary.DataService + "> . ?service ?service11 ?service12 . } } " +
				"} }";
		} else if (voc.equals(VOIDVocabulary.PREFIX)) {
			query =
				"CONSTRUCT { " + 
				"    <" + datasetUri + "> a ?type . " +
		        "    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . ?c1 ?c11 ?c12 . " + 
				"    <" + datasetUri + "> ?dct ?dctvalue . " + 
				"} WHERE  { GRAPH <" + resourceVocabulary.getContentGraphResource() + "> {" +
				"    <" + datasetUri + "> a ?type . VALUES ?type { <" + VOIDVocabulary.Dataset + "> <" + DCATVocabulary.Dataset + "> }" +
				"    OPTIONAL { <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . " + 
				"      <" + datasetUri + "> (<" + VOIDVocabulary.classPartition + ">|<" + VOIDVocabulary.propertyPartition + ">)+ ?c1 . ?c1 ?c11 ?c12 . } " +
				"    OPTIONAL { <" + datasetUri + "> ?dct ?dctvalue. VALUES ?dct { " + dctProperties + " } }  " +
				"} }";
		}
		
//		System.out.println(QueryFactory.create(query));
		if (query != null) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), query)) {
				Model model = qe.execConstruct();
				model = Vocabulary.standarizeNamespaces(model);
				return model;
			}
		}
		
		return null;
    }
    
    public class ClassStructure {
    	
    	@JsonProperty("class")
    	private Resource clazz;
    	
    	private Resource property;
    	
    	private List<Resource> range;
    	
    	private Map<Resource, ClassStructure> children;
    	
    	private int depth;
    	private int size;
    	
    	ClassStructure() {
    	}

		public Resource getClazz() {
			return clazz;
		}

		public void setClazz(Resource clazz) {
			this.clazz = clazz;
		}

		public int getDepth() {
			return depth;
		}

		public void setDepth(int depth) {
			this.depth = depth;
		}

		public Resource getProperty() {
			return property;
		}

		public void setProperty(Resource property) {
			this.property = property;
		}

		public Map<Resource, ClassStructure> getChildren() {
			return children;
		}

		public ClassStructure addChild(Resource property) {
			if (this.children == null) {
				this.children = new LinkedHashMap<>();
			}
			ClassStructure child = this.children.get(property);
			if (child == null) {
				child = new ClassStructure();
				child.setProperty(property);
				this.children.put(property, child);
			}
			
			return child;
		}
		
		public void addRange(Resource range) {
			if (this.range == null) {
				this.range = new ArrayList<>();
			}
			this.range.add(range);
		}
		
		public List<Resource> getRange() {
			return range;
		}

		public int getSize() {
			return size;
		}

		public void setSize(int size) {
			this.size = size;
		}
		
    }
    
    public List<ClassStructure> readTopClasses(Dataset dataset) {
		List<ClassStructure> res = new ArrayList<>();
	
		TripleStoreConfiguration vc = dataset.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		
		String datasetUri = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();
	
		String sparql =
				"SELECT ?clazz FROM <" + resourceVocabulary.getContentGraphResource() +  "> WHERE { " + 
				"    <" + datasetUri + "> a <" + VOIDVocabulary.Dataset + "> . " +
		        "    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + ">/<" + VOIDVocabulary.clazz + "> ?clazz . " + 
				"}";
		
		List<Resource> classes = new ArrayList<>();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), sparql)) {
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
				
				classes.add(qs.get("clazz").asResource());
			}
		}
		
		for (Resource r : classes) {
			int i = 1;
			int size = 0;
			
			ClassStructure cs = new ClassStructure();
			cs.setClazz(r);

			for (; ; i++) {
				
				String path = "";
				String select = "";
				
				for (int k = 0; k < i ; k++) {
					if (k == 0) {
						path += "?cc <" + VOIDVocabulary.propertyPartition + "> ?pp" + k + " . ";
					} else {
						path += "?pp" + (k-1) + " <" + VOIDVocabulary.propertyPartition + "> ?pp" + k + " . ";
					}
					path += "?pp" + k + " <" + VOIDVocabulary.property + "> ?property" + k + " . ";
					
					if (k == i - 1) {
						path += "OPTIONAL { ?pp" + k + " <" + DCAMVocabulary.rangeIncludes + "> ?range . } ";
					}
					
					select += "?property" + k;
				}
				
				select += " ?range";
						
				sparql =
						"SELECT " + select + " FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { " + 
						"    <" + datasetUri + "> a <" + VOIDVocabulary.Dataset + "> . " +
				        "    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . " + 
						"    ?cc <" + VOIDVocabulary.clazz + "> <" + r.toString() + "> . " +
				             path +
						"} ORDER BY " + select;
				
				
//				System.out.println(QueryFactory.create(sparql));
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), sparql)) {
					
					ResultSet rs = qe.execSelect();
					
					if (!rs.hasNext()) {
						break;
					}
					
					while (rs.hasNext()) {
						QuerySolution qs = rs.next();
						
						ClassStructure current = cs;
						
						String prevPath = null;
						
						String newPath = "";
						for (int k = 0; k < i; k++) {
							newPath = newPath + " " + qs.get("property" + k).asResource();
						}
						
						if (prevPath == null || !newPath.equals(prevPath)) {
							for (int k = 0; k < i; k++) {
								Resource prop = qs.get("property" + k).asResource();
							
								current = current.addChild(prop);
								size++;
							}
							prevPath = newPath;
						}
						
						RDFNode rRange = qs.get("range");
						if (rRange != null) {
							current.addRange(rRange.asResource());
						}
					}
				}
			}
			
			cs.setDepth(i - 1);
			cs.setSize(size);
			
//			res.add(ClassStructureResponse.createFrom(cs));
			res.add(cs);
		}
		
		return res;
	}    
	  
	  public ClassStructureResponse readTopClass(Dataset dataset, String uri) {
		TripleStoreConfiguration vc = dataset.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		
		String datasetUri = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();
	
		ClassStructure cs = new ClassStructure();
		Resource r = new ResourceImpl(uri);
		
		cs.setClazz(r);

		int i = 1;
		for (; ; i++) {
			
			String path = "";
			String select = "";
			
			for (int k = 0; k < i ; k++) {
				if (k == 0) {
					path += "?cc <" + VOIDVocabulary.propertyPartition + "> ?pp" + k + " . ";
				} else {
					path += "?pp" + (k-1) + " <" + VOIDVocabulary.propertyPartition + "> ?pp" + k + " . ";
				}
				path += "?pp" + k + " <" + VOIDVocabulary.property + "> ?property" + k + " . ";
				select += "?property" + k + " ";
			}
					
			String sparql =
					"SELECT " + select + " FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { " + 
					"    <" + datasetUri + "> a <" + VOIDVocabulary.Dataset + "> . " +
			        "    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . " + 
					"    ?cc <" + VOIDVocabulary.clazz + "> <" + r.toString() + "> . " +
			             path +
					"} ORDER BY " + select;
			
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), sparql)) {
				
				ResultSet rs = qe.execSelect();
				
				if (!rs.hasNext()) {
					break;
				}
				
				while (rs.hasNext()) {
					QuerySolution qs = rs.next();
					ClassStructure current = cs;
					for (int k = 0; k < i; k++) {
						current = current.addChild(qs.get("property" + k).asResource());
					}
				}
			}
		}
		
		cs.setDepth(i - 1);
		
		return ClassStructureResponse.createFrom(cs);
	}    	  
}
