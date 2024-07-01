package ac.software.semantic.service;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdApi;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.config.AppConfiguration.JenaRDF2JSONLD;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.RemoteTripleStore;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.payload.ByteData;
import ac.software.semantic.payload.PropertyDoubleValue;
import ac.software.semantic.payload.PropertyValue;
import ac.software.semantic.payload.ValueAnnotationReference;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.payload.response.ClassStructureResponse;
import ac.software.semantic.payload.response.ValueResponse;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.DistributionService.DistributionContainer;
import ac.software.semantic.service.container.ObjectContainer;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.lod.vocabularies.DCAMVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCATVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
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
    
	@Autowired
	@Qualifier("dataset-schema-jsonld-context")
    private Map<String, Object> datasetSchemaContext;

	@Autowired
	@Qualifier("dataset-metadata-jsonld-context")
    private Map<String, Object> datasetSchemaMetadataContext;

	@Autowired
	private APIUtils apiUtils;

	@Autowired
	private IdentifiersService identifierService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Lazy
	@Autowired
	private DatasetService datasetService;

    public DatasetCatalog asCatalog(Dataset dataset) {

    	DatasetCatalog dc = new DatasetCatalog(dataset);
    	
    	if (dataset.getDatasets() != null) {
	    	for (ObjectId cid : dataset.getDatasets()) {
	    		Optional<Dataset> dssOpt = datasetRepository.findById(cid);
	    		if (dssOpt.isPresent()) {
	    			dc.addMember(dssOpt.get());
	    		}
	    	}
    	}
    	
    	return dc;
    }
    
    public DatasetCatalog asCatalog(String uuid) {
    	Optional<Dataset> dsOpt = datasetRepository.findByUuid(uuid);
    	
    	if (!dsOpt.isPresent()) {
    		return null;
    	}
    	
    	return asCatalog(dsOpt.get());
    }
    
    public DatasetCatalog asCatalog(ObjectId id) {
    	Optional<Dataset> dsOpt = datasetRepository.findById(id);
    	
    	if (!dsOpt.isPresent()) {
    		return null;
    	}

    	return asCatalog(dsOpt.get());
    }
    
	public String buildFromClause(DatasetCatalog dcg) {
		return buildFromClause(dcg, false);
	}
	
	public String buildFromClause(DatasetCatalog dcg, boolean embeddings) {
		Dataset dataset = dcg.getDataset();
		
		if (dataset.isLocal()) {
			String from = "";
			for (int i = 0; i <= dataset.getMaxGroup(); i++) {
				from += "FROM <" + dataset.getContentTripleStoreGraph(resourceVocabulary, i) + "> ";
			}
			if (embeddings) {
				from += "FROM <" + resourceVocabulary.getDatasetEmbeddingsAsResource(dataset.getUuid()) + "> ";
			}
			for (Dataset member : dcg.getMembers()) {
				for (int i = 0; i <= member.getMaxGroup(); i++) {
					from += "FROM <" + member.getContentTripleStoreGraph(resourceVocabulary, i) + "> ";
				}
				if (embeddings) {
					from += "FROM <" + resourceVocabulary.getDatasetEmbeddingsAsResource(member.getUuid()) + "> ";
				}
			}
		
			return from;
		} else {
			return RemoteTripleStore.buildFromClause(dataset.getRemoteTripleStore());
		}
	}
	
	public String buildUsingClause(DatasetCatalog dcg) {
		Dataset dataset = dcg.getDataset();
		
		String from = "";
		for (int i = 0; i <= dataset.getMaxGroup(); i++) {
			from += "USING <" + dataset.getContentTripleStoreGraph(resourceVocabulary, i) + "> ";
		}

		for (Dataset member : dcg.getMembers()) {
			for (int i = 0; i <= member.getMaxGroup(); i++) {
				from += "USING <" + member.getContentTripleStoreGraph(resourceVocabulary, i) + "> ";
			}			
		}
		
		return from;
	}
	
	private static void explore(String endpoint, List<PathElement> path, String datasetUri, Model outModel, boolean ranges, String fromClause) {
		
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

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, query)) {
				
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
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
		if (ranges) {
			// Get properties ranges 
			String sparql = 
			    	"SELECT ?prop ?datatype " +
			        fromClause +
			        "WHERE { " +
			    	     spath +
	 				"    FILTER (isLiteral(?z)) . BIND (datatype(?z) as ?datatype) " +
	                " } " +
			 		"GROUP BY ?prop ?datatype";
			
			Query query = QueryFactory.create(sparql);
			System.out.println(query);

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, query)) {
				
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
					
					Resource datatype = sol.getResource("datatype");
					if (datatype != null) {
						list.add(datatype);
					} else {
//						list.add(RDFSVocabulary.Literal);
						list.add(RDFVocabulary.langString); // virtuoso returns null instead of langstring
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
		// Get properties 
		String sparql = 
		    	"SELECT ?prop (count(?z) AS ?triples) (count(DISTINCT ?z) AS ?distinctObjects) (count(?b) AS ?countb) (count(DISTINCT ?c0) AS ?distinctSubjects) " +
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
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, query)) {
			
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
				partition.addProperty(VOIDVocabulary.triples, sol.get("triples"));
				partition.addProperty(VOIDVocabulary.distinctObjects, sol.get("distinctObjects"));
				partition.addProperty(VOIDVocabulary.distinctSubjects, sol.get("distinctSubjects"));
				
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
					explore(endpoint, newPath, datasetUri, outModel, ranges, fromClause);
				}
				
			}
		}
	}

	private static List<Resource> getTopClasses(String endpoint, String datasetUri, Model outModel, String fromClause) {

		String lsparql =
				"SELECT  ?clazz (count(?x) as ?count)" +
				fromClause + 
		        "WHERE { " +
	            "    ?x a ?clazz . FILTER isIRI(?x)  " +
		    	"}  " +
		    	"GROUP BY ?clazz ";

		System.out.println("Getting top classes");
		System.out.println(endpoint);
		System.out.println(QueryFactory.create(lsparql));
		List<Resource> result = new ArrayList<>();

		Resource subject = outModel.createResource(datasetUri);
		outModel.add(subject, RDFVocabulary.type, VOIDVocabulary.Dataset);

		try (QueryExecution lqe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(lsparql))) {
			ResultSet rs =  lqe.execSelect();
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
				Resource clazz = qs.get("clazz").asResource();
				Literal count = qs.get("count").asLiteral();

				result.add(clazz);
				
				Resource partition = outModel.createResource();
				outModel.add(subject, VOIDVocabulary.classPartition, partition);
				partition.addProperty(RDFVocabulary.type, VOIDVocabulary.Dataset);
				partition.addProperty(VOIDVocabulary.clazz, clazz);
				partition.addProperty(VOIDVocabulary.entities, count);
				
				String prefix = discoverPrefix(endpoint, fromClause, clazz, count.getInt());
				if (prefix != null) {
					partition.addProperty(VOIDVocabulary.uriSpace, prefix);
				}
				
			}
		}
		
		return result;
	}
	
	private static String discoverPrefix(String endpoint, String fromClause, Resource clazz, int count) {
	   		
		String sparql =
				"SELECT ?item " +
				fromClause + 
		        "WHERE { " +
	            "    ?item a <" + clazz + "> . FILTER isIRI(?item)  " +
		    	"}  ";
		    	
   		List<String> uris = new ArrayList<>();       		
		
   		// get first 50
       	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, sparql + " LIMIT 50")) {
       		
       		ResultSet rs = qe.execSelect();
       		while (rs.hasNext()) {
       			QuerySolution qs = rs.next();
       			uris.add(qs.get("item").toString());
            }
       		
        }

   		// get last 50
       	if (count > 50) {
	       	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, sparql + " LIMIT 50 OFFSET " + (count - 50))) {
	       		
	       		ResultSet rs = qe.execSelect();
	       		while (rs.hasNext()) {
	       			QuerySolution qs = rs.next();
	       			uris.add(qs.get("item").toString());
	            }
	       		
	        }
       	}
       	
   		// get mid 50       	
       	if (count > 150) {
	       	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, sparql + " LIMIT 50 OFFSET " + (count/2 - 25))) {
	       		
	       		ResultSet rs = qe.execSelect();
	       		while (rs.hasNext()) {
	       			QuerySolution qs = rs.next();
	       			uris.add(qs.get("item").toString());
	            }
	       		
	        }
       	}
       	
       	// get some random 
       	if (count > 200) {
	       	for (int i = 0; i < 50; i++) {
	       		int v = (int)(Math.random()*(count-100)) + 50;
	       		try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, sparql + " LIMIT 1 OFFSET " + v)) {
		       		
		       		ResultSet rs = qe.execSelect();
		       		while (rs.hasNext()) {
		       			QuerySolution qs = rs.next();
		       			uris.add(qs.get("item").toString());
		            }
		       		
		        }
	       	}
       	}

   		String prefix = StringUtils.getCommonPrefix(uris.toArray(new String[] {}));
   		
   		while (prefix.length() > 0 && prefix.charAt(prefix.length() - 1) != '/' && prefix.charAt(prefix.length() - 1) != '#') {
   			prefix = prefix.substring(0, prefix.length() - 1);
   		}
   		
   		int prefixLength = prefix.length();

   		if (prefixLength == 0) {
   			return null;
   		}

   		for (String uri : uris) {
   			String local = uri.substring(prefixLength);
   			if (local.contains("#") || local.contains("/")) {
   				return null;
   			}
   		}
   		
   		return prefix;
	}
	
    public Model buildSchema(Dataset dataset, boolean ranges)  {

    	Model model = ModelFactory.createDefaultModel();
    	
//    	String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

    	DatasetCatalog dcg = asCatalog(dataset);
    	
    	if (dcg == null) {
    		return model;
    	}
    	
//    	Dataset dataset = dcg.getDataset();
    	
    	String datasetUri = resourceVocabulary.getDatasetContentAsResource(dataset).toString(); 
    	
    	String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());
    	String fromClause = buildFromClause(dcg);
		
		for (Resource node : getTopClasses(endpoint, datasetUri, model, fromClause)) {

			ArrayList<PathElement> path = new ArrayList<>();
			path.add(PathElement.createClassPathElement(node.getURI()));

			explore(endpoint, path, datasetUri, model, ranges, fromClause);
		}
		
		model = Vocabulary.standarizeNamespaces(model);

		return model;
    }
    
    public void serializeSchema(Writer sw, Model model, RDFFormat format) throws Exception {

	    if (format == RDFFormat.JSONLD) {
	    	Map<String, Object> jn = apiUtils.jsonLDFrame(model, (Map)datasetSchemaContext.get("dataset-schema-jsonld-context"));

			ByteArrayOutputStream jsonbos = new ByteArrayOutputStream();
			ObjectMapper mapper = new ObjectMapper();
			JsonFactory jsonFactory = new JsonFactory();				
			JsonGenerator jsonGenerator = jsonFactory.createGenerator(jsonbos, JsonEncoding.UTF8);
				
	        mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, jn);
	        sw.append(new String(jsonbos.toByteArray()));
						    
	    } else {
			RDFDataMgr.write(sw, model, format) ;
	    }
    }
    
    public Object jsonSerializeSchema(Model model) throws Exception {

    	Map<String, Object> jn = apiUtils.jsonLDFrame(model, (Map)datasetSchemaContext.get("dataset-schema-jsonld-context"));

    	return jn.get("@graph");
    }
    
    public Object jsonSerializeMetadata(Model model) throws Exception {

    	Map<String, Object> jn = apiUtils.jsonLDFrame(model, (Map)datasetSchemaMetadataContext.get("dataset-metadata-jsonld-context"));

    	if (jn.get("@graph") instanceof List) {
    		return ((List)jn.get("@graph")).get(0);
    	} else {
    		return jn.get("@graph");
    	}
    }
    
    public Model readSchema(Dataset dataset) {
    	return readSchema(dataset, null);
    }
    	
    public Model readSchema(Dataset dataset, String voc) {
    	
		TripleStoreConfiguration vc = dataset.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		if (vc == null) { // not published
			return null;
		}
		
		String query = null;
		
		String dctProperties = "";
		for (Property prop : Vocabulary.getProperties(DCTVocabulary.class)) {
			if (!prop.equals(DCTVocabulary.hasPart)) { // ignore it because it causes all datasets of a catalog to be included. a bad fix...
				dctProperties += "<" + prop + "> ";
			}
		}

		String dcatProperties = "";
		for (Property prop : Vocabulary.getProperties(DCATVocabulary.class)) {
			if (prop != DCATVocabulary.distribution) {
				dcatProperties += "<" + prop + "> ";
			}
		}
		
    	Resource datasetUri = resourceVocabulary.getDatasetContentAsResource(dataset);

		if (voc == null) {
			query =
				"CONSTRUCT { " + 
				"    <" + datasetUri + "> a ?type . " +
		        "    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . ?c1 ?c11 ?c12 . " + 
				"    <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 . " +
				"    ?service ?service11 ?service12 . " +
				"} WHERE  { GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> {" +
				"    <" + datasetUri + "> a ?type . VALUES ?type { <" + VOIDVocabulary.Dataset + "> <" + DCATVocabulary.Dataset + "> }" +
				"    OPTIONAL { <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . " + 
				"      <" + datasetUri + "> (<" + VOIDVocabulary.classPartition + ">|<" + VOIDVocabulary.propertyPartition + ">)+ ?c1 . ?c1 ?c11 ?c12 . } " +
				"    OPTIONAL { <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 .  " +
				"    OPTIONAL { ?distr <" + DCATVocabulary.accessService + "> ?service . ?service a <" + DCATVocabulary.DataService + "> . ?service ?service11 ?service12 . } } " +
				"} }";
		} else if (voc.equals(DCATVocabulary.PREFIX)) {
			query =
				"CONSTRUCT { " + 
				"    <" + datasetUri + "> a ?type . " +
				"    <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 . " +
				"    ?service ?service11 ?service12 . " +
				"} WHERE  { GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> {" +
				"    <" + datasetUri + "> a ?type . VALUES ?type { <" + DCATVocabulary.Dataset + "> }" +
				"    OPTIONAL { <" + datasetUri + "> <" + DCATVocabulary.distribution + "> ?distr . ?distr ?distr11 ?distr12 .  " +
				"    OPTIONAL { ?distr <" + DCATVocabulary.accessService + "> ?service . ?service a <" + DCATVocabulary.DataService + "> . ?service ?service11 ?service12 . } } " +
				"} }";
		} else if (voc.equals(VOIDVocabulary.PREFIX)) {
			query =
				"CONSTRUCT { " + 
				"    <" + datasetUri + "> a ?type . " +
		        "    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . ?c1 ?c11 ?c12 . " + 
				"} WHERE  { GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> {" +
				"    <" + datasetUri + "> a ?type . VALUES ?type { <" + VOIDVocabulary.Dataset + "> <" + DCATVocabulary.Dataset + "> }" +
				"    OPTIONAL { <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . " + 
				"      <" + datasetUri + "> (<" + VOIDVocabulary.classPartition + ">|<" + VOIDVocabulary.propertyPartition + ">)+ ?c1 . ?c1 ?c11 ?c12 . } " +
				"} }";
		}
		
		Model model = null;
		
//		System.out.println(QueryFactory.create(query, Syntax.syntaxSPARQL_11));		
		
		if (query != null) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(query, Syntax.syntaxSPARQL_11))) {
				model = qe.execConstruct();
			}
		}
		
		// get properties separaterly to make virtuoso respond!
		String propQuery =
				"CONSTRUCT { " + 
				"    <" + datasetUri + "> ?dct ?dctvalue . " + " ?dctvalue ?dctp1 ?dctp2 . " + 
				"} WHERE  { GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " +
				"    <" + datasetUri + "> ?dct ?dctvalue . VALUES ?dct { " + dctProperties + " } . OPTIONAL { ?dctvalue ?dctp1 ?dctp2 }   " +
                "} } " ;
		
		if (propQuery != null) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(propQuery, Syntax.syntaxSPARQL_11))) {
				model.add(qe.execConstruct());
			}
		}
    	
		if (model != null) {
			addEndpoints(model, voc, dataset);
			addDistributions(model, voc, dataset);
			
			model = Vocabulary.standarizeNamespaces(model);
		}
    	
		return model;
    }
    
    private void addEndpoints(Model model, String voc, Dataset dataset) {
    	Resource datasetUri = resourceVocabulary.getDatasetContentAsResource(dataset);
    	
		if (voc == null || voc.equals(DCATVocabulary.PREFIX)) {
			UpdateRequest ur = UpdateFactory.create();
			String insert;
			
//			String insert = 
//					"insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " + 
////   						"<" + datasetUri + ">  <" + DCTVocabulary.identifier + "> \"" + id + "\" . " + 
//			        "<" + datasetUri + ">  <" + VOIDVocabulary.sparqlEndpoint + "> <" + resourceVocabulary.getContentSparqlEnpoint(dataset.getUuid()) + "> } } WHERE { }" ;
    				
			
			Resource distributionResource = resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql");
			Resource dataServiceResource = resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql");
			
			// DCAT	    				
			
			Resource endpoint = resourceVocabulary.getContentSparqlEnpoint(identifierService.getPreferedIdentifier(dataset));
			
			insert = 
					"insert { " + 
					" <" + datasetUri + ">  <" + DCATVocabulary.distribution + "> <" + resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql") + "> . " +
                    " <" + distributionResource + "> a <" + DCATVocabulary.Distribution + "> . " + 	    								
                    " <" + distributionResource + "> <" + DCATVocabulary.accessService + "> <" + resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql") + "> ." +
                    " <" + distributionResource + "> <" + DCTVocabulary.conformsTo + "> <https://www.w3.org/TR/sparql11-protocol/> . " +
                    " <" + distributionResource + "> <" + DCATVocabulary.accessURL + "> <" + endpoint + "> ." +
                    " <" + distributionResource + "> <" + DCTVocabulary.title + "> \"SPARQL endpoint\" ." +
                    " <" + dataServiceResource + "> a <" + DCATVocabulary.DataService + "> . " +
                    " <" + dataServiceResource + "> <" + DCTVocabulary.conformsTo + "> <https://www.w3.org/TR/sparql11-protocol/> . " +
                    " <" + dataServiceResource + "> <" + DCATVocabulary.endpointURL + "> <" + endpoint + "> ." +
                    " <" + dataServiceResource + "> <" + DCATVocabulary.servesDataset + "> <" + datasetUri + "> ." +
                    " }  WHERE { } ";

			ur.add(insert);
			
			UpdateAction.execute(ur, model);
		}
    }
    
    private void addDistributions(Model model, String voc, Dataset dataset) {
    	
    	if (voc != null && voc.equals(DCATVocabulary.PREFIX)) {
	    	DatasetContainer dc = datasetService.getContainer(null, dataset);
	
			String datasetUri = resourceVocabulary.getDatasetContentAsResource(dataset).toString();
	
			UpdateRequest ur = UpdateFactory.create();
			
	    	for (ObjectContainer ocs : dc.getActiveInnerContainers(DistributionContainer.class)) {
	    		DistributionContainer distrCont = (DistributionContainer)ocs; 
	    	
	    		DistributionDocument distr = distrCont.getObject();
	    	
	    		CreateState cds = distrCont.checkCreateState();
			
				if (cds != null) {
					for (SerializationType st : distr.getSerializations()) {
						String format = st.toString().toLowerCase();
						
						String license = distr.getLicense();
						if (license != null) {
							if (license.length() == 0) {
								license = null;
							} else if (license.startsWith("http://") || license.startsWith("https://")) {
								license = "<" + license + ">";
							} else {
								license = "\"" + license + "\"";
							}
						}
						
						Resource distributionResource = resourceVocabulary.getDistributionAsResource(distr.getUuid() + "/" + format);
						
						String insert = 
							"insert { " + 
							" <" + datasetUri + ">  <" + DCATVocabulary.distribution + "> <" + distributionResource + "> . " +
			                " <" + distributionResource + "> a <" + DCATVocabulary.Distribution + "> . " + 	    								
			                (license != null ? (" <" + distributionResource + "> <" + DCTVocabulary.license + "> " + license + " . ") : "") +
			                (SerializationType.toFormats(st, distr.getSerializationVocabulary()) != null ? (" <" + distributionResource + "> <" + DCTVocabulary.format + "> <" + SerializationType.toFormats(st, distr.getSerializationVocabulary()) + "> . ") : "") +
			                (SerializationType.toMediaType(st) != null ? (" <" + distributionResource + "> <" + DCATVocabulary.mediaType + "> \"" + SerializationType.toMediaType(st) + "\" . ") : "") +
			                " <" + distributionResource + "> <" + DCATVocabulary.accessURL + "> <" + resourceVocabulary.getContentDistribution(identifierService.getPreferedIdentifier(dataset), distr.getIdentifier(), st) + "> . " +
			                " <" + distributionResource + "> <" + DCATVocabulary.downloadURL + "> <" + resourceVocabulary.getContentDistribution(identifierService.getPreferedIdentifier(dataset), distr.getIdentifier(), st) + "> ." +
			                " <" + distributionResource + "> <" + DCTVocabulary.title + "> \"" + distr.getName() + " (" + st.toString() + ")\" ." +
			                "} WHERE { } ";
			
						ur.add(insert);
					}
		    	}
	    	}
    	
	    	UpdateAction.execute(ur, model);
    	}
    }
    
    public ValueResponseContainer<ValueResponse> getPropertyValues(Dataset dataset, List<PathElement> path, String mode, Integer page, int size) {

    	DatasetCatalog dcg = asCatalog(dataset);

    	String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());
    	String fromClause = buildFromClause(dcg);
    	
		String spath = "";
		String endVar = "";

		int pathLength = 0;
		for (int i = 0; i < path.size(); i++) {

			PathElement pe = path.get(i);
			if (pe.isClass()) {
				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
				endVar = "c" + (pathLength);
			} else {
				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
				endVar = "c" + (pathLength + 1);
				pathLength++;
			}
		}

		spath += mode.equals("ALL") ? "" : (mode.equals("IRI") ? "FILTER (isIRI(?" + endVar + "))" : "FILTER (isLiteral(?" + endVar + "))");  
		
		String valuesSparql = 
				"SELECT ?" + endVar + "  ?count " + 
		        fromClause + 
		        "WHERE { " +
				"  SELECT DISTINCT ?" + endVar + " (COUNT(?" + endVar + ") AS ?count) " +
		        "  WHERE {" + spath + "} " +
		        "  GROUP BY ?" + endVar + " " +
		        "  ORDER BY DESC(?count) ?" + endVar + 
		           (page != null ? "  LIMIT " + size + " OFFSET " + (page-1)*size : "") +
		        "} ";
		
		String countSparql = 
				"SELECT (COUNT(?" + endVar + ") AS ?count) (COUNT(DISTINCT ?" + endVar + ") AS ?distinctCount) " +
		        fromClause + 
		        "WHERE { " + spath + "} ";
		
		ValueResponseContainer<ValueResponse> vrc = new ValueResponseContainer<>();
		
//		System.out.println(QueryFactory.create(sparql));
//		System.out.println(QueryFactory.create(sparql));

		List<ValueResponse> res = new ArrayList<>();

		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(valuesSparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				
				res.add(new ValueResponse(RDFTerm.createRDFTerm(sol.get(endVar)), sol.get("count").asLiteral().getInt()));
			}
		}

		vrc.setValues(res);

		int totalCount = 0;
		int distinctTotalCount = 0;

		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(countSparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				
				totalCount = sol.get("count").asLiteral().getInt();
				distinctTotalCount = sol.get("distinctCount").asLiteral().getInt();
			}
		}

		vrc.setTotalCount(totalCount);
		vrc.setDistinctTotalCount(distinctTotalCount);
		
		return vrc;
		
    }
    
	public ValueResponseContainer<ValueResponse> getItemsForPropertyValue(Dataset dataset, PropertyValue pv, Integer page, int size) {
    	
		String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());
		String fromClause = buildFromClause(asCatalog(dataset));
		
		String path = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(pv.getPath()));
		
		String valuesSparql = 
				"SELECT DISTINCT ?s " + 
			    fromClause + 
				"WHERE { ?s " + path + " " + pv.getValue().toRDFString() + "} " +
		        (page != null ? "  LIMIT " + size + " OFFSET " + (page-1)*size : "");

		String countSparql = 
				"SELECT (COUNT(?s) AS ?count) (COUNT(DISTINCT ?s) AS ?distinctCount) " + 
			    fromClause + 
				"WHERE { ?s " + path + " " + pv.getValue().toRDFString() + "} ";
		        
//		System.out.println(QueryFactory.create(valuesSparql));
//		System.out.println(QueryFactory.create(countSparql));
		
		ValueResponseContainer<ValueResponse> vrc = new ValueResponseContainer<>();
		
		List<ValueResponse> res = new ArrayList<>();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(valuesSparql, Syntax.syntaxSPARQL_11))) {
			ResultSet rs = qe.execSelect();
				
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				
				res.add(new ValueResponse(new RDFTerm(sol.get("s").asResource()), 1));
			}
		}
		
		vrc.setValues(res);
		
		int totalCount = 0;
		int distinctTotalCount = 0;

		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(countSparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				
				totalCount = sol.get("count").asLiteral().getInt();
				distinctTotalCount = sol.get("distinctCount").asLiteral().getInt();
			}
		}
		
		vrc.setTotalCount(totalCount);
		vrc.setDistinctTotalCount(distinctTotalCount);
		
		return vrc;
	}
	
	public ValueResponseContainer<ValueResponse> getItemsForPropertyValue(Dataset dataset, PropertyValue pv, List<List<PathElement>> extra, Integer page, int size) {
    	
		String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());
		String fromClause = buildFromClause(asCatalog(dataset));
		
		return getItemsForPropertyValue(endpoint, fromClause, pv, extra, page, size);
	}

	public ValueResponseContainer<ValueResponse> getItemsForPropertyValue(String endpoint, String fromClause, PropertyValue pv, List<List<PathElement>> extra, Integer page, int size) {
		
		String path = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(pv.getPath()));

		String cExtra = "";
		String wExtra = "";
		
		for (int i = 0; i < extra.size(); i++) {
			List<PathElement> extraPath = extra.get(i);
			String pew = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(extraPath), "?VAR_ZZZ_" + i + "_");
			wExtra += " OPTIONAL { ?s " + pew + "?VAR_ZZZ_" + i + " } ";
			
			List<PathElement> extraResultPath = new ArrayList<>();
			extraResultPath.add(extraPath.get(extraPath.size() - 1));
			String pec = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(extraPath), "?VAR_ZZZ_" + i + "_");
			
			cExtra += " ?s " + pec + "?VAR_ZZZ_" + i ;
		}
		
		String valuesSparql = 
				"CONSTRUCT { " +
					cExtra + 
		        "} " +
			    fromClause + 
				"WHERE { ?s " + path + " " + pv.getValue().toRDFString() +
					wExtra +
				"} " +
		        (page != null ? "  LIMIT " + size + " OFFSET " + (page-1)*size : "");

		String countSparql = 
				"SELECT (COUNT(?s) AS ?count) (COUNT(DISTINCT ?s) AS ?distinctCount) " + 
			    fromClause + 
				"WHERE { ?s " + path + " " + pv.getValue().toRDFString() + "} ";
		        
//		System.out.println(valuesSparql);
//		System.out.println(QueryFactory.create(valuesSparql));
//		System.out.println(QueryFactory.create(countSparql));
		
		ValueResponseContainer<ValueResponse> vrc = new ValueResponseContainer<>();
		
//		List<ValueResponse> res = new ArrayList<>();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(valuesSparql, Syntax.syntaxSPARQL_11))) {
			Model model = qe.execConstruct();
			
	    	Map<String, Object> jn = apiUtils.jsonLDFrame(model, new HashMap<>());
	    	
	    	vrc.setExtra(jn);
		}
		
//		vrc.setValues(res);
		
		int totalCount = 0;
		int distinctTotalCount = 0;

		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(countSparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				
				totalCount = sol.get("count").asLiteral().getInt();
				distinctTotalCount = sol.get("distinctCount").asLiteral().getInt();
			}
		}
		
		vrc.setTotalCount(totalCount);
		vrc.setDistinctTotalCount(distinctTotalCount);
		
		return vrc;
	}

	public List<String> getUriSpaces(Dataset dataset) {
		String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());
		
		String newSparql = "SELECT DISTINCT ?uriSpace FROM <"+ dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " + 
		"  <" + resourceVocabulary.getDatasetContentAsResource(dataset) + "> <" + VOIDVocabulary.classPartition + ">/<" + VOIDVocabulary.uriSpace + "> ?uriSpace . }";

		List<String> res = new ArrayList<>();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(newSparql, Syntax.syntaxARQ))) {
			ResultSet rs = qe.execSelect();

			while (rs.hasNext()) {
				QuerySolution qs = rs.next();

				res.add(qs.get("uriSpace").asLiteral().toString());
			}
		}
		
		return res;
	}
	
	public List<ValueAnnotationReference> getPropertyValuesForItem(Dataset dataset, PropertyDoubleValue pdv) {

		String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());
		String fromClause = buildFromClause(asCatalog(dataset));
    	
		String spath = "";
		String endVar = "";

		List<PathElement> path = pdv.getPath();
		int pathLength = 0;
		
		for (int i = 0; i < path.size(); i++) {

			PathElement pe = path.get(i);
			if (pe.isClass()) {
				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
				endVar = "c" + (pathLength);
			} else {
				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
				endVar = "c" + (pathLength + 1);
				pathLength++;
			}
		}
		
		spath = spath.replaceAll("\\?" + endVar, pdv.getValue().toString());


		String sparql = 
				"SELECT ?prop (count(?prop) AS ?count)  " +
		        fromClause + 
		        "WHERE { " +
  			        spath + 
			       "?c" + (pathLength - 1) + " ?prop <" + pdv.getTarget() + "> . " +
				" } " +
		        "GROUP BY ?prop ";
		
		List<ValueAnnotationReference> res = new ArrayList<>();

//		System.out.println(sparql);
//		System.out.println(QueryFactory.create(sparql));

		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(sparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				
				res.add(new ValueAnnotationReference(sol.get("prop").toString(), sol.get("count").asLiteral().getInt()));
			}
		}

		return res;
	}
    
	public ByteData downloadPropertyValues(Dataset dataset, List<PathElement> path, String mode) throws Exception {

		String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());    	
    	String fromClause = buildFromClause(asCatalog(dataset));

		String spath = "";
		String endVar = "";

		int pathLength = 0;

		for (int i = 0; i < path.size(); i++) {
			PathElement pe = path.get(i);
			if (pe.isClass()) {
				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
			} else {
				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
				endVar = "c" + (pathLength + 1);
				pathLength++;
			}
		}

		String filter = mode.equals("ALL") ? "" : (mode.equals("IRI") ? "FILTER (isIRI(?" + endVar + "))" : "FILTER (isLiteral(?" + endVar + "))");
		String sparql = "SELECT ?c0 ?" + endVar + " " + fromClause + " WHERE { " + spath + filter + "  } ";

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
				Writer writer = new BufferedWriter(new OutputStreamWriter(bos));
				CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Item", "IRI", "LiteralLexicalForm", "LiteralLanguage", "LiteralDatatype"));
				ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			
			try (VirtuosoSelectIterator vs = new VirtuosoSelectIterator(endpoint, sparql)) {
				while (vs.hasNext()) {
					QuerySolution sol = vs.next();
					Resource id = sol.get("c0").asResource();
					RDFNode node = sol.get(endVar);

					List<Object> line = new ArrayList<>();
					line.add(id);
					
					if (node.isResource()) {
						line.add(node.asResource());
						line.add(null);
						line.add(null);
						line.add(null);
					} else if (node.isLiteral()) {
						line.add(null);
						line.add(Utils.escapeJsonTab(node.asLiteral().getLexicalForm()));
						line.add(node.asLiteral().getLanguage());
						line.add(node.asLiteral().getDatatype().getURI());
					}
					csvPrinter.printRecord(line);
				}
				
			}

			csvPrinter.flush();
			bos.flush();

			String fileName = dataset.getUuid() + "_" + apiUtils.generateNameFromUrl(path.get(path.size() -1).getUri());

			try (ZipOutputStream zos = new ZipOutputStream(baos)) {
				ZipEntry entry = new ZipEntry(fileName + ".csv");

				zos.putNextEntry(entry);
				zos.write(bos.toByteArray());
				zos.closeEntry();

			}

			return new ByteData(baos.toByteArray(), fileName + ".zip");
		}
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
		
		String datasetUri = resourceVocabulary.getDatasetContentAsResource(dataset).toString();
	
		String sparql =
				"SELECT ?clazz FROM <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) +  "> WHERE { " + 
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
						"SELECT " + select + " FROM <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> WHERE { " + 
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
	  
	  public ClassStructureResponse readTopClass(Dataset dataset, String uri, boolean includeDatatypes) {
		TripleStoreConfiguration vc = dataset.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		
		String datasetUri = resourceVocabulary.getDatasetContentAsResource(dataset).toString();
	
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
					"SELECT " + select + " FROM <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> WHERE { " + 
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
		
		return ClassStructureResponse.createFrom(cs, includeDatatypes);
	}
	  
    public Model readMetadata(Dataset dataset) {
    	
		TripleStoreConfiguration vc = dataset.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		if (vc == null) { // not published
			return null;
		}
		
    	Resource datasetUri = resourceVocabulary.getDatasetContentAsResource(dataset);

		String query =  
				"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
				+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
				+ " ?url <http://purl.org/dc/terms/creator> ?creator . "
				+ " ?url <http://purl.org/dc/terms/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/terms/language> ?language . "
				+ " ?url <" + SEMAVocabulary.scheme + "> ?scheme } " + "WHERE { " + " GRAPH <"
				+ dataset.getMetadataTripleStoreGraph(resourceVocabulary)+ "> { ?url a ?type . VALUES ?url { <" + datasetUri + "> } " 
				+ " OPTIONAL { ?url <" + RDFSVocabulary.label + ">|<" + DCTVocabulary.title + "> ?label } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.language + "> ?language } ."
				+ " OPTIONAL { ?url <" + SEMAVocabulary.scheme + "> ?scheme } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } . "
				+ " OPTIONAL { ?url <" + DCTVocabulary.hasPart + "> ?part }   . "
				+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
				+ "  ?alurl <" + SEMAVocabulary.target + ">/<" + RDFSVocabulary.label + "> ?targetlabel  } }"
				+ "} ";
					

//    	System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

//		Writer sw = new StringWriter();
		
		Model model = ModelFactory.createDefaultModel();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(query, Syntax.syntaxARQ))) {
			model.add(qe.execConstruct());
		}
		
		return model;

	}    	

	  
}
