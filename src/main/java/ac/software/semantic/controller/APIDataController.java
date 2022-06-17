package ac.software.semantic.controller;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import io.swagger.v3.oas.annotations.Parameter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ac.software.semantic.model.Access;
import ac.software.semantic.model.AccessType;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditType;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.UserType;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueResponse;
import ac.software.semantic.repository.AccessRepository;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.SchemaService;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SOAVocabulary;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Data API")

@RestController
@RequestMapping("/api/data")
public class APIDataController {

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	AnnotationEditGroupRepository aegRepository;

	@Autowired
	PagedAnnotationValidationRepository pavRepository;

	@Autowired
	AccessRepository accessRepository;
	
	@Autowired
	SchemaService schemaService;
	
    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;
    
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

    
//  Moved to SchemaService    
//	private void explore(VirtuosoConfiguration vc, List<PathElement> path, String datasetUri, ArrayNode array, List<PagedAnnotationValidation> pavList, Model outModel) throws JsonParseException, JsonMappingException, IOException {
//		
////		System.out.println("EXPLORING " + path.size());
//		String spath = "";
//		String endVar = "";
//		
//		String modelPath = "BIND(<" + datasetUri + "> AS ?c0) . ";
//		
//		int pathLength = 0;
//		int modelLength = 1;
//		for (int i = 0; i < path.size(); i++) {
//			PathElement pe = path.get(i);
//			if (pe.getType().equals("class")) {
//				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
//				
//				modelPath += "?c" + (modelLength-1) +  " <" + VOIDVocabulary.classPartition + "> ?c" + modelLength + " . ?c" + modelLength +  " <" + VOIDVocabulary.clazz + "> " + "<" + pe.getUri() + "> ." ;
//				
//				modelLength++;
//			} else {
//				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
//				endVar = "c" + (pathLength + 1);
//			    pathLength++;
//
//			    modelPath += "?c" + (modelLength-1) +  " <" + VOIDVocabulary.propertyPartition + "> ?c" + modelLength + " . ?c" + modelLength +  " <" + VOIDVocabulary.property + "> " + "<" + pe.getUri() + "> ." ;
//			    
//			    modelLength++;
//			}
//		}
//		
//		modelPath = "SELECT ?c" + (modelLength - 1) + " WHERE { " + modelPath + " } ";
//				
//
////		System.out.println("EXPLORING " + spath);
//		if (pathLength == 0) {
////			spath = "?entry  ?prop ?z . FILTER NOT EXISTS { ?k1 ?k2 ?entry }" +
////					" OPTIONAL { " +
////		            "  ?entry  ?prop ?b  " + 
////		            "  FILTER isBlank(?b) } ";
////			spath = "?c0 ?prop ?z . ?c0 a ?type . FILTER isIRI(?c0) " +
//			spath = "?c0 ?prop ?z . " + spath + // " FILTER (isIRI(?c0))" + // FILTER is not necessary here and may causes virtuoso timeout
//					" OPTIONAL { " +
//		            "  ?c0  ?prop ?b  " + 
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
//		}
//		
//		String sparql = 
////		    	"SELECT ?type ?prop (count(?z) AS ?countz) (count(?b) AS ?countb) " + 
////		        "WHERE { " +
////				"  GRAPH <" + url + "> { " +
////		    	     spath +
////                " } } " +
////		 		"GROUP BY ?type ?prop " +
////                "ORDER BY ?type ?prop";
//		    	"SELECT ?prop (count(?z) AS ?countz) (count(?b) AS ?countb) " + 
//		        "WHERE { " +
//				"  GRAPH <" + datasetUri + "> { " +
//		    	     spath +
//                " } } " +
//		 		"GROUP BY ?prop " +
//                "ORDER BY ?prop";
//
//		
//		Query query = QueryFactory.create(sparql);
////		System.out.println(query);
//		
//		Resource subject = null;
//		try (QueryExecution lqe = QueryExecutionFactory.create(modelPath, outModel)) {
//			ResultSet qrs = lqe.execSelect();
//			while (qrs.hasNext()) {
//				QuerySolution qqs = qrs.next();
//				subject = qqs.get("?c" + (modelLength - 1)).asResource();
//			}
//		}
//
//		
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), query)) {
//			
//			ResultSet rs = qe.execSelect();
//			
//			loop:
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//				String prop = sol.get("prop").asResource().getURI();
//				if (prop.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
//					continue;
//				}
//
//				// check this
//				String prefix = PathElement.onPropertyListAsString(path);
//				String pp =  (prefix.length() > 0 ? "/" : "") + "<" + prop  + ">";
//
//				List<PagedAnnotationValidation> relevantPavs = new ArrayList<>();
//				if (pavList != null) {
//
//					for (PagedAnnotationValidation xpav : pavList) {
//						if (AnnotationEditGroup.onPropertyListAsString(xpav.getOnProperty()).startsWith(pp)) {
//							relevantPavs.add(xpav);
//						}
//					}
//
//					if (relevantPavs.isEmpty()) {
//						continue loop;
//					}
//				}
//
//				String lsparql = legacyUris ?
//				    	"CONSTRUCT  { " + 
//				    	  "<" + prop + "> a <http://www.w3.org/2000/01/rdf-schema#Property> . " +
//				    	  "<" + prop + "> <" + RDFSVocabulary.label + "> ?label } " + 
//				        "WHERE { " +
//				        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//				        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//				    	"	 OPTIONAL { <" + datasetUri + ">  <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> [ " +
//				    	"      <http://sw.islab.ntua.gr/apollonis/ms/uri> <" + prop + "> ; " +
//				    	"      <" + RDFSVocabulary.label + "> ?label ] } } }" :
//				    	"CONSTRUCT  { " + 
//				    	  "<" + prop + "> a <http://www.w3.org/2000/01/rdf-schema#Property> . " +
//				    	  "<" + prop + "> <" + RDFSVocabulary.label + "> ?label } " + 
//				        "WHERE { " +
//				        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//				        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//				    	"	 OPTIONAL { <" + datasetUri + ">  <" + SEMAVocabulary.dataProperty + "> [ " +
//				    	"      <" + SEMAVocabulary.uri + "> <" + prop + "> ; " +
//				    	"      <" + RDFSVocabulary.label + "> ?label ] } } }";
//	
////				System.out.println(QueryFactory.create(lsparql));
//
//				Resource partition = outModel.createResource();
//				partition.addProperty(VOIDVocabulary.property, outModel.createResource(prop));
//				
//				outModel.add(subject, VOIDVocabulary.propertyPartition, partition);
//				
//				Writer sw = new StringWriter();
//	
//				try (QueryExecution lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql))) {
//					Model model =  lqe.execConstruct();
//					RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//				}
//				
//				ObjectMapper mapper = new ObjectMapper();
//				ObjectNode info = (ObjectNode)((ArrayNode)mapper.readValue(sw.toString(), ArrayNode.class)).get(0);
//				ObjectNode node = mapper.createObjectNode();
//				node.put("info", info);
//	
//				int total = sol.get("countz").asLiteral().getInt();
//				int blank = sol.get("countb").asLiteral().getInt();
//
//				List<PathElement> newPath = new ArrayList<>();
//				newPath.addAll(path);
//				newPath.add(new PathElement("property", prop));
//
//				if (blank > 0) {
//					ArrayNode newArray = mapper.createArrayNode();
//
//					explore(vc, newPath, datasetUri, newArray, pavList, outModel);
//					node.put("children", newArray);
//				}
//
//				node.put("count", total);
//
//				partition.addProperty(VOIDVocabulary.triples, sol.get("countz"));
//
//				array.add(node);
//				
//			}
//		}
//	}

//	private List<ObjectNode> getTopClassesDescription(VirtuosoConfiguration vc, String datasetUri, ObjectMapper mapper, Model outModel) throws Exception {
//		String lsparql =
////	    	"SELECT  ?clazz ?label " +
//			"SELECT  ?clazz (count(?x) as ?count)" +
//	        "WHERE { " +
//	        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//	        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//	    	"	 <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/class" : SEMAVocabulary.clazz) + "> ?t . " +
//	    	"    ?t  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> ?clazz . } " +
////		    	"    OPTIONAL { ?t <http://www.w3.org/2000/01/rdf-schema#label> ?label } } " +
//            "  GRAPH <" + datasetUri + "> { " +
//            "    ?x a ?clazz } " +
//	    	"  } " +
//	    	"GROUP BY ?clazz ";
//	    	//"ORDER BY ?clazz ";
//
//			System.out.println(QueryFactory.create(lsparql));
//		List<ObjectNode> result = new ArrayList<>();
//
//		try (QueryExecution lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql))) {
//			ResultSet rs =  lqe.execSelect();
//			while (rs.hasNext()) {
//				QuerySolution qs = rs.next();
//				Resource clazz = qs.get("clazz").asResource();
//
//				String csparql =
//				    	"CONSTRUCT  { " +
//				    	  "<" + clazz + "> a <http://www.w3.org/2000/01/rdf-schema#Class> . " +
//				    	  "<" + clazz + "> <" + RDFSVocabulary.label + "> ?label } " +
//				        "WHERE { " +
//				        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//				        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//				    	"	 OPTIONAL { <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/dataProperty" : SEMAVocabulary.dataProperty) + "> [ " +
//				    	"      <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> <" + clazz + "> ; " +
//				    	"      <" + RDFSVocabulary.label + "> ?label ] } } }";
//
//				Writer sw = new StringWriter();
//
//				try (QueryExecution cqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(csparql))) {
//					Model model =  cqe.execConstruct();
//					RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//				}
//			
//				ObjectNode node = (ObjectNode)((ArrayNode)mapper.readValue(sw.toString(), ArrayNode.class)).get(0);
//				node.put("count", qs.get("count").asLiteral().getInt());
//
//				result.add(node);
//				
//				csparql =
//				    	"CONSTRUCT  { " +
//				    	  "<" + datasetUri +"> a <" + VOIDVocabulary.Dataset + "> ; " +
//				    	  "  <" + VOIDVocabulary.classPartition + "> [ " +
//				    	  "    <" + VOIDVocabulary.clazz + "> <" + clazz + "> ; " +
//				    	  "    <" + VOIDVocabulary.entities + "> " + qs.get("count").asLiteral().getInt() + " ; " +
//				    	  "    <" + RDFSVocabulary.label + "> ?label ] } " +
//				        "WHERE { " +
//				        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//				        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//				    	"	 OPTIONAL { <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/dataProperty" : SEMAVocabulary.dataProperty) + "> [ " +
//				    	"      <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> <" + clazz + "> ; " +
//				    	"      <" + RDFSVocabulary.label + "> ?label ] } } }";
//
//
//				try (QueryExecution cqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(csparql))) {
//					outModel.add(cqe.execConstruct());
//				}
//				
//			}
//		}
//		
//		return result;
//	}
//	private List<ObjectNode> getTopClassesDescription(VirtuosoConfiguration vc, String datasetUri, ObjectMapper mapper, Model outModel) throws Exception {
//		String lsparql =
////	    	"SELECT  ?clazz ?label " +
//			"SELECT  ?clazz (count(?x) as ?count)" +
//	        "WHERE { " +
//	        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//	        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//	    	"	 <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/class" : SEMAVocabulary.clazz) + "> ?t . " +
//	    	"    ?t  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> ?clazz . } " +
////		    	"    OPTIONAL { ?t <http://www.w3.org/2000/01/rdf-schema#label> ?label } } " +
//            "  GRAPH <" + datasetUri + "> { " +
//            "    ?x a ?clazz } " +
//	    	"  } " +
//	    	"GROUP BY ?clazz ";
//	    	//"ORDER BY ?clazz ";
//
//			System.out.println(QueryFactory.create(lsparql));
//		List<ObjectNode> result = new ArrayList<>();
//
//		try (QueryExecution lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql))) {
//			ResultSet rs =  lqe.execSelect();
//			while (rs.hasNext()) {
//				QuerySolution qs = rs.next();
//				Resource clazz = qs.get("clazz").asResource();
//
//				String csparql =
//				    	"CONSTRUCT  { " +
//				    	  "<" + clazz + "> a <http://www.w3.org/2000/01/rdf-schema#Class> . " +
//				    	  "<" + clazz + "> <" + RDFSVocabulary.label + "> ?label } " +
//				        "WHERE { " +
//				        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//				        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//				    	"	 OPTIONAL { <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/dataProperty" : SEMAVocabulary.dataProperty) + "> [ " +
//				    	"      <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> <" + clazz + "> ; " +
//				    	"      <" + RDFSVocabulary.label + "> ?label ] } } }";
//
//				Writer sw = new StringWriter();
//
//				try (QueryExecution cqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(csparql))) {
//					Model model =  cqe.execConstruct();
//					RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//				}
//			
//				ObjectNode node = (ObjectNode)((ArrayNode)mapper.readValue(sw.toString(), ArrayNode.class)).get(0);
//				node.put("count", qs.get("count").asLiteral().getInt());
//
//				result.add(node);
//				
//				csparql =
//				    	"CONSTRUCT  { " +
//				    	  "<" + datasetUri +"> a <" + VOIDVocabulary.Dataset + "> ; " +
//				    	  "  <" + VOIDVocabulary.classPartition + "> [ " +
//				    	  "    <" + VOIDVocabulary.clazz + "> <" + clazz + "> ; " +
//				    	  "    <" + VOIDVocabulary.entities + "> " + qs.get("count").asLiteral().getInt() + " ; " +
//				    	  "    <" + RDFSVocabulary.label + "> ?label ] } " +
//				        "WHERE { " +
//				        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//				        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
//				    	"	 OPTIONAL { <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/dataProperty" : SEMAVocabulary.dataProperty) + "> [ " +
//				    	"      <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> <" + clazz + "> ; " +
//				    	"      <" + RDFSVocabulary.label + "> ?label ] } } }";
//
//
//				try (QueryExecution cqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(csparql))) {
//					outModel.add(cqe.execConstruct());
//				}
//				
//			}
//		}
//		
//		return result;
//	}


    @GetMapping(value = "/schema/get",
                produces = "application/json")
	public ResponseEntity<?> getValidatorSchema(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("dataset") String datasetUri)  {

		ObjectMapper mapper = new ObjectMapper();
    	ArrayNode result = mapper.createArrayNode();

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		UserType userType = currentUser.getType();
		List<PagedAnnotationValidation> pavList = null;

		if (userType == UserType.VALIDATOR) {
			List<Access> accessList = accessRepository.findByUserIdAndCollectionUuidAndAccessType(new ObjectId(currentUser.getId()), datasetUuid, AccessType.VALIDATOR);
			if (accessList.isEmpty()) {
				return ResponseEntity.ok(result);
			}

			if (userType == UserType.VALIDATOR) {
				pavList = pavRepository.findByDatasetUuid(datasetUuid);
	    	}
		} else {
			if (!datasetRepository.findByUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId())).isPresent()) {
				return ResponseEntity.ok(result);
			}
		}
		
//		Dataset ds = datasetRepository.findByUuid(datasetUuid).get();
//		VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		try {
			Writer sw = new StringWriter();

//			Model model = ModelFactory.createDefaultModel();
//			
//			for (ObjectNode node : getTopClassesDescription(vc, datasetUri, mapper, model)) {
//
//				ArrayList<PathElement> path = new ArrayList<>();
//				path.add(new PathElement("class", node.get("@id").asText()));
//
//				ArrayNode array = mapper.createArrayNode();
//				
//				explore(vc, path, datasetUri, array, pavList, model);
//
//				node.put("content", array);
//
//				if (array.size() > 0 || pavList == null) {
//					result.add(node);
//				}
//			}
//			

			Model model = schemaService.readSchema(datasetUri);
			if (model.size() == 0) {
				model = schemaService.buildSchema(datasetUri, false);
			}
			
			if (pavList != null) {	
				StmtIterator siter = model.listStatements(model.createResource(datasetUri), VOIDVocabulary.classPartition, (RDFNode)null);
				while (siter.hasNext()) {
					Statement stmt = siter.next();
					Resource obj = stmt.getObject().asResource();
					
					List<PathElement> path = new ArrayList<>();
					path.add(new PathElement("class", obj.getProperty(VOIDVocabulary.clazz).getObject().toString()));
					removeNonValidationPaths(model, obj, path, pavList);
							
				}
			}
			
			RDFDataMgr.write(sw, model, RDFFormat.JSONLD) ;
			
			return ResponseEntity.ok(sw.toString());				

		} catch (Exception e) {
			e.printStackTrace();

			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

    private void removeNonValidationPaths(Model model, Resource subj, List<PathElement> path, List<PagedAnnotationValidation> pavList) {
		StmtIterator siter = model.listStatements(subj, VOIDVocabulary.propertyPartition, (RDFNode)null);
		while (siter.hasNext()) {
			Statement stmt = siter.next();
			Resource obj = stmt.getObject().asResource();
		
			String prop = obj.getProperty(VOIDVocabulary.property).getObject().toString();
			
			String prefix = PathElement.onPropertyListAsString(path);
			String pp =  (prefix.length() > 0 ? "/" : "") + "<" + prop  + ">";

			List<PagedAnnotationValidation> relevantPavs = new ArrayList<>();
			for (PagedAnnotationValidation xpav : pavList) {
				if (AnnotationEditGroup.onPropertyListAsString(xpav.getOnProperty()).startsWith(pp)) {
					relevantPavs.add(xpav);
				}
			}

			if (relevantPavs.isEmpty()) {
				siter.remove();
			} else {
				List<PathElement> newPath = new ArrayList<>();
				newPath.addAll(path);
				newPath.add(new PathElement("property", prop));

				removeNonValidationPaths(model, obj, newPath, pavList);
			}
			
		}
    }

	@PostMapping(value = "/schema/getPropertyValues", produces = "application/json")
	public ResponseEntity<?> getValues(@CurrentUser UserPrincipal currentUser, @RequestParam("uri") String uri,
			@RequestParam("mode") String mode, @RequestBody List<PathElement> path) {

		String datasetUuid = SEMAVocabulary.getId(uri);

		Dataset ds = datasetRepository.findByUuid(datasetUuid).get();
		VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		String spath = "";
		String endVar = "";

//		for (int i = 0; i < path.size(); i++) {
//			if (i > 0) {
//				spath +="/";
//			}
//			spath += "<" + path.get(i) + ">";

		int pathLength = 0;
		for (int i = 0; i < path.size(); i++) {
//			if (i > 0) {
//				spath += "/";
//			}
//			spath += "<" + path.get(i) + ">";

			PathElement pe = path.get(i);
			if (pe.getType().equals("class")) {
				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
			} else {
				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
				endVar = "c" + (pathLength + 1);
				pathLength++;
			}
		}


		String filter = mode.equals("ALL") ? "" : (mode.equals("IRI") ? "FILTER (isIRI(?" + endVar + "))" : "FILTER (isLiteral(?" + endVar + "))");  
		String sparql = 
				"SELECT ?" + endVar + "  ?count  { " +
				"SELECT distinct ?" + endVar + " (count(?" + endVar + ") as ?count) " +
		        "WHERE { " +
				"  GRAPH <" + uri + "> { " +
//		        "    ?s " + spath + " ?value } } " +
				      spath + filter + 
				"  } } " +
		        "GROUP BY ?" + endVar + " " +
		        "ORDER BY DESC(?count) ?" + endVar + 
		        "} ";
		
		List<ValueResponse> res = new ArrayList<>();

//		System.out.println(sparql);
//		System.out.println(QueryFactory.create(sparql));

//		try (VirtuosoSelectIterator vs = new VirtuosoSelectIterator(virtuosoConfiguration.getSparqlEndpoint(), sparql)) {
		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				RDFNode node = sol.get(endVar);

				res.add(new ValueResponse(node.toString(), sol.get("count").asLiteral().getInt(), node.isLiteral()));
			}
		}

		return ResponseEntity.ok(res);
	}

	@PostMapping(value = "/schema/downloadPropertyValues")
	public ResponseEntity<?> downloadValues(@CurrentUser UserPrincipal currentUser, @RequestParam("uri") String uri,
			@RequestParam("mode") String mode, @RequestBody List<PathElement> path) {

		String datasetUuid = SEMAVocabulary.getId(uri);

		Dataset ds = datasetRepository.findByUuid(datasetUuid).get();
		VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		String spath = "";
		String endVar = "";

		int pathLength = 0;

		for (int i = 0; i < path.size(); i++) {
			PathElement pe = path.get(i);
			if (pe.getType().equals("class")) {
				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
			} else {
				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
				endVar = "c" + (pathLength + 1);
				pathLength++;
			}
		}

		String filter = mode.equals("ALL") ? ""
				: (mode.equals("IRI") ? "FILTER (isIRI(?" + endVar + "))" : "FILTER (isLiteral(?" + endVar + "))");
		String sparql = "SELECT ?c0 ?" + endVar + " " + "WHERE { " + "  GRAPH <" + uri + "> { " + spath + filter
				+ "  } } ";
		

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
				Writer writer = new BufferedWriter(new OutputStreamWriter(bos));
				CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Item", "IRI", "LiteralLexicalForm", "LiteralLanguage", "LiteralDatatype"));
				ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			
			try (VirtuosoSelectIterator vs = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {
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

			String fileName = SEMAVocabulary.getId(uri) + "_" + generateNameFromUrl(path.get(path.size() -1).getUri());

			try (ZipOutputStream zos = new ZipOutputStream(baos)) {
				ZipEntry entry = new ZipEntry(fileName + ".csv");

				zos.putNextEntry(entry);
				zos.write(bos.toByteArray());
				zos.closeEntry();

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}

			ByteArrayResource resource = new ByteArrayResource(baos.toByteArray());

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fileName + ".zip");

			return ResponseEntity.ok().headers(headers)
//	            .contentLength(ffile.length())
					.contentType(MediaType.APPLICATION_OCTET_STREAM).body(resource);

		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	
	public static String generateNameFromUrl(String url){

	    // Replace useless chareacters with UNDERSCORE
	    String uniqueName = url.replace("://", "_").replace(".", "_").replace("/", "_");
	    // Replace last UNDERSCORE with a DOT
	    uniqueName = uniqueName.substring(0,uniqueName.lastIndexOf('_'))
	            +"."+uniqueName.substring(uniqueName.lastIndexOf('_')+1,uniqueName.length());
	    return uniqueName;
	}

}
