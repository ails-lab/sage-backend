package ac.software.semantic.controller;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import io.swagger.v3.oas.annotations.Parameter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Access;
import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.constants.AccessType;
import ac.software.semantic.model.constants.UserRoleType;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.PropertyDoubleValue;
import ac.software.semantic.payload.PropertyValue;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationReference;
import ac.software.semantic.payload.ValueResponse;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.repository.AccessRepository;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.SchemaService;
import ac.software.semantic.service.ValueCount;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
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
	private SEMRVocabulary resourceVocabulary;
	
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;
    

    @GetMapping(value = "/schema/get",
                produces = "application/json")
	public ResponseEntity<?> getValidatorSchema(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("dataset") String datasetUri, @RequestParam(name = "role", defaultValue = "EDITOR") UserRoleType role)  {

		ObjectMapper mapper = new ObjectMapper();
    	ArrayNode result = mapper.createArrayNode();

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

		UserRoleType userType = currentUser.getType();
		List<PagedAnnotationValidation> pavList = null;

//		System.out.println(">> " + userType + " " + role + " " + datasetUuid);
		
		if (userType == UserRoleType.VALIDATOR || (userType == UserRoleType.EDITOR && role == UserRoleType.VALIDATOR)) {
			List<Access> accessList = accessRepository.findByUserIdAndCollectionUuidAndAccessType(new ObjectId(currentUser.getId()), datasetUuid, AccessType.VALIDATOR);
			if (accessList.isEmpty()) {
				return ResponseEntity.ok(result);
			}

			pavList = pavRepository.findByDatasetUuid(datasetUuid);
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
			
//			System.out.println(">> " + model.size());
			if (model.size() <= 1) {
				model = schemaService.buildSchema(datasetUri, false);
			}
			
			if (pavList != null) {	
				StmtIterator siter = model.listStatements(model.createResource(datasetUri), VOIDVocabulary.classPartition, (RDFNode)null);
				while (siter.hasNext()) {
					Statement stmt = siter.next();
					Resource obj = stmt.getObject().asResource();
					
					List<PathElement> path = new ArrayList<>();
					path.add(PathElement.createClassPathElement(obj.getProperty(VOIDVocabulary.clazz).getObject().toString()));
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
			
//			String prefix = PathElement.onPathElementListAsString(path);
//			String pp =  (prefix.length() > 0 ? "/" : "") + "<" + prop  + ">";
			
			List<String> prefix = PathElement.onPathElementListAsStringList(path);

			List<PagedAnnotationValidation> relevantPavs = new ArrayList<>();
			for (PagedAnnotationValidation xpav : pavList) {
//				if (AnnotationEditGroup.onPropertyListAsString(xpav.getOnProperty()).startsWith(pp)) {
//					relevantPavs.add(xpav);
//				}

//				System.out.println(prefix + " /2 " + xpav.getOnProperty());
				
				if (prefix.size() <= xpav.getOnProperty().size()) {
					boolean keep = true;
					for (int i = 0; i < prefix.size(); i++) {
						if (!prefix.get(i).equals(xpav.getOnProperty().get(i))) {
							keep = false;
							break;
						}
					}
					
					if (keep) {
						relevantPavs.add(xpav);
					}
				}
			}

			if (relevantPavs.isEmpty()) {
				siter.remove();
			} else {
				List<PathElement> newPath = new ArrayList<>();
				newPath.addAll(path);
				newPath.add(PathElement.createPropertyPathElement(prop));

				removeNonValidationPaths(model, obj, newPath, pavList);
			}
			
		}
		
		siter = model.listStatements(subj, VOIDVocabulary.classPartition, (RDFNode)null);
		while (siter.hasNext()) {
			Statement stmt = siter.next();
			Resource obj = stmt.getObject().asResource();
		
			String prop = obj.getProperty(VOIDVocabulary.clazz).getObject().toString();
			
//			String prefix = PathElement.onPathElementListAsString(path);
//			String pp =  (prefix.length() > 0 ? "/" : "") + "<" + prop  + ">";
			
			List<String> prefix = PathElement.onPathElementListAsStringList(path);

			List<PagedAnnotationValidation> relevantPavs = new ArrayList<>();
			
			for (PagedAnnotationValidation xpav : pavList) {
//				if (AnnotationEditGroup.onPropertyListAsString(xpav.getOnProperty()).startsWith(pp)) {
//					relevantPavs.add(xpav);
//				}
			
//				System.out.println(prefix + " /1 " + xpav.getOnProperty());

				if (prefix.size() <= xpav.getOnProperty().size()) {
					boolean keep = true;
					for (int i = 0; i < prefix.size(); i++) {
						if (!prefix.get(i).equals(xpav.getOnProperty().get(i))) {
							keep = false;
							break;
						}
					}
					
					if (keep) {
						relevantPavs.add(xpav);
					}
				}
			}

			if (relevantPavs.isEmpty()) {
				siter.remove();
			} else {
				List<PathElement> newPath = new ArrayList<>();
				newPath.addAll(path);
				newPath.add(PathElement.createClassPathElement(prop));

				removeNonValidationPaths(model, obj, newPath, pavList);
			}
			
		}
    }

	@PostMapping(value = "/schema/get-property-values", produces = "application/json")
	public ResponseEntity<?> getValues(@CurrentUser UserPrincipal currentUser, 
			                           @RequestParam("uri") String datasetUri,
			                           @RequestParam("mode") String mode, 
			                           @RequestBody List<PathElement> path) {

		List<ValueResponse> res = new ArrayList<>();

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

//		Dataset ds = datasetRepository.findByUuid(datasetUuid).get();
		
    	DatasetCatalog dcg = schemaService.asCatalog(datasetUuid);

    	String fromClause = schemaService.buildFromClause(dcg);

    	// get triple store of first dataset ... wrong!
		TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

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


		String filter = mode.equals("ALL") ? "" : (mode.equals("IRI") ? "FILTER (isIRI(?" + endVar + "))" : "FILTER (isLiteral(?" + endVar + "))");  
		
		String sparql = 
				"SELECT ?" + endVar + "  ?count " + fromClause + " { " +
				"SELECT distinct ?" + endVar + " (count(?" + endVar + ") as ?count) " +
		        "WHERE { " +
				      spath + filter + 
				"  } " +
		        "GROUP BY ?" + endVar + " " +
		        "ORDER BY DESC(?count) ?" + endVar + 
		        "} ";
		
		ValueResponseContainer<ValueResponse> vrc = new ValueResponseContainer<>();
		

//		System.out.println(sparql);
//		System.out.println(QueryFactory.create(sparql));

		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				RDFNode node = sol.get(endVar);

				AnnotationEditValue aev = null;
		    		
	    		if (node.isLiteral()) {
					aev = new AnnotationEditValue(node.asLiteral());
				} else {
					aev = new AnnotationEditValue(node.asResource());
				}
				
				res.add(new ValueResponse(aev, sol.get("count").asLiteral().getInt()));
			}
		}

		vrc.setValues(res);
		
		sparql = 
				"SELECT (count(?" + endVar + ") as ?count) (count(distinct ?" + endVar + ") as ?distinctCount) " +
		        fromClause + 
		        "WHERE { " +
				      spath + filter + 
				"   } ";
		

//		System.out.println(sparql);
//		System.out.println(QueryFactory.create(sparql));

		int totalCount = 0;
		int distinctTotalCount = 0;
		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();

				totalCount = sol.get("count").asLiteral().getInt();
				distinctTotalCount = sol.get("distinctCount").asLiteral().getInt();
			}
		}

		vrc.setTotalCount(totalCount);
		vrc.setDistinctTotalCount(distinctTotalCount);
		
		return ResponseEntity.ok(vrc);
	}

	@PostMapping(value = "/schema/download-property-values")
	public ResponseEntity<?> downloadValues(
			@CurrentUser UserPrincipal currentUser, 
			@RequestParam("uri") String datasetUri,
			@RequestParam("mode") String mode,
			@RequestBody List<PathElement> path) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

//		Dataset ds = datasetRepository.findByUuid(datasetUuid).get();
//		TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

    	DatasetCatalog dcg = schemaService.asCatalog(datasetUuid);
    	String fromClause = schemaService.buildFromClause(dcg);

    	// get triple store of first dataset ... wrong!
		TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

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

		String filter = mode.equals("ALL") ? ""
				: (mode.equals("IRI") ? "FILTER (isIRI(?" + endVar + "))" : "FILTER (isLiteral(?" + endVar + "))");
		String sparql = "SELECT ?c0 ?" + endVar + " " + fromClause + " WHERE { " + spath + filter + "  } ";
		

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

			String fileName = resourceVocabulary.getUuidFromResourceUri(datasetUri) + "_" + generateNameFromUrl(path.get(path.size() -1).getUri());

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

	@PostMapping(value = "/schema/get-items-by-property-value", produces = "application/json")
	public ResponseEntity<?> getItemsByPropertyValue(
			@CurrentUser UserPrincipal currentUser, 
			@RequestParam("uri") String datasetUri,
			@RequestBody PropertyValue pv) {
    	
		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

//		Dataset ds = datasetRepository.findByUuid(datasetUuid).get();
//		TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

    	DatasetCatalog dcg = schemaService.asCatalog(datasetUuid);
    	String fromClause = schemaService.buildFromClause(dcg);

    	// get triple store of first dataset ... wrong!
		TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		
		String path = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(pv.getPath()));
		
		String sparql = 
				"SELECT ?s " + 
			    fromClause + 
				"WHERE { " +
		        "	?s " + path + " " + (pv.getValue().getIri() != null ? "<" : "") + pv.getValue().toString() + (pv.getValue().getIri() != null ? ">" : "") +
		        " } ";
			
		ValueResponseContainer<ValueResponse> vrc = new ValueResponseContainer<>();
		
		List<ValueResponse> res = new ArrayList<>();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
			
			ResultSet rs = qe.execSelect();
				
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				Resource node = sol.get("s").asResource();
				
				
				res.add(new ValueResponse(new AnnotationEditValue(node.asResource()), 1));

			}
		}
		
		vrc.setValues(res);
//		vrc.setTotalCount(totalCount);
		
		return ResponseEntity.ok(vrc);
	}
	
	public static String generateNameFromUrl(String url){

	    // Replace useless chareacters with UNDERSCORE
	    String uniqueName = url.replace("://", "_").replace(".", "_").replace("/", "_");
	    // Replace last UNDERSCORE with a DOT
	    uniqueName = uniqueName.substring(0,uniqueName.lastIndexOf('_'))
	            +"."+uniqueName.substring(uniqueName.lastIndexOf('_')+1,uniqueName.length());
	    return uniqueName;
	}

	@PostMapping(value = "/schema/get-properties-by-value-and-target", produces = "application/json")
	public ResponseEntity<?> getValues(@CurrentUser UserPrincipal currentUser, 
			                           @RequestParam("uri") String datasetUri,
			                           @RequestBody PropertyDoubleValue pdv) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

//		Dataset ds = datasetRepository.findByUuid(datasetUuid).get();
//		TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

    	DatasetCatalog dcg = schemaService.asCatalog(datasetUuid);
    	String fromClause = schemaService.buildFromClause(dcg);

    	// get triple store of first dataset ... wrong!
		TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

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

		try (QueryExecution  lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql))) {
			ResultSet vs =  lqe.execSelect();
			while (vs.hasNext()) {
				QuerySolution sol = vs.next();
				
				res.add(new ValueAnnotationReference(sol.get("prop").toString(), sol.get("count").asLiteral().getInt()));
			}
		}

		return ResponseEntity.ok(res);
	}

}
