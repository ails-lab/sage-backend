package ac.software.semantic.service;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import ac.software.semantic.model.*;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.constants.AnnotationEditType;
import ac.software.semantic.model.constants.SerializationType;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.DatasetFactory;
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
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdApi;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.config.AppConfiguration.JenaRDF2JSONLD;
import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.payload.AnnotationEditGroupResponse;
import ac.software.semantic.payload.Distribution;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoConstructIterator;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.FOAFVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class AnnotationEditGroupService {

	Logger logger = LoggerFactory.getLogger(AnnotationEditGroupService.class);

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;
			
	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	AnnotatorDocumentRepository annotatorRepository;
	
	@Autowired
	AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	PagedAnnotationValidationRepository pavRepository;

	@Autowired
	FilterAnnotationValidationRepository favRepository;
	
	@Autowired
	DataServiceRepository dsRepository;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private SPARQLService sparqlService;
	
	@Autowired
	@Qualifier("annotation-jsonld-context")
    private Map<String, Object> annotationContext;
	
	public List<AnnotationEditGroupResponse> getAnnotationEditGroups(UserPrincipal currentUser, String datasetUri) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);
		
		ProcessStateContainer psv = datasetRepository.findByUuid(datasetUuid).get().getCurrentPublishState(virtuosoConfigurations.values());
		
		TripleStoreConfiguration vc;
		if (psv != null) {
			vc = psv.getTripleStoreConfiguration();
		} else {
			vc = null;
		}

		List<AnnotationEditGroup> docs = aegRepository.findByDatasetUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		List<AnnotationEditGroupResponse> response = docs.stream()
				.map(doc -> modelMapper.annotationEditGroup2AnnotationEditGroupResponse(vc, doc, pavRepository.findByAnnotationEditGroupId(doc.getId()), favRepository.findByAnnotationEditGroupId(doc.getId())))
				.collect(Collectors.toList());

		return response;
	}

	public List<AnnotationEditGroupResponse> getAnnotationEditGroups(String datasetUri) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

		ProcessStateContainer psv = datasetRepository.findByUuid(datasetUuid).get().getCurrentPublishState(virtuosoConfigurations.values());
		
		TripleStoreConfiguration vc;
		if (psv != null) {
			vc = psv.getTripleStoreConfiguration();
		} else {
			vc = null;
		}
		
		List<AnnotationEditGroup> docs = aegRepository.findByDatasetUuid(datasetUuid);

		List<AnnotationEditGroupResponse> response = docs.stream()
				.map(doc -> modelMapper.annotationEditGroup2AnnotationEditGroupResponse(vc, doc, pavRepository.findByAnnotationEditGroupId(doc.getId()), favRepository.findByAnnotationEditGroupId(doc.getId())))
				.collect(Collectors.toList());

		return response;
	}

	public ByteArrayResource exportAnnotations(UserPrincipal currentUser, String id, SerializationType format, 
			boolean onlyReviewed, boolean onlyNonRejected, boolean onlyFresh, boolean created, boolean creator, boolean score, boolean scope, boolean selector, 
//			String defaultScope, 
			String archive
			) throws Exception {
		if (archive.equalsIgnoreCase("zip")) {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				try (ZipOutputStream zos = new ZipOutputStream(baos)) {
					exportAnnotations(id, format, onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, zos);
					zos.finish();
				}

				return new ByteArrayResource(baos.toByteArray());
			}
		} else if (archive.equalsIgnoreCase("tgz"))	{
    		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
    			try (BufferedOutputStream buffOut = new BufferedOutputStream(baos);
    					GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
    					TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {

    				exportAnnotations(id, format, onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, tOut);
    			}
		    	
    			return new ByteArrayResource(baos.toByteArray());
			}
		} else {
			return null;
		}
		
	}
	
	public void exportAnnotations(String id, SerializationType format, 
			boolean onlyReviewed, boolean onlyNonRejected, boolean onlyFresh, boolean created, boolean creator, boolean confidence, boolean scope, boolean selector,  
			OutputStream out
			) throws Exception {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(id));
		if (!aegOpt.isPresent()) {
			return ;
		}		
		
		AnnotationEditGroup aeg = aegOpt.get();
		
		Optional<Dataset> dopt = datasetRepository.findByUuid(aeg.getDatasetUuid());
		
		TripleStoreConfiguration vc = dopt.get().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		List<String> generatorIds = new ArrayList<>();
		generatorIds.addAll(annotatorRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(doc -> resourceVocabulary.getAnnotatorAsResource(doc.getUuid()).toString()).collect(Collectors.toList()));
		generatorIds.addAll(pavRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> resourceVocabulary.getAnnotationValidatorAsResource(doc.getUuid()).toString()).collect(Collectors.toList()));
		generatorIds.addAll(favRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> resourceVocabulary.getAnnotationValidatorAsResource(doc.getUuid()).toString()).collect(Collectors.toList()));

		String annfilter = sparqlService.generatorFilter("annotation", generatorIds);
		
		String valFilter = "";
		if (onlyNonRejected) {
			valFilter = " FILTER NOT EXISTS { ?annotation <" + SOAVocabulary.hasValidation + "> [ <" + SOAVocabulary.action + "> <" + SOAVocabulary.Delete + "> ] } . ";
		}
	
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
    	int pathLength = PathElement.onPathLength(aeg.getOnProperty());
		String onPropertyStringAsPath = PathElement.onPathStringListAsMiddleRDFPath(aeg.getOnProperty());
    	if (pathLength == 1) {
    		onPropertyStringAsPath = onPropertyStringAsPath.substring(1, onPropertyStringAsPath.length() - 1);
    	}

    	boolean state = true;

    	// validated first 
    	String whereSparqlValidated = 
    			"WHERE { " +
    			"  GRAPH <" + resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString() + "> { " + 
    			"    ?source " + onPropertyString + " ?value }  " +
    			"  GRAPH <" + aeg.getAsProperty() + "> { " + 
    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
    			     annfilter + 
    			     valFilter +
    			     (created ? " OPTIONAL { ?annotation <" + DCTVocabulary.created + "> ?created . } " : "") +
    			     (confidence ? " OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?confidence . } " : "") +
    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
    			"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
        		"    ?target <" + SOAVocabulary.onValue + "> ?value . BIND (str(?value) AS ?valueStr) . BIND (lang(?value) AS ?valueLang) " +
        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
        		     (onlyFresh ? "FILTER NOT EXISTS  { ?target <" + DCTVocabulary.isReferencedBy + "> ?reference } . " : "") +
        		"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " + 
        		"    ?validation <" + SOAVocabulary.action + "> ?validationAction  . " +
//        		     (creator ? " OPTIONAL { ?annotation <" + DCTVocabulary.creator + "> [ a ?ct ] } .  BIND (IF(bound(?ct), ?ct, <" + ASVocabulary.Application + ">) AS ?creatorType) . " : "") + // manuall created annotations have <http://purl.org/dc/terms/creator> [ a       <http://xmlns.com/foaf/0.1/Person> ] ;
        		     (selector ? " OPTIONAL { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?startV . BIND (STRDT(STR(?startV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?start) } " : "") +
        		     (selector ? " OPTIONAL { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?endV . BIND (STRDT(STR(?endV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?end) } " : "") +
        		     (scope ? " OPTIONAL { ?validation <" + OAVocabulary.hasScope + "> ?scope } . " : "") +
//        		     (scope ? " BIND (" + (defaultScope != null ? "IF(bound(?scope), ?scope, <" + defaultScope + ">)" : "?scope") + " AS ?aScope) " : "") +
        		"  } } ";
    	
//    	String whereSparqlValidatedGroup = 
//    			"WHERE { " +
//    	        "SELECT ?source ?body ?validationAction ?scope (SAMPLE(?annotation) AS ?sampleAnnotation) (AVG(?confidence) AS ?avgConfidence) { " +
//    			"  GRAPH <" + resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString() + "> { " + 
//    			"    ?source " + onPropertyString + " ?value }  " +
//    			"  GRAPH <" + aeg.getAsProperty() + "> { " + 
//    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
//    			     annfilter + 
//    			     valFilter +
//    			     (confidence ? " OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?confidence . } " : "") +
//    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
//    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
//    			"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
//        		"    ?target <" + SOAVocabulary.onValue + "> ?value . BIND (str(?value) AS ?valueStr) . BIND (lang(?value) AS ?valueLang) " +
//        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//        		     (onlyFresh ? "FILTER NOT EXISTS  { ?target <" + DCTVocabulary.isReferencedBy + "> ?reference } . " : "") +
//        		"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " + 
//        		"    ?validation <" + SOAVocabulary.action + "> ?validationAction  . " +
//        		     (scope ? " OPTIONAL { ?validation <" + OAVocabulary.hasScope + "> ?scope } . " : "") +
//        		"  } } GROUP BY ?source ?body ?validationAction ?scope }";
    	
    	String whereSparqlUnvalidated = 
    			"WHERE { " +
    			"  GRAPH <" + resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString() + "> { " + 
    			"    ?source " + onPropertyString + " ?value }  " +
    			"  GRAPH <" + aeg.getAsProperty() + "> { " + 
    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
    			     annfilter + 
    			     valFilter +
    			     (created ? " OPTIONAL { ?annotation <" + DCTVocabulary.created + "> ?created . } " : "") +
    			     (confidence ? " OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?confidence . } " : "") +
    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
        		"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
        		"    ?target <" + SOAVocabulary.onValue + "> ?value . BIND (str(?value) AS ?valueStr) . BIND (lang(?value) AS ?valueLang) " +
        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
        		     (onlyFresh ? "FILTER NOT EXISTS  { ?target <" + DCTVocabulary.isReferencedBy + "> ?reference } . " : "") +
        		"    FILTER NOT EXISTS { ?annotation <" + SOAVocabulary.hasValidation + "> ?val . } " +
//        		     (creator ? " OPTIONAL { ?annotation <" + DCTVocabulary.creator + "> [ a ?ct ] } .  BIND (IF(bound(?ct), ?ct, <" + ASVocabulary.Application + ">) AS ?creatorType) . " : "") +
        		     (selector ? " OPTIONAL {?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?startV . BIND (STRDT(STR(?startV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?start) } " : "") +
        		     (selector ? " OPTIONAL {?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?endV . BIND (STRDT(STR(?endV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?end) } " : "") +
//        		     (scope ? " OPTIONAL {?annotation <" + OAVocabulary.hasScope + "> ?scope } . " : "") +
//        		     (scope ? " BIND (" + (defaultScope != null ? "IF(bound(?scope), ?scope, <" + defaultScope + ">)" : "?scope") + " AS ?aScope) " : "") +
        		"  } } ";
        		

    	if (format == SerializationType.TTL || format == SerializationType.NT || format == SerializationType.RDF_XML || format == SerializationType.JSONLD) {
    		
    		String sparqlValidated = 
			"  CONSTRUCT { " + 
			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
			     (created ? " ?annotation <" + DCTVocabulary.created + "> ?created . " : "") +
//			     (creator ? "?annotation <" + DCTVocabulary.creator + "> [ a ?creatorType ] . " : "" ) +
			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . " +
  		         (confidence ? " ?annotation <" + SOAVocabulary.confidence + "> ?confidence . " : "") +
			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    		     (selector ?  "?target <" + OAVocabulary.hasSelector + "> [" + (pathLength == 1 ? " a <" + SOAVocabulary.RDFPropertySelector + "> ; <" + SOAVocabulary.property + "> " + onPropertyStringAsPath + "" : " a <" + SOAVocabulary.RDFPathSelector + "> ; <" + SOAVocabulary.rdfPath + "> \"" + onPropertyStringAsPath + "\"" ) + " ; <" + SOAVocabulary.destination + "> [ a <" + SOAVocabulary.Literal + "> ; <" + RDFVocabulary.value + "> ?valueStr ; <" + DCTVocabulary.language + "> ?valueLang ] ; <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start ;  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end ] . " : "") +
    		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//    		     (state ? "?annotation <" + OAVocabulary.hasState + "> [ a <" + SOAVocabulary.ValidationState + "> ; <" + RDFVocabulary.value + "> ?validationState ] . " : "" ) +
    		     (state ? "?annotation <" + SOAVocabulary.hasReview + "> [ a <" + SOAVocabulary.Validation + "> ; <" + SOAVocabulary.recommendation + "> ?validationAction ] . " : "" ) +
//    		     (scope ? "?annotation <" + OAVocabulary.hasScope + "> ?aScope . " : "") +
    		     (scope ? "?annotation <" + OAVocabulary.hasScope + "> ?scope . " : "") +
    		 "   ?annotation <" + ASVocabulary.generator + "> ?generator . " +
    		" } " + whereSparqlValidated ;
    		
    		String sparqlValidatedCount = "SELECT DISTINCT ?annotation " + whereSparqlValidated ;
    		
//    		String sparqlValidatedGroup = 
//			"  CONSTRUCT { " + 
//			"    ?sampleAnnotation a <" + OAVocabulary.Annotation + ">  . " +
//			"    ?sampleAnnotation <" + OAVocabulary.hasBody + "> ?body . " +
//  		         (confidence ? " ?sampleAnnotation <" + SOAVocabulary.confidence + "> ?avgConfidence . " : "") +
//			"    ?sampleAnnotation <" + OAVocabulary.hasTarget + "> _:target . " + 
//    		"    _:target <" + OAVocabulary.hasSource + "> ?source . " +
//    		     (state ? "?sampleAnnotation <" + SOAVocabulary.hasReview + "> [ a <" + SOAVocabulary.Validation + "> ; <" + SOAVocabulary.recommendation + "> ?validationAction ] . " : "" ) +
//    		     (scope ? "?sampleAnnotation <" + OAVocabulary.hasScope + "> ?scope . " : "") +
//    		" } " + whereSparqlValidatedGroup ;
    		
    		String sparqlUnvalidated = 
			"  CONSTRUCT { " + 
			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
			     (created ? " ?annotation <" + DCTVocabulary.created + "> ?created . " : "") +
//			     (creator ? "?annotation <" + DCTVocabulary.creator + "> [ a ?creatorType ] . " : "" ) +
			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . " +
  	             (confidence ? " ?annotation <" + SOAVocabulary.confidence + "> ?confidence . " : "") +
			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
			     (selector ?  "?target <" + OAVocabulary.hasSelector + "> [" + (pathLength == 1 ? " a <" + SOAVocabulary.RDFPropertySelector + "> ; <" + SOAVocabulary.property + "> " + onPropertyStringAsPath + "" : " a <" + SOAVocabulary.RDFPathSelector + "> ; <" + SOAVocabulary.rdfPath + "> \"" + onPropertyStringAsPath + "\"" ) + " ; <" + SOAVocabulary.destination + "> [ a <" + SOAVocabulary.Literal + "> ; <" + RDFVocabulary.value + "> ?valueStr ; <" + DCTVocabulary.language + "> ?valueLang ] ; <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start ;  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end ] . " : "") +
    		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//    		     (state ? "?annotation <" + OAVocabulary.hasState + "> [ a <" + SOAVocabulary.ValidationState + "> ; <" + RDFVocabulary.value + "> ?validationState ] . " : "" ) +
//    		     (scope ? "?annotation <" + OAVocabulary.hasScope + "> ?aScope . " : "") +
            "    ?annotation <" + ASVocabulary.generator + "> ?generator . " +
    		" } " + whereSparqlUnvalidated ;
        	
    		String sparqlUnvalidatedCount = "SELECT DISTINCT ?annotation " + whereSparqlUnvalidated ;
//    		

			int counter = 0;
			
			String[] queries;
			String[] countQueries;
			
//			System.out.println(onlyReviewed + " " + onlyNonRejected);
			if (onlyReviewed) {
				queries = new String [ ] { sparqlValidated } ;
				countQueries = new String [ ] { sparqlValidatedCount } ;
			} else {
				queries = new String [ ] { sparqlValidated, sparqlUnvalidated } ;
				countQueries = new String [ ] { sparqlValidatedCount, sparqlUnvalidatedCount } ;
			}
//			queries = new String [ ] { sparqlValidatedGroup } ;
			
			JsonGenerator jsonGenerator = null;
			ByteArrayOutputStream jsonbos = null;
			ObjectMapper mapper = null;
			
			try {
				if (format == SerializationType.JSONLD) {
					jsonbos = new ByteArrayOutputStream();
					mapper = new ObjectMapper();
				}
	
//				int tc = 0;
				for (int i = 0; i < queries.length; i++) {
					String sparql = queries[i];
	
//					System.out.println(QueryFactory.create(sparql));
					
					// it is strange that constuct works in this way. in other similar construct queries the limit/offset works by counting triples 
					try (VirtuosoConstructIterator vs = new VirtuosoConstructIterator(vc.getSparqlEndpoint(), sparql, 300)) {
						
						Model model1 = ModelFactory.createDefaultModel();
						
						while (vs.hasNext()) {
	//						Model model1 = vs.next();
							
							Model modelx = vs.next();
							model1.add(modelx);
							
							//possible memory issues here split !
							
	//						System.out.println("-- " + modelx.size());
						}
						
//						tc += model1.listStatements(null, RDFVocabulary.type, OAVocabulary.Annotation).toList().size();
//						System.out.println(model1.listStatements(null, RDFVocabulary.type, OAVocabulary.Annotation).toList().size());
						model1.clearNsPrefixMap();
						
						UpdateRequest ur = UpdateFactory.create();
						
						// remove empty language fields
						ur.add("DELETE { ?x <" + DCTVocabulary.language + "> \"\" } WHERE { ?x <" + DCTVocabulary.language + "> \"\" }" );
						
						// rename recommendations
						ur.add("DELETE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Approve + "> } INSERT { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Accept + "> } WHERE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Approve + "> }");
						ur.add("DELETE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Delete + "> } INSERT { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Reject + "> } WHERE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Delete + "> }");
						
						// move start, end to refinedBy object 
						ur.add("DELETE { ?selector <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start . ?selector  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } INSERT { ?selector <" + OAVocabulary.refinedBy + "> [ a <" + OAVocabulary.TextPositionSelector + "> ; <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start ;  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end ] } WHERE { ?selector <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start . ?selector <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end }");
						
						//  add annotator uris 
						try (QueryExecution qe = QueryExecutionFactory.create("select distinct ?generator {?annotation <" + ASVocabulary.generator + "> ?generator }", model1)) {
							ResultSet qr = qe.execSelect();
							while (qr.hasNext()) {
								String generator = qr.next().get("generator").toString();
								if (resourceVocabulary.isAnnotator(generator)) {
									AnnotatorDocument adoc = annotatorRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(generator)).get();
									DataService ds = dsRepository.findByIdentifierAndType(adoc.getAnnotator(), DataServiceType.ANNOTATOR).get();
//										System.out.println(ds + " " + ds.getUri() + " " + ds.getTitle());
									if (ds.getUri() != null) {
										ur.add("INSERT { ?annotation <" + DCTVocabulary.creator + "> <" + ds.getUri() + "> . <" + ds.getUri() + "> a <" + ASVocabulary.Application + "> } WHERE { ?annotation <" + ASVocabulary.generator + "> <" + generator + "> }");
									}
									
									if (scope && adoc.getDefaultTarget() != null) {
										ur.add("INSERT { ?annotation <" + OAVocabulary.hasScope + "> <" + adoc.getDefaultTarget() + "> } WHERE { ?annotation <" + ASVocabulary.generator + "> <" + generator + "> . FILTER NOT EXISTS { ?annotation <" + OAVocabulary.hasScope + "> ?scope } } ");
									}
									
								} else if (resourceVocabulary.isAnnotationValidator(generator)) {
									ur.add("INSERT { ?annotation <" + DCTVocabulary.creator + "> [ a <" + FOAFVocabulary.Person + "> ] } WHERE { ?annotation <" + ASVocabulary.generator + "> <" + generator + "> }");
								}
							}
							
						}

						
						// delete sage generator
						ur.add("DELETE { ?annotation <" + ASVocabulary.generator + "> ?generator } WHERE { ?annotation <" + ASVocabulary.generator + "> ?generator }");

						// delete scope from rejected annotations
						ur.add("DELETE { ?annotation <" + OAVocabulary.hasScope + "> ?scope } WHERE { ?annotation <" + OAVocabulary.hasScope + "> ?scope . ?annotation <" + SOAVocabulary.hasReview + "> ?review . ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Reject + "> }");

						UpdateAction.execute(ur, model1);

			    		Model model = model1;
			    		
				        if (model.size() > 0) {
	
	//						try (QueryExecution qe = QueryExecutionFactory.create(sp, model1)) {
	//							model = qe.execConstruct();
	//							model.clearNsPrefixMap();
	//						}
	
							String suffix = "";
							
							try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
	
								if (format == SerializationType.TTL) {
									RDFDataMgr.write(bos, model, Lang.TURTLE);
									suffix = "ttl";
									
									bos.flush();
									writeStream(out, aeg.getUuid() + (counter == 0 ? "" : "_" + counter) + "." + suffix, bos.toByteArray());
									counter++;

								} else if (format == SerializationType.NT) {
									RDFDataMgr.write(bos, model, Lang.TURTLE);
									suffix = "nt";
									
									bos.flush();
									writeStream(out, aeg.getUuid() + (counter == 0 ? "" : "_" + counter) + "." + suffix, bos.toByteArray());
									counter++;

								} else if (format == SerializationType.RDF_XML ) {
									RDFDataMgr.write(bos, model, Lang.RDFXML);
									suffix = "xml";
									
									bos.flush();
									writeStream(out, aeg.getUuid() + (counter == 0 ? "" : "_" + counter) + "." + suffix, bos.toByteArray());
									counter++;

								} else if (format == SerializationType.JSONLD) {
									suffix = "jsonld";
									
		//							System.out.println(">>> " + annotationContext);
									
							       	Map<String, Object> frame = new HashMap<>();
							    	frame.put("@type" , "http://www.w3.org/ns/oa#Annotation");
							    	frame.put("@context", ((Map)annotationContext.get("annotation-jsonld-context")).get("@context")); // why ????
		
							        JsonLdOptions options = new JsonLdOptions();
							        options.setCompactArrays(true);
							        options.useNamespaces = true ; 
							        options.setUseNativeTypes(true); 	      
							        options.setOmitGraph(false);
							        options.setPruneBlankNodeIdentifiers(true);
								        
							        final RDFDataset jsonldDataset = (new JenaRDF2JSONLD()).parse(DatasetFactory.wrap(model).asDatasetGraph());
							        Object obj = (new JsonLdApi(options)).fromRDF(jsonldDataset, true);
		//						    
							        Map<String, Object> jn = JsonLdProcessor.frame(obj, frame, options);
							        
//							        mapper.writerWithDefaultPrettyPrinter().writeValue(bos, jn);
							        
									if (jsonGenerator == null) { // first json-ld part 
										JsonFactory jsonFactory = new JsonFactory();				
										jsonGenerator = jsonFactory.createGenerator(jsonbos, JsonEncoding.UTF8);
										jsonGenerator = jsonGenerator.useDefaultPrettyPrinter();
										jsonGenerator.writeStartObject();

										jsonGenerator.writeFieldName("@context");
										mapper.setDefaultPrettyPrinter(jsonGenerator.getPrettyPrinter());
										mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, jn.get("@context"));
										
										jsonGenerator.writeFieldName("@graph");
										jsonGenerator.writeStartArray();
										
									}
									
									for (Object element : (List)jn.get("@graph")) {
										mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, element);
									}
									
								}
							}
				        }						
					}
				}
				
				if (format == SerializationType.JSONLD) {
					jsonGenerator.writeEndArray();
					jsonGenerator.writeEndObject();
					jsonGenerator.flush();
					
					writeStream(out, aeg.getUuid() + ".jsonld", jsonbos.toByteArray());
				}
				
			} finally {
				if (jsonbos != null) {
					jsonbos.close();
				}
			}
				
		}

     		
//    	} else if (format == SerializationType.CSV) {
//    		
//        	String sparql = 
//        			"SELECT ?annotation " +
//                    (created ? "?created " : " ") +
//                    (score ? "?score " : " ") +
//                    "?body " +
//                    (selector ? "?start " : " ") +
//                    (selector ? "?end " : " ") +
//	                "?source " +
//	                whereSparqlUnvalidated ;
//    		
//			try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
//					Writer writer = new BufferedWriter(new OutputStreamWriter(bos));
//					CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Item", "AnnotationIRI", "AnnotationLiteral", "Score", "SourceLiteralLexicalForm", "SourceLiteralLanguage", "SourceProperty"));
//					ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
//				
//				try (VirtuosoSelectIterator vs = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {
//					while (vs.hasNext()) {
//						QuerySolution sol = vs.next();
//						Resource sid = sol.get("source").asResource();
//						RDFNode node1 = sol.get("body");
//	//					RDFNode node2 = sol.get("body2");
//						RDFNode scorev = sol.get("score");
//						RDFNode value = sol.get("value");
//	
//						List<Object> line = new ArrayList<>();
//						line.add(sid);
//						
//						if (node1.isResource()) {
//							line.add(node1.asResource());
//							line.add(null);
//						} else if (node1.isLiteral()) {
//							line.add(null);
//							line.add(NodeFactory.createLiteralByValue(Utils.escapeJsonTab(node1.asLiteral().getLexicalForm()), node1.asLiteral().getLanguage(), node1.asLiteral().getDatatype()));
//						}
//						
//						if (scorev != null) {
//							line.add(scorev.asLiteral().getDouble());
//						} else {
//							line.add(null);
//						}
//						
//						if (value != null) {
//							line.add(value.asLiteral().getLexicalForm());
//							line.add(value.asLiteral().getLanguage());
//							line.add(onPropertyString);
//						} else {
//							line.add(null);
//							line.add(null);
//							line.add(null);
//						}
//						
//	
//	//					if (node2 != null) {
//	//						if (node2.isResource()) {
//	//							line.add(node2.asResource());
//	//							line.add(null);
//	//						} else if (node2.isLiteral()) { 
//	//							line.add(null);
//	//							line.add(NodeFactory.createLiteralByValue(Utils.escapeJsonTab(node2.asLiteral().getLexicalForm()), node2.asLiteral().getLanguage(), node2.asLiteral().getDatatype()));
//	//						}
//	//					}
//						
//						csvPrinter.printRecord(line);
//					}
//					
//				}
//	
//				csvPrinter.flush();
//				bos.flush();
//				
//				try (ZipOutputStream zos = new ZipOutputStream(baos)) {
//					ZipEntry entry = new ZipEntry(aeg.getUuid() + ".csv");
//	
//					zos.putNextEntry(entry);
//					zos.write(bos.toByteArray());
//					zos.closeEntry();
//	
//				} catch (IOException ioe) {
//					ioe.printStackTrace();
//				}
//	
//				return new ByteArrayResource(baos.toByteArray());
//			}
//    	}
//    	
//    	return null;
	}
	
	private void writeStream(OutputStream out, String filename, byte[] bytes) throws Exception {
		if (out instanceof ZipOutputStream) {
			ZipEntry entry = new ZipEntry(filename);
			
			((ZipOutputStream)out).putNextEntry(entry);
			out.write(bytes);
			((ZipOutputStream)out).closeEntry();
		} else if (out instanceof TarArchiveOutputStream) {
			
			TarArchiveEntry tarEntry = new TarArchiveEntry(filename);
			tarEntry.setSize(bytes.length);
			
	        ((TarArchiveOutputStream)out).putArchiveEntry(tarEntry);
	        out.write(bytes);
	        ((TarArchiveOutputStream)out).closeArchiveEntry();
		}

	}
	
	
//	public List<Map<String, Object>> computeValidationDistribution(UserPrincipal currentUser, String id, int accuracy) throws Exception {
//
//		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(id));
//		if (!aegOpt.isPresent()) {
//			return null;
//		}		
//		
//		AnnotationEditGroup aeg = aegOpt.get();
//		
//		Optional<ac.software.semantic.model.Dataset> dopt = datasetRepository.findByUuid(aeg.getDatasetUuid());
//		TripleStoreConfiguration vc = dopt.get().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
//
//		List<String> generatorIds = new ArrayList<>();
//		generatorIds.addAll(annotatorRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(doc -> "<" + resourceVocabulary.getAnnotatorAsResource(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
//		generatorIds.addAll(pavRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> "<" + resourceVocabulary.getAnnotationValidatorAsResource(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
//		generatorIds.addAll(favRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> "<" + resourceVocabulary.getAnnotationValidatorAsResource(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
//
//		String annfilter = generatorFilter("annotation", generatorIds);
//		
//    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
//    	
//    	List<Map<String, Object>> result = new ArrayList<>();
//
//    	for (String validation : new String[] { SOAVocabulary.Delete.toString(), SOAVocabulary.Approve.toString() } ) {
//    		List<Distribution> distr = new ArrayList<>();
//    		
//	    	for (int i = 0; i < 100/accuracy; i++) {
//		    	String sparql = 
//		    			"SELECT (count(*) AS ?count) " +
//		    			"WHERE { " +
//		    			"  GRAPH <" + resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString() + "> { " + 
//		    			"    ?source " + onPropertyString + " ?value }  " +
//		    			"  GRAPH <" + aeg.getAsProperty() + "> { " + 
//		    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
//		    			     annfilter + 
//		    			"    ?annotation <" + SOAVocabulary.hasValidation + "> [ <" + SOAVocabulary.action + "> <" + validation + "> ] } " +
//		    			"    ?annotation <" + SOAVocabulary.score + "> ?score . " +
//		    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
//		    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
//		        		"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
//		        		"    ?target <" + SOAVocabulary.onValue + "> ?value . " +
//		        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//		        		"    FILTER ( ?score > " + i*accuracy/(double)100 + " && ?score <= " + (i + 1)*accuracy/(double)100 + ") " +
//		        		"  }  ";
//		    	
////		    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
//		    	
//		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//
//					ResultSet results = qe.execSelect();
//					
//					while (results.hasNext()) {
//						QuerySolution qs = results.next();
//						
//						Distribution d = new Distribution();
//						d.setCount(qs.get("count").asLiteral().getInt());
//						d.setLowerBound(i*accuracy/(double)100);
//						d.setLowerBoundIncluded(false);
//						d.setUpperBound((i + 1)*accuracy/(double)100);
//						d.setUpperBoundIncluded(true);
//						
//						distr.add(d);
//					}
//		    	}
//		    	
//	    	}
//	    	
//	    	Map<String, Object> map = new HashMap<>();
//	    	map.put("key", validation);
//	    	map.put("distribution", distr);
//	    	
//	    	result.add(map);
//    	}
// 
//    	return result;
//	}
	
	private TripleStoreConfiguration getPublishVirtuosoConfiguration(String datasetUuid) {
	
		Optional<ac.software.semantic.model.Dataset> dataset = datasetRepository.findByUuid(datasetUuid);
		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site    	
			if (dataset.get().checkPublishState(vc.getId()) != null) {
				return vc;
			}
		}
		
		return null;
	}
	

	public ValueResponseContainer<ValueAnnotation> view(UserPrincipal currentUser, String aegId, AnnotationValidationRequest mode, int page, String annotators) {
		
		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));
		if (!aegOpt.isPresent()) {
			return new ValueResponseContainer<>();
		}		
		
		AnnotationEditGroup aeg = aegOpt.get();

		TripleStoreConfiguration vc = getPublishVirtuosoConfiguration(aeg.getDatasetUuid());

//		String datasetUri = resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString();
		DatasetCatalog dcg = schemaService.asCatalog(aeg.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		
		String spath = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
    	

		String annfilter = annotatorFilter("v", annotatorRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(doc -> doc.getUuid()).collect(Collectors.toList()));

		String sparql = 
				"SELECT (count(?v) AS ?annCount) (count(DISTINCT ?source) AS ?sourceCount) (count(DISTINCT ?value) AS ?valueCount)" + 
		        "WHERE { " +  
		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
		        annfilter + 
			    " ?v <" + OAVocabulary.hasTarget + "> [ <" + OAVocabulary.hasSource + "> ?source ; <" + SOAVocabulary.onValue + "> ?value ] } ";
	
		int annCount = 0;
		int sourceCount = 0;
		int valueCount = 0;
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				
				annCount = sol.get("annCount").asLiteral().getInt();
				sourceCount = sol.get("sourceCount").asLiteral().getInt();
				valueCount = sol.get("valueCount").asLiteral().getInt();
			}
		}
		
//    	System.out.println("SCHEMA");
//    	System.out.println(uri);
//    	System.out.println(asUri);
//    	System.out.println(path);

		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
		vrc.setTotalCount(annCount);
		vrc.setDistinctSourceTotalCount(sourceCount);
		vrc.setDistinctValueTotalCount(valueCount);
		
		sparql = null;
		if (mode == AnnotationValidationRequest.ALL) {
			sparql = "SELECT ?value ?t ?ie ?start ?end ?score ?count " +
			        fromClause + 
			        "FROM NAMED <" + aeg.getAsProperty() + "> " + 
					"{ " +
//					"SELECT distinct ?value ?t ?ie ?start ?end (COUNT(?ac) AS ?acCount) (SAMPLE(?ac) AS ?action) (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count)" + 
					"SELECT distinct ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count) " +
			        "WHERE { " + 
				    "    ?s " + spath + " ?value . FILTER (isLiteral(?value)) .   " + 
			        "  OPTIONAL { GRAPH <" + aeg.getAsProperty() + "> { " + 
				    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
			        "     <" + OAVocabulary.hasTarget + "> ?r . " + 
				    annfilter + 
				    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
				    " { ?v <" + OAVocabulary.hasBody + "> [ " + 
				    " a <" + OWLTime.DateTimeInterval + "> ; " + 
				    " <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
				    " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " + 
				    "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
				    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
				    "     <" + OAVocabulary.hasSource + "> ?s . " +
//				    " OPTIONAL { ?v <" + SOAVocabulary.hasValidation + "> ?validation . ?validation <" + SOAVocabulary.action + "> ?ac } . " +
                    " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +					
				    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
				    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " + " } } }" + 
				    "GROUP BY ?t ?ie ?value ?start ?end " +
					// "ORDER BY DESC(?count) ?value ?start ?end LIMIT 50 OFFSET " + 50*(page - 1);
					"ORDER BY desc(?count) ?value ?start ?end " +
					"} LIMIT 50 OFFSET " + 50 * (page - 1);
		} else if (mode == AnnotationValidationRequest.ANNOTATED_ONLY) {
			sparql = 
					"SELECT ?value ?t ?ie ?start ?end ?score ?count " + 
			        fromClause + 
			        "FROM NAMED <" + aeg.getAsProperty() + "> " + 
					"{ " +
//			        "SELECT distinct ?value ?t ?ie ?start ?end (COUNT(?ac) AS ?acCount) (SAMPLE(?ac) AS ?action) (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count)" + 
			        "SELECT distinct ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count) " +
			        "WHERE { " + 
		            "    ?s " + spath + " ?value  . " + 
					"  GRAPH <" + aeg.getAsProperty() + "> { " + 
		            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
					"     <" + OAVocabulary.hasTarget + "> ?r . " + 
		            annfilter + 
		            " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
		            " { ?v <" + OAVocabulary.hasBody + "> [ " + 
		            " a <" + OWLTime.DateTimeInterval + "> ; " + 
		            " <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
		            " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " + 
		            "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
		            "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		            "     <" + OAVocabulary.hasSource + "> ?s . " +
//		            " OPTIONAL { ?v <" + SOAVocabulary.hasValidation + "> ?validation . ?validation <" + SOAVocabulary.action + "> ?ac } . " +
                    " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +					
		            " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
		            " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " + "   } } " + 
		            "GROUP BY ?t ?ie ?value ?start ?end " +
					// "ORDER BY DESC(?count) ?value ?start ?end LIMIT 50 OFFSET " + 50*(page - 1);
					"ORDER BY desc(?count) ?value ?start ?end " +
					"} LIMIT 50 OFFSET " + 50 * (page - 1);
		} else if (mode == AnnotationValidationRequest.UNANNOTATED_ONLY) {
			sparql = 
					"SELECT ?value ?count " +
			        fromClause + 
			        "FROM NAMED <" + aeg.getAsProperty() + "> " + 
					"{ " +
			
			        "SELECT distinct ?value (count(distinct ?s) AS ?count) " +
					"WHERE { " + 
					"    ?s " + spath + " ?value . FILTER (isLiteral(?value)) .  " +
		            " FILTER NOT EXISTS { GRAPH <" + aeg.getAsProperty() + "> { " + 
					"  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "     <" + OAVocabulary.hasTarget + "> ?r . " + 
					annfilter +
					"  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
					"     <" + SOAVocabulary.onValue + "> ?value ; " + 
					"     <" + OAVocabulary.hasSource + "> ?s  } } }" + 
					"GROUP BY ?value  " + 
					"ORDER BY desc(?count) ?value " + 
					"} LIMIT 50 OFFSET " + 50 * (page - 1);
		}    	

//		System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

		//grouping does not work well with paging!!! annotations of some group may be split in different pages
		//it should be fixed somehow;
		//also same blank node annotation are repeated
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
		
			ResultSet rs = qe.execSelect();
			
			AnnotationEditValue prev = null;
			ValueAnnotation va = null;
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				
				RDFNode value = sol.get("value");
				
				String ann = sol.get("t") != null ? sol.get("t").toString() : null;
				Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
				Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
				int count = sol.get("count").asLiteral().getInt();
				Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
				
//				Resource validation = (Resource)sol.get("action");
				
				String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;

				AnnotationEditValue aev = null;
				if (value.isResource()) {
					aev = new AnnotationEditValue(value.asResource());
				} else if (value.isLiteral()) {
					aev = new AnnotationEditValue(value.asLiteral());
				}
				
//				System.out.println(validation + " " + sol.get("acCount"));
				
				if (!aev.equals(prev)) {
					if (prev != null) {
						res.put(prev, va);
					}

					prev = aev;
					
					va = new ValueAnnotation();
					va.setOnValue(aev);
					va.setCount(count);
						
					if (ann != null) {
						ValueAnnotationDetail vad  = new ValueAnnotationDetail();
						vad.setValue(ann);
						vad.setValue2(ie);
						vad.setStart(start);
						vad.setEnd(end);
//						vad.setCount(count);
						
						vad.setScore(score);
						
//						if (validation != null) {
//							int valCount =  sol.get("acCount").asLiteral().getInt();
//							if (valCount == 1) {
//								if (validation == SOAVocabulary.Add || validation == SOAVocabulary.Approve) {
//									vad.setValidation(ValidationType.APPROVE);
//								} else if (validation == SOAVocabulary.Delete) {
//									vad.setValidation(ValidationType.DELETE);
//								}
//							} // else multiple possibly conficting
// 						}
						
						va.getDetails().add(vad);
					}
				} else {
					ValueAnnotationDetail vad  = new ValueAnnotationDetail();
					vad.setValue(ann);
					vad.setValue2(ie);
					vad.setStart(start);
					vad.setEnd(end);
//					vad.setCount(count);
					
					vad.setScore(score);
					
//					if (validation != null) {
//						int valCount =  sol.get("acCount").asLiteral().getInt();
//						if (valCount == 1) {
//							if (validation == SOAVocabulary.Add || validation == SOAVocabulary.Approve) {
//								vad.setValidation(ValidationType.APPROVE);
//							} else if (validation == SOAVocabulary.Delete) {
//								vad.setValidation(ValidationType.DELETE);
//							}
//						} // else multiple possibly conficting
//					}
					
					va.getDetails().add(vad);
				}
				
			}
			if (prev != null) {
				res.put(prev, va);
			}
		}
		
		// is this needed ? AnnotationEdit do not have any more editType
//		for (Map.Entry<AnnotationEditValue, ValueAnnotation> entry : res.entrySet()) {
//			AnnotationEditValue aev = entry.getKey();
//			ValueAnnotation nva = entry.getValue();
//			
//			List<AnnotationEdit> edits = null;
//			if (aev.getIri() != null) {
//				edits = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserIdAndIriValue(aeg.getDatasetUuid(), aeg.getOnProperty(), aeg.getAsProperty(), new ObjectId(currentUser.getId()), aev.getIri());
//			} else {
//				edits = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserIdAndLiteralValue(aeg.getDatasetUuid(), aeg.getOnProperty(), aeg.getAsProperty(), new ObjectId(currentUser.getId()), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype());
//			}
//			
//			  
//			for (AnnotationEdit edit : edits) {
//
//				if (edit.getEditType() == AnnotationEditType.REJECT) {
//					for (ValueAnnotationDetail vad : nva.getDetails()) {
//						if (vad.getValue().equals(edit.getAnnotationValue())) {
////							vad.setId(edit.getId().toString());
//							vad.setState(AnnotationEditType.REJECT);
////							break;
//						}
//					}
//				} else if (edit.getEditType() == AnnotationEditType.ACCEPT) {
//					for (ValueAnnotationDetail vad : nva.getDetails()) {
//						if (vad.getValue().equals(edit.getAnnotationValue())) {
////							vad.setId(edit.getId().toString());
//							vad.setState(AnnotationEditType.ACCEPT);
////							break;
//						}
//					}
//				} else if (edit.getEditType() == AnnotationEditType.ADD) {
////					ValueAnnotationDetail vad = new ValueAnnotationDetail(edit.getAnnotationValue(), edit.getAnnotationValue(), -1, -1);
//					ValueAnnotationDetail vad  = new ValueAnnotationDetail();
//					vad.setValue(edit.getAnnotationValue());
//					vad.setStart(edit.getStart() != -1 ? edit.getStart() : null );
//					vad.setEnd(edit.getEnd() != -1 ? edit.getEnd() : null);
//					
////					vad.setId(edit.getId().toString());
//					vad.setState(AnnotationEditType.ADD);
//					
//					nva.getDetails().add(vad);
//				}
//			}
//		}
		
		vrc.setValues(new ArrayList<>(res.values()));
		
		return vrc;
    } 
	
//	public boolean createPagedAnnotationValidation(UserPrincipal currentUser, String virtuoso, String aegId) {
//
//		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));
//		if (!aegOpt.isPresent()) {
//			return false;
//		}		
//
//		// temporary: do not create more that one pavs for an aeg;
//		List<PagedAnnotationValidation> pavList = pavRepository.findByAnnotationEditGroupId(new ObjectId(aegId));
//		if (pavList.size() > 0) {
//			return false;
//		}		
//
//		PagedAnnotationValidation pav = null;
//		
//		try {
//			AnnotationEditGroup aeg = aegOpt.get();
//	
//			pav = new PagedAnnotationValidation();
//			pav.setUserId(new ObjectId(currentUser.getId()));
//			pav.setAnnotationEditGroupId(aeg.getId());
//	
//			String datasetUri = resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString();
//			String spath = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
//
//			logger.info("Starting paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ".");
//
//			String annotatedCountSparql =
//					"SELECT (count(DISTINCT ?value) AS ?count)" +
//			        "WHERE { " +
//					"  GRAPH <" + datasetUri + "> { " +
//			        "    ?s " + spath + " ?value }  " +
//					"  GRAPH <" + aeg.getAsProperty() + "> { " +
//			        "  ?v a <" + OAVocabulary.Annotation + "> ; " +
//					"     <" + OAVocabulary.hasTarget + "> ?r . " +
//			        "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
//			        "     <" + SOAVocabulary.onValue + "> ?value ; " +
//			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } ";
//
//			int annotatedValueCount = 0;
//
//	//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));
//
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(),
//					QueryFactory.create(annotatedCountSparql, Syntax.syntaxSPARQL_11))) {
//
//				ResultSet rs = qe.execSelect();
//
//				while (rs.hasNext()) {
//					QuerySolution sol = rs.next();
//					annotatedValueCount = sol.get("count").asLiteral().getInt();
//				}
//			}
//
//			int annotatedPages = annotatedValueCount / pageSize + (annotatedValueCount % pageSize > 0 ? 1 : 0);
//
//			String nonAnnotatedCountSparql =
//					"SELECT (count(DISTINCT ?value) AS ?count)" +
//			        "WHERE { " +
//					"  GRAPH <" + datasetUri + "> { " +
//			        "    ?s " + spath + " ?value }  " +
//					"  FILTER NOT EXISTS {GRAPH <" + aeg.getAsProperty() + "> { " +
//			        "  ?v a <" + OAVocabulary.Annotation + "> ; " +
//					"     <" + OAVocabulary.hasTarget + "> ?r . " +
//			        "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
//			        "     <" + SOAVocabulary.onValue + "> ?value ; " +
//			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } }";
//
//			int nonAnnotatedValueCount = 0;
//
//	//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));
//
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(),
//					QueryFactory.create(nonAnnotatedCountSparql, Syntax.syntaxSPARQL_11))) {
//
//				ResultSet rs = qe.execSelect();
//
//				while (rs.hasNext()) {
//					QuerySolution sol = rs.next();
//					nonAnnotatedValueCount = sol.get("count").asLiteral().getInt();
//				}
//			}
//
//			int nonAnnotatedPages = nonAnnotatedValueCount / pageSize + (nonAnnotatedValueCount % pageSize > 0 ? 1 : 0);
//
//			logger.info("Paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ": valueCount=" + annotatedValueCount + "/" + nonAnnotatedValueCount + " pages=" + annotatedPages + "/" + nonAnnotatedPages);
//
//			pav.setPageSize(pageSize);
//			pav.setAnnotatedPagesCount(annotatedPages);
//			pav.setNonAnnotatedPagesCount(nonAnnotatedPages);
//
//			pav = pavRepository.save(pav);
//
//			// temporary: do not populate pages
//			boolean createPages = false;
//
//			if (createPages) {
//				int totalCount = 0;
//
//				for (int i = 1; i <= nonAnnotatedPages; i++) {
//					String subsparql =
//							"SELECT ?value (count(*) AS ?valueCount)" +
//				            "WHERE { " +
//						    "  GRAPH <" + datasetUri + "> { " +
//				            "    ?s " + spath + " ?value }  " +
//						    "  GRAPH <" + aeg.getAsProperty() + "> { " +
//				            "  ?v a <" + OAVocabulary.Annotation + "> ; " +
//						    "     <" + OAVocabulary.hasTarget + "> ?r . " +
//				            "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
//						    "     <" + SOAVocabulary.onValue + "> ?value ; " +
//				            "     <" + OAVocabulary.hasSource + "> ?s . " + " } }  " +
//						    "GROUP BY ?value " +
//				            "ORDER BY desc(?valueCount) ?value " +
//						    "LIMIT " + pageSize + " OFFSET " + pageSize * (i - 1);
//
//			    	StringBuffer values = new StringBuffer();
//		//	    	System.out.println(QueryFactory.create(subsparql, Syntax.syntaxSPARQL_11));
//
//			    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(subsparql, Syntax.syntaxSPARQL_11))) {
//
//			    		ResultSet rs = qe.execSelect();
//
//			    		while (rs.hasNext()) {
//			    			QuerySolution qs = rs.next();
//			    			RDFNode value = qs.get("value");
//			    			if (value.isLiteral()) {
//			    				Literal l = value.asLiteral();
//			    				String lf = l.getLexicalForm();
//
//			    				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
//		//
//			    				values.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString() + " ");
//			    			} else {
//			    				values.append("<" + value.toString() + "> ");
//			    			}
//			    		}
//			    	}
//
//		//			SPLIT INTO TWO QUERIES : MUCH FASTER !!!!
//
//					String sparql =
//							"SELECT ?value ?t ?ie  " +
//					        "WHERE { " + "  GRAPH <" + datasetUri + "> { " +
//							"    ?s " + spath + " ?value }  " +
//					        "   GRAPH <" + aeg.getAsProperty() + "> { " +
//							"  ?v a <" + OAVocabulary.Annotation + "> ; " +
//					        "     <" + OAVocabulary.hasTarget + "> ?r . " +
//							" { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " +
//					        " { ?v <" + OAVocabulary.hasBody + "> [ " + " a <" + OWLTime.DateTimeInterval + "> ; " +
//							" <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " +
//					        " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " +
//							"  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
//					        "     <" + SOAVocabulary.onValue + "> ?value ; " +
//							"     <" + OAVocabulary.hasSource + "> ?s . " + " } " +
//		//			        " { " + subsparql  + " }  }";
//		                    " VALUES ?value { " + values.toString()  + " }  }";
//
//		//			System.out.println(sparql);
//		//			System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
//
//					int localCount = 0;
//					try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(),
//							QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
//						ResultSet rs = qe.execSelect();
//
//						while (rs.hasNext()) {
//							rs.next();
//							localCount++;
//						}
//					}
//
//					totalCount += localCount;
//
//					logger.info("Paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ": page=" + i + "/" + annotatedPages + "count=" + localCount + "/" + totalCount);
//
//					PagedAnnotationValidationPage pavp = new PagedAnnotationValidationPage();
//					pavp.setPagedAnnotationValidationId(pav.getId());
////					pavp.setAnnotations(localCount);
//					
//					pavpRepository.save(pavp);
//				}
//	//			System.out.println(count);
//			}
//			
//			logger.info("Finished paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ".");
//	
//			return true;
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			
//			if (pav != null) {
//				pavRepository.deleteById(pav.getId());
//			}
//			
//			return false;
//		}
//	}
	
	public String annotatorFilter(String var, List<String> annotatorUuids) {
		String annfilter = "";
		
		for (String uuid : annotatorUuids) {
			annfilter += "<" + resourceVocabulary.getAnnotatorAsResource(uuid).toString() + "> ";
		}
	
		if (annfilter.length() > 0) {
			annfilter = "?" + var + " <" + ASVocabulary.generator + "> ?generator . VALUES ?generator { " + annfilter + " } . ";
		}
		
		return annfilter;
	}
	
//	//does not work needs updating with OutputHandler
//	public boolean execute(UserPrincipal currentUser, String aegId, ApplicationEventPublisher applicationEventPublisher) throws Exception {
//
//		Optional<AnnotationEditGroup> odoc = aegRepository.findById(aegId);
//	    if (!odoc.isPresent()) {
//	    	return false;
//	    }
//    	
//	    AnnotationEditGroup aeg = odoc.get();
//
//	    Date executeStart = new Date(System.currentTimeMillis());
//	    
//    	ExecuteState es = aeg.getExecuteState(fileSystemConfiguration.getId());
//    	
//		// Clearing old files
//		if (es.getExecuteState() == MappingState.EXECUTED) {
//			for (int i = 0; i < es.getExecuteShards(); i++) {
//				(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_add"
//						+ (i == 0 ? "" : "_#" + i) + ".trig")).delete();
//			}
//			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_add_catalog.trig").delete();
//			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete.trig").delete();
//			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete_catalog.trig").delete();
//		}
//		
//		es.setExecuteState(MappingState.EXECUTING);
//		es.setExecuteStartedAt(executeStart);
//		es.setExecuteShards(0);
//		es.setCount(0);
//		
//		aegRepository.save(aeg);
//
//		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(
//				fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder, aeg.getId().toString() + "_add",
//				shardSize);
//				Writer delete = new OutputStreamWriter(new FileOutputStream(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete.trig"), false), StandardCharsets.UTF_8);
//				Writer deleteCatalog = new OutputStreamWriter(new FileOutputStream(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete_catalog.trig"), false), StandardCharsets.UTF_8)				
//				) {
//			
//			Executor exec = new Executor(outhandler, safeExecute);
//			exec.keepSubjects(true);
//			
//			try (ExecuteMonitor em = new ExecuteMonitor("annotation-edit", aegId, null, applicationEventPublisher)) {
//				exec.setMonitor(em);
//				
//				String d2rml = env.getProperty("annotator.manual.d2rml"); 
//				InputStream inputStream = resourceLoader.getResource("classpath:"+ d2rml).getInputStream();
//				D2RMLModel rmlMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
//
//				Dataset ds2 = DatasetFactory.create();
//				Model deleteModel2 = ds2.getDefaultModel();
//		
//				for (AnnotationEdit edit :  annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(aeg.getDatasetUuid(), aeg.getOnProperty(), aeg.getAsProperty(), new ObjectId(currentUser.getId()))) {
//					String prop = "";
//					for (int i = aeg.getOnProperty().size() - 1; i >= 0; i--) {
//						if (i < aeg.getOnProperty().size() - 1) {
//							prop += "/";
//						}
//						prop += "<" + aeg.getOnProperty().get(i) + ">";
//					}
//					
////					System.out.println(edit.getEditType() + " " + edit.getAnnotationValue());
//					
//					if (edit.getEditType() == AnnotationEditType.ADD) {
//						
//						Map<String, Object> params = new HashMap<>();
//						params.put("iigraph", SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString());
//						params.put("iiproperty", prop);
//						params.put("iivalue", edit.getOnValue().toString());
//						params.put("iiannotation", edit.getAnnotationValue());
//						params.put("iirdfsource", virtuosoConfiguration.getSparqlEndpoint());
////						params.put("iiannotator", SEMAVocabulary.getAnnotatorEditGroup(aeg.getUuid()));
//
////						System.out.println(edit.getOnValue().toString());
//						exec.partialExecute(rmlMapping, params);
//						
//					} else if (edit.getEditType() == AnnotationEditType.REJECT) {
//	
////						System.out.println(edit.getOnValue().toString());
//				    	String sparql = 
//				    			"CONSTRUCT { " + 
//					            "  ?annId ?p1 ?o1 ." + 
//					            "  ?o1 ?p2 ?o2 .  " +  
//		     			        "} WHERE { " + 
//		    			        "  GRAPH <" + aeg.getAsProperty() + "> { " + 
//		    			        "   ?annId <http://www.w3.org/ns/oa#hasTarget> [ " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onProperty> \"" + prop + "\" ; " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onValue> " + edit.getOnValue().toString() + " ; " +
//		    			        "     <http://www.w3.org/ns/oa#hasSource> ?s ] . " +
//		    		            "   ?annId ?p1 ?o1 . " +
//		    		            "   OPTIONAL { ?o1 ?p2 ?o2 } } . " +	    			        
//		    			        " GRAPH <" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + "> { " +
//		    			        "  ?s a ?type } " +
//		                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
//		                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annId . } " +		    			        
//		    			        "}";
//		    	
//				    	Writer sw = new StringWriter();
//				    	
////				    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
//				    	
//				    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//					    	Model model = qe.execConstruct();
//					    	model.setNsPrefixes(new HashMap<>());
//					    	
//							RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_FLAT) ;
//				    	}
//				    	
//						Dataset ds = DatasetFactory.create();
//						Model deleteModel = ds.getDefaultModel();
//	
//						deleteModel.read(new StringReader(sw.toString()), null, "JSON-LD");
//	//	
//						RDFDataMgr.write(delete, deleteModel, RDFFormat.TRIG);
//						delete.write("\n");
//						
//						String sparql2 = 
//				    			"CONSTRUCT { " + 
//					            "  ?adocid <http://purl.org/dc/terms/hasPart> ?annId . " + 
//		     			        "} WHERE { " + 
//		    			        "  GRAPH <" + aeg.getAsProperty() + "> { " + 
//		    			        "   ?annId <http://www.w3.org/ns/oa#hasTarget> [ " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onProperty> \"" + prop + "\" ; " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onValue> " + edit.getOnValue().toString()  + " ; " +
//		    			        "     <http://www.w3.org/ns/oa#hasSource> ?s ] . }" +
//		    			        " GRAPH <" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + "> { " +
//		    			        "  ?s a ?type } " +
//		                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
//		                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annId . } " +		    			        
//		    			        "}";
//	
//				    	Writer sw2 = new StringWriter();
//	
//				    	try (QueryExecution qe2 = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(), QueryFactory.create(sparql2, Syntax.syntaxARQ))) {
//					    	Model model2 = qe2.execConstruct();
//					    	model2.setNsPrefixes(new HashMap<>());
//					    	
////					    	System.out.println(model2);
//					    	
//							RDFDataMgr.write(sw2, model2, RDFFormat.JSONLD_EXPAND_FLAT) ;
//				    	}
//				    	
//						deleteModel2.read(new StringReader(sw2.toString()), null, "JSON-LD");
//		    		}
//					
//				}
//				exec.completeExecution();
//					
//				RDFDataMgr.write(deleteCatalog, deleteModel2, RDFFormat.TRIG);
//	//			deleteCatalog.write("\n");
//		
//				String asetId = UUID.randomUUID().toString();
//		    	
//				Set<Resource> subjects = exec.getSubjects();
//				
//	        	try (Writer sw = new OutputStreamWriter(new FileOutputStream(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_add_catalog.trig"), false), StandardCharsets.UTF_8)) {
//	//	        		sw.write("<" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + ">\n");
//	        		sw.write("<" + SEMAVocabulary.getAnnotationSet(asetId).toString() + ">\n");
//	        		sw.write("        <http://purl.org/dc/terms/hasPart>\n" );
//	        		sw.write("                " );
//	        		int c = 0;
//	        		for (Resource r : subjects) {
//	        			if (c++ > 0) {
//	        				sw.write(" , ");
//	        			}
//	        			sw.write("<" + r.getURI() + ">");
//	        		}
//	        		sw.write(" .");
//	    		}
//	        	
//				Date executeFinish = new Date(System.currentTimeMillis());
//					
//				es.setExecuteCompletedAt(executeFinish);
//				es.setExecuteState(MappingState.EXECUTED);
//				es.setExecuteShards(outhandler.getShards());
////				es.setCount(outhandler.getTotalItems());
//				es.setCount(subjects.size());
//					
//				aegRepository.save(aeg);
//		
////				RDFJenaConnection conn = (RDFJenaConnection)ts.getConnection();
////		
////		    	conn.saveAsTRIG(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder, aeg.getId().toString() + "_add");
//		
//					
//		    	
//		        	
//		//	        	try (Writer sw = new OutputStreamWriter(new FileOutputStream(new File(annotationsFolder + aeg.getId().toString() + "_delete_catalog.trig"), false), StandardCharsets.UTF_8)) {
//		////	        		sw.write("<" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + ">\n");
//		//	        		sw.write("        <http://purl.org/dc/terms/hasPart>\n" );
//		//	        		sw.write("                " );
//		//	        		int c = 0;
//		//	        		for (Resource r : deleteSubjects) {
//		//	        			if (c++ > 0) {
//		//	        				sw.write(" , ");
//		//	        			}
//		//	        			sw.write("<" + r.getURI() + ">");
//		//	        		}
//		//	        		sw.write(" .");
//		//	    		}
//					      
//				SSEController.send("edits", applicationEventPublisher, this, new NotificationObject("execute",
//						MappingState.EXECUTED.toString(), aegId, null, executeStart, executeFinish, subjects.size()));
//
//				logger.info("Annotation edits executed -- id: " + aegId + ", shards: " + outhandler.getShards());
//
////				try {
////					zipExecution(currentUser, adoc, outhandler.getShards());
////				} catch (Exception ex) {
////					ex.printStackTrace();
////					
////					logger.info("Zipping annotator execution failed -- id: " + id);
////				}
//				
//				return true;
//				
//			} catch (Exception ex) {
//				ex.printStackTrace();
//				
//				logger.info("Annotation edits failed -- id: " + aegId);
//				
//				exec.getMonitor().currentConfigurationFailed();
//
//				throw ex;
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//
//			es.setExecuteState(MappingState.EXECUTION_FAILED);
//
//			SSEController.send("edits", applicationEventPublisher, this,
//					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), aegId, null, null, null));
//
//			aegRepository.save(aeg);
//
//			return false;
//		}
//	}
//	
//	public boolean publish(UserPrincipal currentUser, String id) throws Exception {
//		
//		Optional<AnnotationEditGroup> doc = aegRepository.findById(new ObjectId(id));
//	
//		if (doc.isPresent()) {
//			AnnotationEditGroup adoc = doc.get();
//			
//			PublishState ps = adoc.getPublishState(virtuosoConfiguration.getDatabaseId());
//		
//			ps.setPublishState(DatasetState.PUBLISHING);
//			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			
//			aegRepository.save(adoc);
//			
//			List<AnnotationEdit> deletes = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndEditTypeAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), AnnotationEditType.REJECT, adoc.getUserId());
//		
//			virtuosoJDBC.publish(currentUser, adoc, deletes);
//	    	
//			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//			ps.setPublishState(DatasetState.PUBLISHED);
//			
//			aegRepository.save(adoc);
//		}
//		
//		System.out.println("PUBLICATION COMPLETED");
//		
//		return true;
//	}
//	
//	public boolean unpublish(UserPrincipal currentUser, String aegId) throws Exception {
//		
//		Optional<AnnotationEditGroup> doc = aegRepository.findById(new ObjectId(aegId));
//	
//		if (doc.isPresent()) {
//			AnnotationEditGroup adoc = doc.get();
//			
//			PublishState ps = adoc.getPublishState(virtuosoConfiguration.getDatabaseId());
//		
//			ps.setPublishState(DatasetState.UNPUBLISHING);
//			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			
//			aegRepository.save(adoc);
//			
//			List<AnnotationEdit> adds = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndEditTypeAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), AnnotationEditType.ADD, adoc.getUserId());
//		
//			virtuosoJDBC.unpublish(currentUser, adoc, adds);
//	    	
//			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//			ps.setPublishState(DatasetState.UNPUBLISHED);
//			
//			aegRepository.save(adoc);
//		}
//		
//		System.out.println("UNPUBLICATION COMPLETED");
//		
//		return true;
//	}
//	
//	public Optional<String> getLastExecution(UserPrincipal currentUser, String aegId) throws Exception {
//		Optional<AnnotationEditGroup> entry = aegRepository.findById(new ObjectId(aegId));
//		
//		if (entry.isPresent()) {
//			AnnotationEditGroup doc = entry.get();
//      	
//			StringBuffer result = new StringBuffer();
//			
//			result.append(">> ADD    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//			result.append(new String(Files.readAllBytes(Paths.get(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + doc.getId().toString() + "_add.trig"))));
//			result.append("\n");
//			result.append(">> DELETE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//			result.append(new String(Files.readAllBytes(Paths.get(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + doc.getId().toString() + "_delete.trig"))));
//
//			return Optional.of(result.toString());
//		} else {
//			return Optional.empty();
//		}
//	}
	
	
}
