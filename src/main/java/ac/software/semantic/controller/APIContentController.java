package ac.software.semantic.controller;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;

import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipOutputStream;

import ac.software.util.PointSerializer;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.lucene.analysis.Analyzer;
import org.bson.types.ObjectId;
import org.locationtech.jts.geom.Point;
//import org.ehcache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationExportPublicTaskData;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ComparatorDocument;
import ac.software.semantic.model.ComputationResultWrapper;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.model.PublicTask;
import ac.software.semantic.model.RDFMediaType;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.PublicTaskType;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.model.expr.ComputationResult;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.response.ListElement;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.ComparatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.DistributionDocumentRepository;
import ac.software.semantic.repository.core.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.core.IndexDocumentRepository;
import ac.software.semantic.repository.core.IndexStructureRepository;
import ac.software.semantic.repository.core.MappingDocumentRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.repository.core.PublicTaskRepository;
import ac.software.semantic.repository.core.UserRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService;
import ac.software.semantic.service.AnnotatorService;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.ContentService;
import ac.software.semantic.service.DistributionService;
import ac.software.semantic.service.DistributionService.DistributionContainer;
import ac.software.semantic.service.IdentifiersService;
import ac.software.semantic.service.IdentifiersService.GraphLocation;
import ac.software.semantic.service.MappingService;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.SPARQLService;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import ac.software.semantic.service.SchemaService;
import ac.software.semantic.service.SideSpecificationDocument;
import ac.software.semantic.service.FolderService;
import ac.software.semantic.vocs.SEMRVocabulary;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchPhrasePrefixQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchPhraseQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import edu.ntua.isci.ac.common.db.rdf.RDFJenaResults;
import edu.ntua.isci.ac.common.db.rdf.VectorDatatype;
import edu.ntua.isci.ac.d2rml.vocabulary.lucene.LuceneFunctions;
import io.swagger.v3.oas.annotations.tags.Tag;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

@Tag(name = "Content API")

@RestController
@RequestMapping("/api/content")
public class APIContentController {

	Logger logger = LoggerFactory.getLogger(APIContentController.class);

    @Value("${backend.server}")
    private String server;
    
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    @Autowired
    private IndexStructureRepository indexStructureRepository;
    
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Autowired
	private ContentService contentService;

	@Autowired
	private MappingDocumentRepository mappingsRepository;

	@Autowired
	private PrototypeDocumentRepository prototypeRepository;

	@Autowired
	private ComparatorDocumentRepository comparatorRepository;

	@Autowired
	private DatasetRepository datasetsRepository;

	@Autowired
	private AnnotationEditGroupRepository aegRepository;

	@Autowired
	private PagedAnnotationValidationRepository pavRepository;

	@Autowired
	private FilterAnnotationValidationRepository favRepository;

	@Autowired
	private AnnotatorDocumentRepository adocRepository;

	@Autowired
	private IndexDocumentRepository indexDocumentRepository;

	@Autowired
	private PublicTaskRepository publicTaskRepository;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private IdentifiersService idService;

	@Autowired
	private FolderService folderService;

	@Autowired
	private SPARQLService sparqlService;

	@Autowired
	private AnnotationEditGroupService aegService;

	@Autowired
	private DistributionDocumentRepository distributionRepository;

	@Autowired
	private DistributionService distributionService;

	@Autowired
	private MappingService mappingsService;

	@Autowired
	private AnnotatorService annotatorService;

	@Autowired
	private APIUtils apiUtils;

//	@Autowired
//	@Qualifier("all-datasets")
//	private VocabulariesMap vm;
	
//	@Autowired
//	@Qualifier("endpoints-cache")
//	private Cache endpointsCache;

	@Autowired
	@Qualifier("indices-cache")
	private Cache indicesCache;

	@Autowired
	@Qualifier("api-cache")
	private org.ehcache.Cache<String, Map> apiCache;
	
    @GetMapping(value = "/{dataset-identifier}/mapping/{uuid}", produces = "text/turtle; charset=UTF-8")
 	public ResponseEntity<?> getMappingContent(@PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("uuid") String uuid)  {
    	return igetMappingContent(null, null, datasetIdentifier, uuid);
 	}

    @GetMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/mapping/{uuid}", produces = "text/turtle; charset=UTF-8")
 	public ResponseEntity<?> getMappingContent(@PathVariable String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("uuid") String uuid)  {
    	return igetMappingContent(pu, puIdentifier, datasetIdentifier, uuid);
 	}
    
    @GetMapping(value = "/{dataset-identifier}/prototype/{uuid}", produces = "text/turtle; charset=UTF-8")
 	public ResponseEntity<?> getPrototypeContent(@PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("uuid") String uuid)  {
    	return igetPrototypeContent(null, null, datasetIdentifier, uuid);
 	}
    
    @GetMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/prototype/{uuid}", produces = "text/turtle; charset=UTF-8")
 	public ResponseEntity<?> getPrototypeContent(@PathVariable String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("uuid") String uuid)  {
    	return igetPrototypeContent(pu, puIdentifier, datasetIdentifier, uuid);
 	}
    
 	private ResponseEntity<?> igetMappingContent(String pu, String puIdentifier, String datasetIdentifier, String uuid)  {

    	GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);
    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Dataset dataset = gl.getMainGraph();
    	
    	Optional<MappingDocument> mappingOpt = mappingsRepository.findByDatasetIdAndUuid(dataset.getId(), uuid);
    	
    	if (!mappingOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	return ResponseEntity.ok(mappingOpt.get().getFileContents());
 	}
 	
 	private ResponseEntity<?> igetPrototypeContent(String pu, String puIdentifier, String datasetIdentifier, String uuid)  {
    	GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);
    	if (gl == null || gl.getMainGraph() == null) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Dataset dataset = gl.getMainGraph();
    	
    	Optional<PrototypeDocument> prototypeOpt = prototypeRepository.findByDatasetIdAndUuid(dataset.getId(), uuid);
    	
    	if (!prototypeOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	return ResponseEntity.ok(prototypeOpt.get().getContent());
 	}

//  @GetMapping(value = "/{database}/{order}/{graph}/sparql") // no well supported for multi-datasets
//  public ResponseEntity<?> getSparql(HttpServletRequest request, @PathVariable("database") String database, @PathVariable("order") int order, @PathVariable("graph") String graph) throws Exception {
////  	System.out.println("GET 2");
//  	return sparql(request, database, order, graph, true);
//  }
//  
//  @PostMapping(value = "/{database}/{order}/{graph}/sparql", consumes = "application/sparql-query") // not well supported for multi-datasets
//  public ResponseEntity<?> spostSparql(HttpServletRequest request, @PathVariable("database") String database, @PathVariable("order") int order, @PathVariable("graph") String graph) throws Exception {
////  	System.out.println("POST-SPARQL 2");
//  	return sparql(request, database, order, graph, true); 
//  }
//
//  @PostMapping(value = "/{database}/{order}/{graph}/sparql", consumes = "application/x-www-form-urlencoded")
//  public ResponseEntity<?> xpostSparql(HttpServletRequest request, @PathVariable("database") String database, @PathVariable("order") int order, @PathVariable("graph") String graph) throws Exception {
////  	System.out.println("POST-FORM 2");    	
//  	return sparql(request, database, order, graph, false); 
//  }
 	
//  private ResponseEntity<?> sparql(HttpServletRequest request, String db, int order, String graph, boolean queryStringParams) throws Exception {
//	
////	System.out.println(db);
////	System.out.println(order);
////	System.out.println(graph);
//	
//	if (!db.equals(database.getName())) {
//		return new ResponseEntity<>(HttpStatus.NOT_FOUND);	
//	}
//	
//	TripleStoreConfiguration vc = virtuosoConfigurations.getByOrder(order);
//	if (vc == null) {
//		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
//	}
//	
////	System.out.println(vc);
////	System.out.println(vc.getGraphUri(graph));
//	
//	return doSparql(request, vc, Arrays.asList(new Resource[] { new ResourceImpl(vc.getGraphUri(graph))} ), queryStringParams);
//}
  
 	@GetMapping(value = { "/{dataset-identifier}/sparql", "/{dataset-identifier}/sparql" }) // no well supported for multi-datasets
 	public ResponseEntity<?> getSparql(HttpServletRequest request, @PathVariable("dataset-identifier") String datasetIdentifier) throws Exception {
// 	    System.out.println("GET");
 		return sparql(request, null, null, datasetIdentifier, null, true);
 	}

 	@PostMapping(value = { "/{dataset-identifier}/sparql", "/{dataset-identifier}/sparql" }, 
 			     consumes = "application/x-www-form-urlencoded")
 	public ResponseEntity<?> xpostSparql(HttpServletRequest request, @PathVariable("dataset-identifier") String datasetIdentifier) throws Exception {
//  	System.out.println("POST-FORM");    	
 		return sparql(request, null, null, datasetIdentifier, null, false); 
 	}

 	@PostMapping(value = { "/{dataset-identifier}/sparql", "/{dataset-identifier}/sparql" }, 
 			     consumes = "application/sparql-query") // not well supported for multi-datasets
 	public ResponseEntity<?> spostSparql(HttpServletRequest request, @PathVariable("dataset-identifier") String datasetIdentifier) throws Exception {;
//  	System.out.println("POST-SPARQL");
 		return sparql(request, null, null, datasetIdentifier, null, true); 
 	}

 	@GetMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/sparql" })  
 	public ResponseEntity<?> getSparql(HttpServletRequest request, @PathVariable String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier) throws Exception {
//  	System.out.println("GET");
 		return sparql(request, pu, puIdentifier, datasetIdentifier, null, true);
 	}

 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/sparql" }, 
 			     consumes = "application/x-www-form-urlencoded")
 	public ResponseEntity<?> xpostSparql(HttpServletRequest request, @PathVariable String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier) throws Exception {
//  	System.out.println("POST-FORM");    	
 		return sparql(request, pu, puIdentifier, datasetIdentifier, null, false); 
 	}

 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/sparql" }, 
 			     consumes = "application/sparql-query") // not well supported for multi-datasets
 	public ResponseEntity<?> spostSparql(HttpServletRequest request, @PathVariable String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier) throws Exception {;
//  	System.out.println("POST-SPARQL");
 		return sparql(request, pu, puIdentifier, datasetIdentifier, null, true); 
 	}
 	

 	@GetMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/mapping/{mapping-identifier}/vsparql", 
 			              "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/mapping/{mapping-identifier}/{instance-identifier}/vsparql", 
 			              "/{dataset-identifier}/mapping/{mapping-identifier}/vsparql", 
 			              "/{dataset-identifier}/mapping/{mapping-identifier}/{instance-identifier}/vsparql" } ) // no well supported for multi-datasets
 	public ResponseEntity<?> vgetSparql(HttpServletRequest request, @PathVariable(name = "pu", required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("mapping-identifier") String mappingIdentifier, @PathVariable(name = "instance-identifier", required = false) String instanceIdentifier) throws Exception {
//  	System.out.println("GET");
 		return vsparql(request, pu, puIdentifier, datasetIdentifier, mappingIdentifier, instanceIdentifier, null, "GET");
 	}

 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/mapping/{mapping-identifier}/vsparql", 
 			               "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/mapping/{mapping-identifier}/{instance-identifier}/vsparql", 
 			               "/{dataset-identifier}/mapping/{mapping-identifier}/vsparql", 
 			               "/{dataset-identifier}/mapping/{mapping-identifier}/{instance-identifier}/vsparql" }, 
 			     consumes = "application/sparql-query") // not well supported for multi-datasets
 	public ResponseEntity<?> vspostSparql(HttpServletRequest request, @PathVariable(name = "pu", required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("mapping-identifier") String mappingIdentifier, @PathVariable(name = "instance-identifier", required = false) String instanceIdentifier) throws Exception {
//  	System.out.println("POST-SPARQL");
 		return vsparql(request, pu, puIdentifier, datasetIdentifier, mappingIdentifier, instanceIdentifier, null, "POST-SPARQL"); 
 	}

 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/mapping/{mapping-identifier}/vsparql", 
 			               "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/mapping/{mapping-identifier}/{instance-identifier}/vsparql", 
 			               "/{dataset-identifier}/mapping/{mapping-identifier}/vsparql", 
 			               "/{dataset-identifier}/mapping/{mapping-identifier}/{instance-identifier}/vsparql" }, consumes = "application/x-www-form-urlencoded")
 	public ResponseEntity<?> vxpostSparql(HttpServletRequest request, @PathVariable(name = "pu", required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("mapping-identifier") String mappingIdentifier, @PathVariable(name = "instance-identifier", required = false) String instanceIdentifier) throws Exception {
//  	System.out.println("POST-FORM");    	
 		return vsparql(request, pu, puIdentifier, datasetIdentifier, mappingIdentifier, instanceIdentifier, null, "POST-FORM"); 
 	}
 	
 	
 	@GetMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/annotator/{annotator-identifier}/sparql", 
 			              "/{dataset-identifier}/annotator/{annotator-identifier}/sparql" }) 
 	public ResponseEntity<?> agetSparql(HttpServletRequest request, @PathVariable(required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("annotator-identifier") String annotatorIdentifier) throws Exception {
//  	System.out.println("GET");
 		return sparql(request, pu, puIdentifier, datasetIdentifier, annotatorIdentifier, true);
 	}

 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/annotator/{annotator-identifier}/sparql", 
 			               "/{dataset-identifier}/annotator/{annotator-identifier}/sparql" }, 
 			     consumes = "application/x-www-form-urlencoded")
 	public ResponseEntity<?> axpostSparql(HttpServletRequest request, @PathVariable(required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("annotator-identifier") String annotatorIdentifier) throws Exception {
//  	System.out.println("POST-FORM");    	
 		return sparql(request, pu, puIdentifier, datasetIdentifier, annotatorIdentifier, false); 
 	}
 	
 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/annotator/{annotator-identifier}/sparql", 
 			               "/{dataset-identifier}/annotator/{annotator-identifier}/sparql" }, 
 			     consumes = "application/sparql-query") // not well supported for multi-datasets
 	public ResponseEntity<?> aspostSparql(HttpServletRequest request, @PathVariable(required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("annotator-identifier") String annotatorIdentifier) throws Exception {
//  	System.out.println("POST-SPARQL");
 		return sparql(request, pu, puIdentifier, datasetIdentifier, annotatorIdentifier, true); 
 	}

 	@GetMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/annotator/{annotator-identifier}/vsparql", 
 			              "/{dataset-identifier}/annotator/{annotator-identifier}/vsparql" }) 
 	public ResponseEntity<?> vagetSparql(HttpServletRequest request, @PathVariable(required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("annotator-identifier") String annotatorIdentifier) throws Exception {
//  	System.out.println("GET");
 		return vsparql(request, pu, puIdentifier, datasetIdentifier, null, null, annotatorIdentifier, "GET");
 	}

 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/annotator/{annotator-identifier}/vsparql", 
 			               "/{dataset-identifier}/annotator/{annotator-identifier}/vsparql" }, 
 			     consumes = "application/x-www-form-urlencoded")
 	public ResponseEntity<?> vaxpostSparql(HttpServletRequest request, @PathVariable(required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier,  @PathVariable("annotator-identifier") String annotatorIdentifier) throws Exception {
//  	System.out.println("POST-FORM");    	
 		return vsparql(request, pu, puIdentifier, datasetIdentifier, null, null, annotatorIdentifier, "POST-FORM"); 
 	}
 	
 	@PostMapping(value = { "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/annotator/{annotator-identifier}/vsparql", 
 			               "/{dataset-identifier}/annotator/{annotator-identifier}/vsparql" }, 
 			     consumes = "application/sparql-query") // not well supported for multi-datasets
 	public ResponseEntity<?> vaspostSparql(HttpServletRequest request, @PathVariable(required = false) String pu, @PathVariable(name = "pu-identifier", required = false) String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("annotator-identifier") String annotatorIdentifier) throws Exception {
//  	System.out.println("POST-SPARQL");
 		return vsparql(request, pu, puIdentifier, datasetIdentifier, null, null, annotatorIdentifier, "POST-SPARQL"); 
 	}
 	
 	private static Pattern gpA = Pattern.compile("^([0-9]{0,3})(i)?$");
 	private static Pattern gpB = Pattern.compile("^([0-9]{0,3})-([0-9]{0,3})$");
 	
 	private class DatasetPart {
 		public List<Dataset> datasets;
// 		public int groupIdentifier;
// 		public  boolean incrementalGroup;
 		public List<GroupRange> groups;
		
// 		public DatasetPart(List<Dataset> datasets, int groupIdentifier, boolean incrementalGroup) {
//			this.datasets = datasets;
//			this.groupIdentifier = groupIdentifier;
//			this.incrementalGroup = incrementalGroup;
//		}
 		
 		public DatasetPart(List<Dataset> datasets, List<GroupRange> groups) {
			this.datasets = datasets;
			this.groups = groups;
		}

 	}
 	
 	private class GroupRange {
 		private int start;
 		private int end;
 		
 		GroupRange(int start, int end) {
 			this.start = start;
 			this.end = end;
 		}
 	}
 	
	private ResponseEntity<?> sparql(HttpServletRequest request, String pu, String puIdentifier, String datasetIdentifiers, String annotatorIdentifier, boolean queryStringParams) throws Exception {
//		System.out.println(request.getQueryString());
//		System.out.println("T " + pu + " P " + puIdentifier + " D " + datasetIdentifier + " G " + groupIdentifierx + " A " + annotatorIdentifier);

		TripleStoreConfiguration vc = null; // supports only one triple store
		List<DatasetPart> dsp = new ArrayList<>();
		List<Dataset> allics = new ArrayList<>();

		for (String datasetIdentifier : datasetIdentifiers.split("~")) {
			String groupIdentifierx = null;
			int pos = datasetIdentifier.indexOf("_");
			if (pos > 0) {
				groupIdentifierx = datasetIdentifier.substring(pos + 1);
				datasetIdentifier = datasetIdentifier.substring(0, pos);
			}

			GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);
			if (gl == null || !gl.isPublik()) {
				return ResponseEntity.notFound().build();
			}
	
			// System.out.println(gl.getMainGraph());
			// System.out.println(gl.getSubGraphMap());

			List<Dataset> ics = new ArrayList<>();
	
			List<GroupRange> gid = new ArrayList<>();
			if (groupIdentifierx != null) {
				for (String g : groupIdentifierx.split("_")) {
					int gStart = -1;
					int gEnd = -1;
				
					Matcher m = gpA.matcher(g);
					if (m.find()) {
						gEnd = Integer.parseInt(m.group(1));
						if (m.group(2) != null && m.group(2).equals("i")) {
							gStart = 0;
						} else {
							gStart = gEnd;
						}
						
						gid.add(new GroupRange(gStart, gEnd));
					} else { 
						m = gpB.matcher(g);
						if (m.find()) {
							gStart = Integer.parseInt(m.group(1));
							gEnd = Integer.parseInt(m.group(2));
						}
						gid.add(new GroupRange(gStart, gEnd));
					}
				}
			}
		
//			int groupIdentifier = -1;
//			boolean incrementalGroup = false;
//			
//			if (groupIdentifierx != null) {
//				Matcher m = gp.matcher(groupIdentifierx);
//				if (m.find()) {
//					groupIdentifier = Integer.parseInt(m.group(1));
//					if (m.group(2) != null && m.group(2).equals("i")) {
//						incrementalGroup  = true;
//					}
//				}
//			}
			
			if (gl.getMainTripleStore() != null) {
				vc = gl.getMainTripleStore();
				ics.add(gl.getMainGraph());
	
//				if (groupIdentifier == -1) { // not well defined for nested datasets
//					List<Dataset> list = gl.getSubGraphMap().get(vc);
//					if (list != null) {
//						ics.addAll(list);
//					}
//				}
				
				if (gid.size() == 0) { // not well defined for nested datasets
					List<Dataset> list = gl.getSubGraphMap().get(vc);
					if (list != null) {
						ics.addAll(list);
					}
				}
	
			} else if (gl.getSubGraphMap().size() > 0) {
				Map.Entry<TripleStoreConfiguration, List<Dataset>> imap = gl.getSubGraphMap().entrySet().iterator().next();
	
				vc = imap.getKey();
				ics = imap.getValue();
			} else {
				return ResponseEntity.notFound().build();
			}
			
			allics.addAll(ics);
			
//			dsp.add(new DatasetPart(ics, groupIdentifier, incrementalGroup));
			dsp.add(new DatasetPart(ics, gid));
		}
		
		return doSparql(request, vc, dsp, idService.getExtraGraphs(allics, annotatorIdentifier), queryStringParams);
	}
	  
	private ResponseEntity<?> vsparql(HttpServletRequest request, String pu, String puIdentifier, String datasetIdentifier, String mappingIdentifier, String instanceIdentifier, String annotatorIdentifier, String type) throws Exception {

//		 System.out.println(request.getQueryString());
		GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);
		if (gl == null || !gl.isPublik()) {
			return ResponseEntity.notFound().build();
		}

		org.apache.jena.query.Dataset paramDataset = DatasetFactory.create();

		if (mappingIdentifier != null) {
			Optional<MappingDocument> mdocOpt = mappingsRepository.findByDatasetIdAndIdentifier(gl.getMainGraph().getId(), mappingIdentifier);
			if (!mdocOpt.isPresent()) {
				mdocOpt = mappingsRepository.findByUuid(mappingIdentifier);
			}

			if (!mdocOpt.isPresent()) {
				return ResponseEntity.notFound().build();
			}
			
			MappingContainer mc = mappingsService.getContainer((UserPrincipal)null, mdocOpt.get());
			if (mc == null) {
				return ResponseEntity.notFound().build(); 
			}
			
			List<ObjectId> mappingIds = new ArrayList<>();
			mappingIds.add(mc.getObject().getId());
			
			List<String> mappingInstanceIdentifiers = null;
			if (instanceIdentifier != null) {
				mappingInstanceIdentifiers = new ArrayList<>();
				mappingInstanceIdentifiers.add(instanceIdentifier);
			}
			
			mappingsService.executionResultsToModel(paramDataset, null, mappingIds, mappingInstanceIdentifiers);
					
		} else if (annotatorIdentifier != null) {
			Optional<AnnotatorDocument> adocOpt = adocRepository.findByDatasetIdAndIdentifier(gl.getMainGraph().getId(), annotatorIdentifier);
			if (!adocOpt.isPresent()) {
				adocOpt = adocRepository.findByUuid(annotatorIdentifier);
			}
			
			if (!adocOpt.isPresent()) {
				return ResponseEntity.notFound().build();
			}
			
			AnnotatorContainer ac = annotatorService.getContainer((UserPrincipal)null, adocOpt.get());
			if (ac == null) {
				return ResponseEntity.notFound().build(); 
			}
			
			List<ObjectId> annotatorIds = new ArrayList<>();
			annotatorIds.add(ac.getObject().getId());
			
			annotatorService.executionResultsToModel(paramDataset, null, annotatorIds);
					
		}
		
		return vdoSparql(request, paramDataset, type);

	}	

	private ResponseEntity<?> doSparql(HttpServletRequest request, TripleStoreConfiguration vc, List<DatasetPart> dsp, List<SideSpecificationDocument> extraDocs, boolean queryStringParams) throws Exception {
		
//		System.out.println(groupIdentifier + " > " + incrementalGroup);
		StringBuffer uri = new StringBuffer(vc.getSparqlEndpoint());

		String stringBody = null;
		MultiValueMap<String, String> formBody = null;

		Set<String> graphs = new HashSet<>();
		
		for (DatasetPart dp : dsp) {
			for (Dataset ic : dp.datasets) { // virtuoso does not support multiple parameteres separated with comma // it
										// will fail if length of URI is exceeded!!!
//				if (dp.groupIdentifier != -1) {
//					if (dp.incrementalGroup) {
//						for (int i = 0; i <= Math.min(ic.getMaxGroup(), dp.groupIdentifier); i++) {
//							graphs.add(ic.getContentTripleStoreGraph(resourceVocabulary, i));
//						}
//					} else {
//						graphs.add(ic.getContentTripleStoreGraph(resourceVocabulary, dp.groupIdentifier));
//					}
				if (dp.groups.size() > 0) {
					for (GroupRange gr : dp.groups) {
						for (int i = Math.min(ic.getMaxGroup(), gr.start); i <= Math.min(ic.getMaxGroup(), gr.end); i++) {
							graphs.add(ic.getContentTripleStoreGraph(resourceVocabulary, i));
						}
					}
				} else {
					for (int i = 0; i <= ic.getMaxGroup(); i++) {
						graphs.add(ic.getContentTripleStoreGraph(resourceVocabulary, i));
					}
				}
			}
			
			if (extraDocs != null) {
				for (SideSpecificationDocument doc : extraDocs) {
					graphs.add(doc.getTripleStoreGraph(resourceVocabulary, true));
				}
			}
		}
		
//		System.out.println(graphs);
		
		if (queryStringParams) {
			// builder.queryParam("default-graph-uri", graph.toString());
			int j = 0;
			
			for (String s : graphs) { // virtuoso does not support multiple parameteres separated with comma // it
										// will fail if length of URI is exceeded!!!
				
				s = URLEncoder.encode(s);
				if (j == 0) {
					uri.append("?default-graph-uri=" + s);
				} else {
					uri.append("&default-graph-uri=" + s);
				}
				j++;
			}

			// uri.append("?default-graph-uri=" + s);
			//
			for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
				if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
					continue;
				}
				//
				// Object[] obj = new Object[entry.getValue().length];
				for (int i = 0; i < entry.getValue().length; i++) {
					// obj[i] = entry.getValue()[i];

					uri.append("&" + entry.getKey() + "=" + URLEncoder.encode(entry.getValue()[i]));
				}
				//
				// builder = builder.queryParam(entry.getKey(), obj);
			}

			try {
				stringBody = IOUtils.toString(request.getInputStream(),
						Charset.forName(request.getCharacterEncoding()));
			} catch (IOException e1) {
				e1.printStackTrace();
				return ResponseEntity.badRequest().build();
			}

//			 System.out.println("STRING");
//			 System.out.println(stringBody);
//			 System.out.println(builder);

		} else {
			// request.getParameterMap();

			formBody = new LinkedMultiValueMap<String, String>();
			for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
				if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
					continue;
				}
				formBody.addAll(entry.getKey(), Arrays.asList(entry.getValue()));
			}

			for (String s : graphs) {
				formBody.add("default-graph-uri", s);
			}

//			 System.out.println("FORM");
//			 System.out.println(formBody);

		}

		// UriComponents uri = builder.build();

		// HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new
		// HttpComponentsClientHttpRequestFactory();
		// clientHttpRequestFactory.setReadTimeout(0);;
		//
		// RestTemplate restTemplate = new RestTemplate(clientHttpRequestFactory);
		RestTemplate restTemplate = new RestTemplate();
		// restTemplate.

//		 System.out.println(" " + uri.toUri());
//		 System.out.println(" " + uri.toString());
//		 System.out.println("BODY " + body);
//		 System.out.println("METHOD " + request.getMethod());

		HttpHeaders headers = new HttpHeaders();

		Enumeration<String> en = request.getHeaderNames();
		while (en.hasMoreElements()) {
			String header = en.nextElement();
			if (header.equalsIgnoreCase("accept-language") || header.equalsIgnoreCase("accept-encoding")) {// ||
																											// header.equalsIgnoreCase("connection"))
																											// {
				continue;
			}

			headers.add(header, request.getHeader(header));
			// System.out.println(header + "=" + request.getHeader(header));
		}
		// headers.add("connection", "Keep-Alive");

		try {
			ResponseEntity<String> s = restTemplate.exchange(
					// uri.toUri(),
					new URI(uri.toString()), HttpMethod.valueOf(request.getMethod()),
					stringBody != null ? new HttpEntity<>(stringBody, headers)
							: new HttpEntity<>(formBody, headers),
					String.class);

			// System.out.println(s);
			return s;

		} catch (HttpClientErrorException | HttpServerErrorException e) {
//	      	e.printStackTrace();
			return new ResponseEntity<>(e.getResponseBodyAsByteArray(), e.getResponseHeaders(), e.getStatusCode());
		}
	}
	
//	private ResponseEntity<?> doSparql(HttpServletRequest request, TripleStoreConfiguration vc, List<Dataset> ics, int groupIdentifier, boolean incrementalGroup, List<SideSpecificationDocument> extraDocs, boolean queryStringParams) throws Exception {
//		
////		System.out.println(groupIdentifier + " > " + incrementalGroup);
//		StringBuffer uri = new StringBuffer(vc.getSparqlEndpoint());
//
//		String stringBody = null;
//		MultiValueMap<String, String> formBody = null;
//
//		List<String> graphs = new ArrayList<>();
//		for (Dataset ic : ics) { // virtuoso does not support multiple parameteres separated with comma // it
//									// will fail if length of URI is exceeded!!!
//			if (groupIdentifier != -1) {
//				if (incrementalGroup) {
//					for (int i = 0; i <= Math.min(ic.getMaxGroup(), groupIdentifier); i++) {
//						graphs.add(ic.getContentTripleStoreGraph(resourceVocabulary, i));
//					}
//				} else {
//					graphs.add(ic.getContentTripleStoreGraph(resourceVocabulary, groupIdentifier));
//				}
//			} else {
//				for (int i = 0; i <= ic.getMaxGroup(); i++) {
//					graphs.add(ic.getContentTripleStoreGraph(resourceVocabulary, i));
//				}
//			}
//		}
//		
//		if (extraDocs != null) {
//			for (SideSpecificationDocument doc : extraDocs) {
//				graphs.add(doc.getTripleStoreGraph(resourceVocabulary, true));
//			}
//		}
//		
////		System.out.println(graphs);
//		
//		if (queryStringParams) {
//			// builder.queryParam("default-graph-uri", graph.toString());
//			int j = 0;
//			
//			for (String s : graphs) { // virtuoso does not support multiple parameteres separated with comma // it
//										// will fail if length of URI is exceeded!!!
//				
//				s = URLEncoder.encode(s);
//				if (j == 0) {
//					uri.append("?default-graph-uri=" + s);
//				} else {
//					uri.append("&default-graph-uri=" + s);
//				}
//				j++;
//			}
//
//			// uri.append("?default-graph-uri=" + s);
//			//
//			for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
//				if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
//					continue;
//				}
//				//
//				// Object[] obj = new Object[entry.getValue().length];
//				for (int i = 0; i < entry.getValue().length; i++) {
//					// obj[i] = entry.getValue()[i];
//
//					uri.append("&" + entry.getKey() + "=" + URLEncoder.encode(entry.getValue()[i]));
//				}
//				//
//				// builder = builder.queryParam(entry.getKey(), obj);
//			}
//
//			try {
//				stringBody = IOUtils.toString(request.getInputStream(),
//						Charset.forName(request.getCharacterEncoding()));
//			} catch (IOException e1) {
//				e1.printStackTrace();
//				return ResponseEntity.badRequest().build();
//			}
//
////			 System.out.println("STRING");
////			 System.out.println(stringBody);
////			 System.out.println(builder);
//
//		} else {
//			// request.getParameterMap();
//
//			formBody = new LinkedMultiValueMap<String, String>();
//			for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
//				if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
//					continue;
//				}
//				formBody.addAll(entry.getKey(), Arrays.asList(entry.getValue()));
//			}
//
//			for (String s : graphs) {
//				formBody.add("default-graph-uri", s);
//			}
//
////			 System.out.println("FORM");
////			 System.out.println(formBody);
//
//		}
//
//		// UriComponents uri = builder.build();
//
//		// HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new
//		// HttpComponentsClientHttpRequestFactory();
//		// clientHttpRequestFactory.setReadTimeout(0);;
//		//
//		// RestTemplate restTemplate = new RestTemplate(clientHttpRequestFactory);
//		RestTemplate restTemplate = new RestTemplate();
//		// restTemplate.
//
////		 System.out.println(" " + uri.toUri());
////		 System.out.println(" " + uri.toString());
////		 System.out.println("BODY " + body);
////		 System.out.println("METHOD " + request.getMethod());
//
//		HttpHeaders headers = new HttpHeaders();
//
//		Enumeration<String> en = request.getHeaderNames();
//		while (en.hasMoreElements()) {
//			String header = en.nextElement();
//			if (header.equalsIgnoreCase("accept-language") || header.equalsIgnoreCase("accept-encoding")) {// ||
//																											// header.equalsIgnoreCase("connection"))
//																											// {
//				continue;
//			}
//
//			headers.add(header, request.getHeader(header));
//			// System.out.println(header + "=" + request.getHeader(header));
//		}
//		// headers.add("connection", "Keep-Alive");
//
//		try {
//			ResponseEntity<String> s = restTemplate.exchange(
//					// uri.toUri(),
//					new URI(uri.toString()), HttpMethod.valueOf(request.getMethod()),
//					stringBody != null ? new HttpEntity<>(stringBody, headers)
//							: new HttpEntity<>(formBody, headers),
//					String.class);
//
//			// System.out.println(s);
//			return s;
//
//		} catch (HttpClientErrorException | HttpServerErrorException e) {
////	      	e.printStackTrace();
//			return new ResponseEntity<>(e.getResponseBodyAsByteArray(), e.getResponseHeaders(), e.getStatusCode());
//		}
//	}

	private ResponseEntity<?> vdoSparql(HttpServletRequest request, org.apache.jena.query.Dataset jDataset, String type) throws Exception {

		Map<String, String[]> params = request.getParameterMap();
		String accept = request.getHeader("accept");
		if (accept == null) {
			return ResponseEntity.badRequest().build();
		}
		
		String query;
		if (type.equals("GET") || type.equals("POST-FORM")) {
			String[] queryArray = params.get("query");
			if (queryArray != null && queryArray.length > 0) {
				query = queryArray[0];
			} else {
				return ResponseEntity.badRequest().build();
			}
		} else {
			query = IOUtils.toString(request.getInputStream(), StandardCharsets.UTF_8);
		}		    

		try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(query, Syntax.syntaxSPARQL_11), jDataset)) {
			ResultSet rs = qe.execSelect();
		
			MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();

			try (ByteArrayOutputStream fos = new ByteArrayOutputStream()) {
				if (accept.toLowerCase().startsWith("application/sparql-results+json")) {
					ResultSetFormatter.outputAsJSON(fos,rs);
					
					headers.add("Content-type", "application/sparql-results+json");
					
				} else if (accept.toLowerCase().startsWith("application/sparql-results+xml")) {
					ResultSetFormatter.outputAsXML(fos,rs);
					
					headers.add("Content-type", "application/sparql-results+xml");
				} else if (accept.toLowerCase().startsWith("text/csv")) {
					ResultSetFormatter.outputAsCSV(fos,rs);
					
					headers.add("Content-type", "text/csv");
				} else if (accept.toLowerCase().startsWith("text/tab-separated-values")) {
					ResultSetFormatter.outputAsTSV(fos,rs);
					
					headers.add("Content-type", "text/tab-separated-values");
				}
				
				return new ResponseEntity(new String(fos.toByteArray()), headers, HttpStatus.OK);

			}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}

    
    @GetMapping(value = "/db/{database}/annotations/download-url-list")
    public ResponseEntity<?> getAnnotationsList(@PathVariable("database") String db) {
    	
    	if (!db.equals(database.getName())) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);	
    	}
    	
    	List<ListElement> res = new ArrayList<>();
    	
    	List<Dataset> datasets = datasetsRepository.findByScopeAndDatabaseId(DatasetScope.COLLECTION, database.getId());
    	for (Dataset ds : datasets) {
    		String identifier = ds.getIdentifier();
    		
    		if (identifier != null && ds.isPublik() && aegRepository.findByDatasetUuidAndAutoexportable(ds.getUuid(), true).size() > 0) {
    			ListElement le = new ListElement();
    			le.setDatasetIdentifier(identifier);
    			le.setDownloadUrl(resourceVocabulary.getContentAnnotations(identifier).toString());
    			
    			if (ds.getDatasets() != null) {
	    			for (ObjectId childDatasetId : ds.getDatasets()) {
	    		    	Optional<Dataset> childDatasetOpt = datasetsRepository.findById(childDatasetId);
	    		    	if (childDatasetOpt.isPresent()) {
	    		    		le.addChildrenDatasetIdentifier(childDatasetOpt.get().getIdentifier());
	    		    	}
	    			}
    			}
    			
    			res.add(le);
    		}
    	}
    	
    	return ResponseEntity.ok(res);
    }

    @GetMapping(value = "/{identifier}/annotations")
 	public ResponseEntity<StreamingResponseBody> get(@PathVariable("identifier") String identifier,
 			@RequestParam(required = false, defaultValue = "JSON-LD") String serialization, 
			@RequestParam(required = false, defaultValue = "false") boolean onlyReviewed, 
			@RequestParam(required = false, defaultValue = "true") boolean onlyNonRejected,
			@RequestParam(required = false, defaultValue = "true") boolean onlyFresh,
			@RequestParam(required = false, defaultValue = "true") boolean created,
			@RequestParam(required = false, defaultValue = "true") boolean creator,
			@RequestParam(required = false, defaultValue = "true") boolean score,
			@RequestParam(required = false, defaultValue = "true") boolean scope, 
			@RequestParam(required = false, defaultValue = "true") boolean selector, 
			@RequestParam(required = false, defaultValue = "TGZ") String output
			)  {

    	//check identifier should add dataset uuid to mapping
    	GraphLocation gl = idService.getGraph(identifier);
    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Dataset dataset = gl.getMainGraph();
    	
    	long start = System.currentTimeMillis();
    	
    	logger.info("Start exporting annotations for dataset " + identifier);
    	
    	if (output.equalsIgnoreCase("zip")) {
    	    
    		HttpHeaders headers = new HttpHeaders();
    	    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
    	    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=annotations_" + dataset.getUuid() + ".zip");
    	    
    		StreamingResponseBody stream = outputStream -> {
				try (ZipOutputStream zos = new ZipOutputStream(outputStream)) {
    				for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(dataset.getUuid(), true)) {
    					logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId() + " started.");
		    			try {
							aegService.exportAnnotations(aeg.getId(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, zos);
						} catch (Exception ex) {
							logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId() + " failed.");
							
							ex.printStackTrace();
							break;
						}
		    			logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId() + " completed.");
			    	}
					zos.finish();
				}
    		};
		    	
    	    return ResponseEntity.ok()
    	            .headers(headers)
    	            .contentType(MediaType.APPLICATION_OCTET_STREAM)
    	            .body(stream);
    			    	
    	
    	} else if (output.equalsIgnoreCase("tgz"))	{
    		
    	    HttpHeaders headers = new HttpHeaders();
    	    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
    	    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=annotations_" + dataset.getUuid() + ".tgz");
    	    
    	    StreamingResponseBody stream = outputStream -> {
    			try (BufferedOutputStream buffOut = new BufferedOutputStream(outputStream);
    					GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
    					TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {

    				for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(dataset.getUuid(), true)) {
    					logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId() + " started.");

    					try {
							aegService.exportAnnotations(aeg.getId(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, tOut);
						} catch (Exception ex) {
							logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId() + " failed.");
							
							ex.printStackTrace();
							break;
						}
    					
    					logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId() + " completed.");
    				}
    			}	        
    	    };
    	    
    	    return ResponseEntity.ok()
    	            .headers(headers)
    	            .contentType(MediaType.APPLICATION_OCTET_STREAM)
    	            .body(stream);
    		}

			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    	}

    private Date updateDate(Date previous, Date inew) {
    	if (previous == null) {
    		return inew;
    	} else if (previous.before(inew)) {
    		return inew; 
    	} else {
    		return previous;
    	}
    }
    
    private boolean areEqual(Map<String, Object> p1, Map<String, Object> p2) {
    	if (p1.size() != p2.size()) {
    		return false;
    	}
    	
    	for (Map.Entry<String, Object> entry : p1.entrySet()) {
    		String key = entry.getKey();
    		Object p2Value = p2.get(key);
    		if (p2Value == null || !p2Value.equals(entry.getValue())) {
    			return false;
    		}
    	}
    	
    	return true;
    }
    
    private boolean areEqual(AnnotationExportPublicTaskData p1, AnnotationExportPublicTaskData p2) {
    	if (!p1.getLastUpdatedAt().equals(p2.getLastUpdatedAt())) {
    		return false;
    	}
    	
    	if (p1.getAnnotatorDocumentId() != null || p2.getAnnotatorDocumentId() != null) {
    		if (p1.getAnnotatorDocumentId() == null || p2.getAnnotatorDocumentId() == null) {
    			return false;
    		}
    		
    		if (p1.getAnnotatorDocumentId().size() != p2.getAnnotatorDocumentId().size()) {
    			return false;
    		}
    		
    		if (!p1.getAnnotatorDocumentId().containsAll(p2.getAnnotatorDocumentId())) {
    			return false;
    		}
    		
    	}

    	if (p1.getPagedAnnotationValidationId() != null || p2.getPagedAnnotationValidationId() != null) {
    		if (p1.getPagedAnnotationValidationId() == null || p2.getPagedAnnotationValidationId() == null) {
    			return false;
    		}
    		
	    	if (p1.getPagedAnnotationValidationId().size() != p2.getPagedAnnotationValidationId().size()) {
	    		return false;
	    	}
	    	
    		if (!p1.getPagedAnnotationValidationId().containsAll(p2.getPagedAnnotationValidationId())) {
    			return false;
    		}

    	}

    	if (p1.getFilterAnnotationValidationId() != null || p2.getFilterAnnotationValidationId() != null) {
    		if (p1.getFilterAnnotationValidationId() == null || p2.getFilterAnnotationValidationId() == null) {
    			return false;
    		}

	    	if (p1.getFilterAnnotationValidationId().size() != p2.getFilterAnnotationValidationId().size()) {
	    		return false;
	    	}
	    	
    		if (!p1.getFilterAnnotationValidationId().containsAll(p2.getFilterAnnotationValidationId())) {
    			return false;
    		}

    	}
    	
    	return true;
    }

    @GetMapping(value = "/task/{task-uuid}/state")
    public ResponseEntity<?> ann(@PathVariable("task-uuid") String taskUuid) {
    	Optional<PublicTask> ptOpt = publicTaskRepository.findByUuid(taskUuid);
    	
    	if (!ptOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	PublicTask pt = ptOpt.get();
    	
    	Map<String, Object> result = new LinkedHashMap<>();
    	
    	result.put("uuid", pt.getUuid());
    	result.put("type", pt.getType());
    	result.put("state", pt.getState());
    	
    	result.put("startedAt", pt.getStartedAt());
    	
    	if (pt.getCompletedAt() != null) {
    		result.put("completedAt", pt.getCompletedAt());
    	}

    	result.put("datasetIdentifier", pt.getDatasetIdentifier());

    	result.put("parameters", pt.getParameters());
    	
    	if (pt.getState() == TaskState.COMPLETED) {
    		result.put("result", server + "/api/content/task/" + taskUuid + "/result");
    	}
    	
    	return ResponseEntity.ok(result);
    	
	}

    @GetMapping(value = "/{identifier}/task/export-annotations")
 	public ResponseEntity<?> ann(@PathVariable("identifier") String identifier,
 			@RequestParam(required = false, defaultValue = "JSON-LD") String serialization, 
			@RequestParam(required = false, defaultValue = "false") boolean onlyReviewed, 
			@RequestParam(required = false, defaultValue = "true") boolean onlyNonRejected,
			@RequestParam(required = false, defaultValue = "true") boolean onlyFresh,
			@RequestParam(required = false, defaultValue = "true") boolean created,
			@RequestParam(required = false, defaultValue = "true") boolean creator,
			@RequestParam(required = false, defaultValue = "true") boolean score,
			@RequestParam(required = false, defaultValue = "true") boolean scope, 
			@RequestParam(required = false, defaultValue = "true") boolean selector, 
			@RequestParam(required = false, defaultValue = "TGZ") String output
			)  {
    	
    	//check identifier should add dataset uuid to mapping
    	GraphLocation gl = idService.getGraph(identifier);
    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
//    		resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Dataset with given identifier not found"); 
//    	        return null;
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Dataset dataset = gl.getMainGraph();
    	
    	String uuid = UUID.randomUUID().toString();
		
    	Map<String, Object> params = new LinkedHashMap<>();
    	params.put("output", output);
    	params.put("onlyReviewed", onlyReviewed);
    	params.put("onlyNonRejected", onlyNonRejected);
    	params.put("onlyFresh", onlyFresh);
    	params.put("created", created);
    	params.put("creator", creator);
    	params.put("score", score);
    	params.put("scope", scope);
    	params.put("selector", selector);
    	params.put("serialization", serialization);

    	AnnotationExportPublicTaskData ptData = createAnnotationExportPublicTaskData(dataset);
    	
    	for (PublicTask pt : publicTaskRepository.findByDatasetIdAndTypeAndFileSystemConfigurationId(dataset.getId(), PublicTaskType.ANNOTATION_EXPORT, fileSystemConfiguration.getId())) {
    		if (pt.getState() != TaskState.FAILED && areEqual(params, pt.getParameters()) && areEqual(ptData, pt.getAnnotationExportData())) {
    			HttpHeaders headers = new HttpHeaders();
    			headers.add("Location", server + "/api/content/task/" + pt.getUuid() + "/state");    
    			return new ResponseEntity<String>(headers,HttpStatus.FOUND);
//    			return "redirect:/" + identifier + "/task" + uuid;
    		}
    	}
    	
    	try {

        	PublicTask pt = new PublicTask();
        	pt.setDatabaseId(database.getId());
        	pt.setDatasetId(dataset.getId());
        	pt.setDatasetIdentifier(dataset.getIdentifier());
        	pt.setFileSystemConfigurationId(fileSystemConfiguration.getId());
        	pt.setParameters(params);
        	pt.setType(PublicTaskType.ANNOTATION_EXPORT);
        	pt.setUuid(uuid);
        	
        	pt.setAnnotationExportData(ptData);
        	
        	pt.setState(TaskState.STARTED);
        	pt.setStartedAt(new Date());

        	publicTaskRepository.save(pt);
        	
    		ListenableFuture<Date> task = contentService.execute(uuid, serialization, onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, output, dataset);
			
//    		return "redirect:/" + identifier + "/task" + uuid;
			HttpHeaders headers = new HttpHeaders();
			headers.add("Location", server + "/api/content/task/" + uuid + "/state");    
			return new ResponseEntity<String>(headers,HttpStatus.FOUND);
//			return new ResponseEntity<>("The file will be available at " + server + "/api/content/output?file=" + uuid + "." + output.toLowerCase(), HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//    		resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server error");
//    		return null;
		}
    	
    }
    
    private AnnotationExportPublicTaskData createAnnotationExportPublicTaskData(Dataset dataset) {
    	List<ObjectId> adocs = new ArrayList<>();
    	List<ObjectId> pavs  = new ArrayList<>();
    	List<ObjectId> favs  = new ArrayList<>();

    	Date date = null;
    	for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(dataset.getUuid(), true)) {
    		for (AnnotatorDocument adoc : adocRepository.findByAnnotatorEditGroupId(aeg.getId())) {
    			ProcessStateContainer psc = adoc.getCurrentPublishState(virtuosoConfigurations.values());
    			if (psc != null) {
    				PublishState ps = (PublishState)psc.getProcessState();
    				if (ps.getPublishState() == DatasetState.PUBLISHED) {
    					adocs.add(adoc.getId());
    					date = updateDate(date, ps.getPublishCompletedAt());
    				}
    			}
    		}
    		for (PagedAnnotationValidation pav : pavRepository.findByAnnotationEditGroupId(aeg.getId())) {
    			ProcessStateContainer psc = pav.getCurrentPublishState(virtuosoConfigurations.values());
    			if (psc != null) {
    				PublishState ps = (PublishState)psc.getProcessState();
    				if (ps.getPublishState() == DatasetState.PUBLISHED) {
    					pavs.add(pav.getId());
    					date = updateDate(date, ps.getPublishCompletedAt());
    				}
    			}
    		}
    		for (FilterAnnotationValidation fav : favRepository.findByAnnotationEditGroupId(aeg.getId())) {
    			ProcessStateContainer psc = fav.getCurrentPublishState(virtuosoConfigurations.values());
    			if (psc != null) {
    				PublishState ps = (PublishState)psc.getProcessState();
    				if (ps.getPublishState() == DatasetState.PUBLISHED) {
    					favs.add(fav.getId());
    					date = updateDate(date, ps.getPublishCompletedAt());
    				}
    			}
    		}
    	}
    	
    	AnnotationExportPublicTaskData td = new AnnotationExportPublicTaskData();
    	
    	if (adocs.size() != 0) {
    		td.setAnnotatorDocumentId(adocs);
    	}
    	if (pavs.size() != 0) {
    		td.setPagedAnnotationValidationId(pavs);
    	}
    	if (favs.size() != 0) {
    		td.setFilterAnnotationValidationId(favs);
    	}
    	
    	td.setLastUpdatedAt(date);

    	return td;
    }
    
    @GetMapping(value = "/task/{task-uuid}/result")
    public ResponseEntity<StreamingResponseBody> taskResult(@PathVariable("task-uuid") String taskUuid) {
    	Optional<PublicTask> ptOpt = publicTaskRepository.findByUuid(taskUuid);
    	
    	if (!ptOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	PublicTask pt = ptOpt.get();
    	
    	String fileName = pt.getUuid() + "." + pt.getParameters().get("output").toString().toLowerCase();
    	try {
    		File f = new File(fileSystemConfiguration.getPublicFolder() + File.separatorChar + fileName);
    		if (!f.exists()) {
    			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    		}
    		
			return apiUtils.downloadFile(fileSystemConfiguration.getPublicFolder() + File.separatorChar + fileName);
		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
    	
    }

    

	
//    @GetMapping(value = "/{identifier}/annotations")
// 	public ResponseEntity<?> get(@PathVariable("identifier") String identifier,
// 			@RequestParam(required = false, defaultValue = "JSON-LD") String serialization, 
//			@RequestParam(required = false, defaultValue = "false") boolean onlyReviewed, 
//			@RequestParam(required = false, defaultValue = "true") boolean onlyNonRejected,
//			@RequestParam(required = false, defaultValue = "true") boolean onlyFresh,
//			@RequestParam(required = false, defaultValue = "true") boolean created,
//			@RequestParam(required = false, defaultValue = "true") boolean creator,
//			@RequestParam(required = false, defaultValue = "true") boolean score,
//			@RequestParam(required = false, defaultValue = "true") boolean scope, 
//			@RequestParam(required = false, defaultValue = "true") boolean selector, 
//			@RequestParam(required = false, defaultValue = "TGZ") String output
//			)  {
//
//    	//check identifier should add dataset uuid to mapping
//    	GraphLocation gl = idService.getGraph(identifier);
//    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
//    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
//    	}
//    	
//    	Resource ics = gl.getMainGraph();
//    	
//    	Optional<Dataset> datasetOpt = datasetsRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(ics.toString())); // needs fixing
//    	
//    	if (!datasetOpt.isPresent()) {
//    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
//    	}
//    	
//    	ByteArrayResource resource = null;
//    	
//    	long start = System.currentTimeMillis();
//    	
//    	logger.info("Start exporting annotations for dataset " + identifier);
//    	
//    	if (output.equalsIgnoreCase("zip")) {
//			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
//				try (ZipOutputStream zos = new ZipOutputStream(baos)) {
//    				for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(datasetOpt.get().getUuid(), true)) {
//    					logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId());
//		    			aegService.exportAnnotations(aeg.getId().toString(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, zos);
//			    	}
//					zos.finish();
//
//				}
//				
//		    	resource = new ByteArrayResource(baos.toByteArray());
//		    	
//			} catch (Exception e) {
//				e.printStackTrace();
//				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//			} 
//    	} else if (output.equalsIgnoreCase("tgz"))	{
//    		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
//    			try (BufferedOutputStream buffOut = new BufferedOutputStream(baos);
//    					GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
//    					TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {
//
//    				for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(datasetOpt.get().getUuid(), true)) {
//    					logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId());
//
//    					aegService.exportAnnotations(aeg.getId().toString(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, tOut);
//    				}
//    			}
//		    	
//		    	resource = new ByteArrayResource(baos.toByteArray());
//    		} catch (Exception e) {
//				e.printStackTrace();
//				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//			}
//    	}
//    	
//    	logger.info("Finish exporting annotations for dataset " + identifier + " in " + (System.currentTimeMillis() - start) + " ms.");
//    	
//    	if (resource != null) {
//			HttpHeaders headers = new HttpHeaders();
//			headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
//			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + datasetOpt.get().getUuid() + "." + output.toLowerCase());
//	
//			return ResponseEntity.ok().headers(headers)
//	//	            .contentLength(ffile.length())
//					.contentType(MediaType.APPLICATION_OCTET_STREAM).body(resource);
//    	} else {
//    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);	
//    	}
//
// 	}
    
//    @GetMapping(value = "/{identifier}/dump", produces = "application/rdf+xml")
// 	public ResponseEntity<?> get(@PathVariable("identifier") String identifier)  {
//
//    	//check identifier should add dataset uuid to mapping
//    	GraphLocation gl = getGraph(identifier);
//    	
//    	String sparql = "CONSTRUCT {?p ?q ?r} FROM <" + gl.graph + "> WHERE {?p ?q ?r} ";
//
//		Writer sw = new StringWriter();
//
//    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(gl.vc.getSparqlEndpoint(), sparql)) {
//    	
//			Model model = qe.execConstruct();
//			
//			RDFDataMgr.write(sw, model, RDFFormat.RDFXML) ;
//    	}
//    	
//   		return ResponseEntity.ok(sw.toString()); 
// 	}
    
    // for compatibility only
    @GetMapping(value = "/{dataset-identifier}/distribution/{serialization}")
    public ResponseEntity<StreamingResponseBody> compatibilityDistribution(HttpServletRequest request, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable String serialization) throws Exception {
    	return idistribution(request, null, null, datasetIdentifier, null, serialization);
    }

    @GetMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/distribution/{serialization}")
    public ResponseEntity<StreamingResponseBody> compatibilityDistribution(HttpServletRequest request, @PathVariable("pu") String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable String serialization) throws Exception {
    	return idistribution(request, pu, puIdentifier, datasetIdentifier, null, serialization);
    }
    
    @GetMapping(value = "/{dataset-identifier}/distribution/{distribution-identifier}/{serialization}")
    public ResponseEntity<StreamingResponseBody> distribution(HttpServletRequest request, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("distribution-identifier") String distributionIdentifier, @PathVariable String serialization) throws Exception {
    	return idistribution(request, null, null, datasetIdentifier, distributionIdentifier, serialization);
    }
    
    @GetMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/distribution/{distribution-identifier}/{serialization}")
    public ResponseEntity<StreamingResponseBody> distribution(HttpServletRequest request, @PathVariable("pu") String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("distribution-identifier") String distributionIdentifier, @PathVariable String serialization) throws Exception {
    	return idistribution(request, pu, puIdentifier, datasetIdentifier, distributionIdentifier, serialization);
    }
    
    private ResponseEntity<StreamingResponseBody> idistribution(HttpServletRequest request, String pu, String puIdentifier, String datasetIdentifier, String distributionIdentifier, @PathVariable String serialization) throws Exception {

 		try {
	    	GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);
	    	if (gl == null || !gl.isPublik()) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	    	
	    	Dataset dataset = gl.getMainGraph();
	    	
	    	DistributionDocument distrDoc;
	    	
	    	if (distributionIdentifier == null) {
		    	List<DistributionDocument> distrOpt = distributionRepository.findByDatasetId(dataset.getId());
		    	
		    	if (distrOpt.size() == 0) {
		    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		    	}
		    	
		    	distrDoc = distrOpt.get(0);
	    	} else {
	    		Optional<DistributionDocument> distrOpt = distributionRepository.findByDatasetUuidAndIdentifier(dataset.getUuid(), distributionIdentifier);
	    	
	    		if (!distrOpt.isPresent()) {
	    			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    		}
	    		
	    		distrDoc = distrOpt.get();
	    	}
	    	
	    	DistributionContainer dc = (DistributionContainer)distributionService.getContainer(null, distrDoc);
	    	
	    	if (!dc.isCreated()) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	    	
	    	Optional<User> userOpt = userRepository.findById(dc.getEnclosingObject().getUserId());
	    	
	    	if (!userOpt.isPresent()) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	    	
	    	UserPrincipal user = new UserPrincipal(userOpt.get(), null);
	    	
	    	File file = folderService.getDatasetDistributionFile(user, dc.getEnclosingObject(), dc.getObject(), dc.checkCreateState(), SerializationType.get(serialization));
	    	
	    	return apiUtils.downloadFile(file.getAbsolutePath());
	    	
 		} catch (Exception e) {
 			e.printStackTrace();
 			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
 		}
    }

    @GetMapping(value = "/{dataset-identifier}/schema/{type}")
    public ResponseEntity<?> schema(@PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("type") String type,  @RequestParam(defaultValue = "rdf-xml", name = "format") String format)  {
    	return ischema(null, null, datasetIdentifier, type,  format);
    }
    
    @GetMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/schema/{type}")
    public ResponseEntity<?> schema(@PathVariable("pu") String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("type") String type,  @RequestParam(defaultValue = "rdf-xml", name = "format") String format)  {
    	return ischema(pu, puIdentifier, datasetIdentifier, type,  format);
    }
    
    public ResponseEntity<?> ischema(String pu, String puIdentifier, String datasetIdentifier, String type, String format)  {

    	GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);

    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	if (!format.equals("turtle") && !format.equals("trig") && !format.equals("rdf-xml") && !format.equals("nt")) {
    		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    	}

    	Dataset dataset = gl.getMainGraph();

		Model model = schemaService.readSchema(dataset, type);
		
		if (model != null) {
			Writer sw = new StringWriter();

			if (format.equals("turtle")) {
				RDFDataMgr.write(sw, model, RDFFormat.TURTLE);
				return ResponseEntity.ok().contentType(RDFMediaType.TEXT_TURTLE).body(replacePrefix(sw.toString()));
			} else if (format.equals("trig")) {
				RDFDataMgr.write(sw, model, RDFFormat.TRIG);
				return ResponseEntity.ok().contentType(RDFMediaType.APPLICATION_TRIG).body(replacePrefix(sw.toString()));
			} else if (format.equals("nt")) {
				RDFDataMgr.write(sw, model, RDFFormat.NT);
				return ResponseEntity.ok().contentType(RDFMediaType.APPLICATION_N_TRIPLES).body(replacePrefix(sw.toString()));
			} else if (format.equals("rdf-xml")) {
				RDFDataMgr.write(sw, model, RDFFormat.RDFXML_PLAIN);
				return ResponseEntity.ok().contentType(RDFMediaType.APPLICATION_RDF_XML).body(replacePrefix(sw.toString()));
			}
		} 
		
		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			
    }
    
    private String replacePrefix(String s) {
		if (database.getResourcePrefix().equals("http://sw.islab.ntua.gr/semaspace/resource/")) {
			return s.replaceAll("http://sw\\.islab\\.ntua\\.gr/semaspace/resource/", server + "/resource/");
		} else {
			return s;
		}
    }
    
    @GetMapping(value = "/{dataset-identifier}/view", produces = "text/turtle; charset=UTF-8")
    public ResponseEntity<?> view(HttpServletRequest request, @PathVariable("dataset-identifier") String datasetIdentifier, @RequestParam("uri") String uri, @RequestParam(defaultValue = "turtle", name = "format") String format) {
    	return iview(request, null, null, datasetIdentifier, uri, format);
    }

    @GetMapping(value = "/{pu:[pu]/{pu-identifier}/{dataset-identifier}/view", produces = "text/turtle; charset=UTF-8")
    public ResponseEntity<?> view(HttpServletRequest request, @PathVariable("pu") String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @RequestParam("uri") String uri, @RequestParam(defaultValue = "turtle", name = "format") String format) {
    	return iview(request, pu, puIdentifier, datasetIdentifier, uri, format);
    }
    
    private ResponseEntity<?> iview(HttpServletRequest request, String pu, String puIdentifier, String datasetIdentifier, String uri, String format) {

    	GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);
    	if (gl == null || !gl.isPublik())  {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}

    	TripleStoreConfiguration vc;  // supports only one triple store
    	List<Dataset> ics = new ArrayList<>();
    	
    	if (gl.getMainTripleStore() != null) {
    		vc = gl.getMainTripleStore();
    		ics.add(gl.getMainGraph());
    		
    		List<Dataset> list = gl.getSubGraphMap().get(vc);
    		if (list != null) {
    			ics.addAll(list);
    		}

    	} else {
    		Map.Entry<TripleStoreConfiguration, List<Dataset>> imap = gl.getSubGraphMap().entrySet().iterator().next();
    		
        	vc = imap.getKey();
        	ics = imap.getValue();
    	}
    	
    	StringBuffer res = new StringBuffer();
    	
    	List<String> uris = new ArrayList<>();
    	uris.add(uri);
    	
    	Set<String> usedUris = new HashSet<>();
    	
		for (int i = 0; i < uris.size(); i++) {
			String currentUri = uris.get(i);
			
			if (!usedUris.add(currentUri)) {
				continue;
			}
			
			String s = readEntity(vc, resourceVocabulary.getDatasetContentAsResource(ics.get(0)).getURI(), currentUri, uris, usedUris, format);
			if (s.length() > 0) {
				if (i == 0) {
					res.append(s);
				} else {
					res.append("\n\n");
					res.append(s);
				}
			}
		}
		
		return ResponseEntity.ok(res.toString());    	
    }
    
    private String readEntity(TripleStoreConfiguration vc, String graph, String uri, List<String> uris, Set<String> usedUris, String format) {
		
		String sparql = "SELECT DISTINCT ?o FROM <" + graph + "> WHERE { <" + uri + "> ?p ?o . FILTER(isIRI(?o)) } ORDER BY ?o";

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {

			ResultSet results = qe.execSelect();
			
			while (results.hasNext()) {
				QuerySolution qs = results.next();
	
				String newUri = qs.get("o").asResource().getURI();
				
				if (!usedUris.contains(newUri)) {
//					if (vm.findPrefix(graph, newUri) != null) {
						uris.add(newUri);
//					} else {
//						usedUris.add(newUri);
//					}
				}
			}
		}

		StringBuffer sb = null;
		int i;
		for (i = 1; ; i++) {
			sb = new StringBuffer();
			
//			sb.append("SELECT COUNT(DISTINCT ?o" + i + ")  FROM <" + graph + "> WHERE { <" + uri + "> ?p1 ?o1 . ");
			sb.append("SELECT (COUNT(DISTINCT ?o" + i + ") AS ?x) FROM <" + graph + "> WHERE { <" + uri + "> ?p1 ?o1 . ");
			for (int j = 2; j <= i; j++) {
				sb.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
			}
			for (int j = 2; j <= i; j++) {
				sb.append(" } ");
			}
			
			sb.append("}");
			
//			System.out.println(QueryFactory.create(sb.toString(), Syntax.syntaxARQ));
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sb.toString(), Syntax.syntaxARQ))) {

				ResultSet results = qe.execSelect();
				String name = results.getResultVars().get(0);
				
				QuerySolution qs = results.next();
	
				if (qs.get(name).asLiteral().getInt() == 0) {
					break;
				}
			}
			
		}

		
		StringBuffer db = new StringBuffer();

		db.append("CONSTRUCT { ?s ?p1 ?o1 . ");
		for (int j = 2; j < i; j++) {
			db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
		}
		db.append("} FROM <" + graph + "> WHERE { VALUES ?s { <" + uri + "> }  ?s ?p1 ?o1 . ");
		for (int j = 2; j < i; j++) {
			db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
		}			
		for (int j = 2; j < i; j++) {
			db.append(" } ");
		}
		db.append("}");
				
		
		Model model = ModelFactory.createDefaultModel();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(db.toString(), Syntax.syntaxARQ))) {
			Model md = qe.execConstruct();
			model.add(md.listStatements());
		}
		
		StringWriter sw = new StringWriter();
		try {
			model.write(sw, format);
		} catch (org.apache.jena.riot.RiotException e) {
			model.write(sw, "turtle");
		}
		
		return sw.toString();
		
    }

    private class IndexContainer {
    	String datasetIdentifier;
    	IndexStructure indexStructure;
    	
    	IndexContainer(String datasetIdentifier, IndexStructure indexStructure) {
    		this.datasetIdentifier = datasetIdentifier;
    		this.indexStructure  = indexStructure;
    	}
    	
       	public String getLocalIdentifier() {
    		return database.getName() + "-" + datasetIdentifier + (indexStructure != null ? "-" + indexStructure.getIdentifier() : "");
    	}

    }
    
    private class IndexLocation {
    	public Map<ElasticConfiguration, List<IndexContainer>> indexMap;
    	
    	public IndexLocation() {
    		indexMap = new HashMap<>();
    	}
    	
    	public void add(IndexContainer indexContainer, ElasticConfiguration ec) {
    		List<IndexContainer> list = indexMap.get(ec);
    		if (list == null) {
    			list = new ArrayList<>();
    			indexMap.put(ec, list);
    		}
    		list.add(indexContainer);
    	}
    	
    	public int size() {
    		return indexMap.size();
    	}
    }
    
    private IndexLocation getIndex(String datasetIdentifier, String indexStructureIdentifier) {
 		
    	Element e = indicesCache.get(datasetIdentifier + (indexStructureIdentifier != null ? "-" + indexStructureIdentifier : ""));
 		if (e != null) {
 			return (IndexLocation)e.getObjectValue();
 		}
 		
 		Optional<Dataset> datasetOpt = datasetsRepository.findByIdentifierAndDatabaseId(datasetIdentifier, database.getId());
 		
 		if (!datasetOpt.isPresent()) {
 			return null;
 		}
 		
 		Dataset dataset = datasetOpt.get();
 		
 		List<Dataset> allDatasets = new ArrayList<>();
 		List<IndexStructure> allIndexStructures = new ArrayList<>();
 		
 		if (dataset.getDatasetType() == DatasetType.CATALOG) {
 			for (ObjectId subDatasetId : dataset.getDatasets()) {
 		 		Optional<Dataset> subDatasetOpt = datasetsRepository.findById(subDatasetId);
 		 		
 		 		if (!datasetOpt.isPresent()) {
 		 			continue;
 		 		}
 		 		
 		 		allDatasets.add(subDatasetOpt.get());
 			}
 		} else {
 			allDatasets.add(dataset);
 		}

	 	if (indexStructureIdentifier != null) {
	 		Optional<IndexStructure> indexStructureOpt = indexStructureRepository.findByIdentifierAndDatabaseId(indexStructureIdentifier, database.getId());
	 				
	 		if (!indexStructureOpt.isPresent()) {
	 			return null;
	 		}

	 		IndexStructure indexStructure = indexStructureOpt.get();
	 		
	 		for (int i = 0; i < allDatasets.size(); i++) {
	 			allIndexStructures.add(indexStructure);
	 		}
	 		
	 	} else {
	 		
	 		for (int i = 0; i < allDatasets.size();) {
	 			Dataset ds = allDatasets.get(i);
	 			
		 		Optional<IndexStructure> indexStructureOpt = indexStructureRepository.findByIdentifierAndDatabaseId(ds.getIdentifier(), database.getId());
 				
		 		if (!indexStructureOpt.isPresent()) {
		 			allDatasets.remove(i);
		 			continue;
		 		} 
	 			
	 			allIndexStructures.add(indexStructureOpt.get());
	 			
	 			i++;
	 		}
	 	}
 		
 		IndexLocation dl = new IndexLocation();
 		
	 	ElasticConfiguration ec = null;
	 	
	 	for (int i = 0; i < allDatasets.size(); i++) {
	 		Dataset ds = allDatasets.get(i);
	 		IndexStructure idx = allIndexStructures.get(i);

	 		List<IndexDocument> idocs = indexDocumentRepository.findByDatasetIdAndIndexStructureId(ds.getId(), idx.getId());
	 		
	 		IndexDocument idoc = null;
		 	
		 	if (idocs.size() == 0) {
//		 		return null;
		 	} else if (idocs.size() >= 1) {
		 		idoc = idocs.get(0);
		 		
//		 	} else {
//		 		for (IndexDocument iidoc : idocs) {
//		 			if (iidoc.getIdefault()) {
//		 				idoc = iidoc;
//		 				break;
//		 			}
//		 		}
		 	}
		 		
		 	if (idoc != null) {
		 		ec = elasticConfigurations.getById(idoc.getElasticConfigurationId());
		 		
	 	 	 	IndexState istate = idoc.checkCreateState(ec.getId(),null); // supports one elastic;
	 	 	 	if (istate == null || istate.getCreateState() != CreatingState.CREATED) {
	 	 	 		continue;
	 	 	 	}
	 		 		
	 			dl.add(new IndexContainer(ds.getIdentifier(), idx), ec);
		 	}
	 	} 
 		
 		
 		if (dl.size() > 0) {
 			indicesCache.put(new Element(dataset.getIdentifier(), dl));
 			return dl;
 		} else {
 			return null;
 		}
    }
     
    private static int INDEX_RESULTS_SIZE = 10;
 
    @GetMapping(value = "/index/{dataset}/search", 
	        produces = "application/json")
	public ResponseEntity<?> indexLookup(HttpServletRequest request, @PathVariable("dataset") String dataset,  @RequestParam("text") String text,  @RequestParam("fields") Optional<List<String>> fields,  @RequestParam("keys") Optional<List<String>> keys, @RequestParam("type") String type, @RequestParam("size") Optional<Integer> size) throws Exception {
		return doIndexLookup(dataset, null, text, fields, keys, type, size);
	}

    @GetMapping(value = "/index/{dataset}/{index-structure}/search", 
		        produces = "application/json")
    public ResponseEntity<?> indexLookupStructure(HttpServletRequest request, @PathVariable("dataset") String dataset,  @PathVariable("index-structure") String indexStructure, @RequestParam("text") String text,  @RequestParam("fields") Optional<List<String>> fields,  @RequestParam("keys") Optional<List<String>> keys, @RequestParam("type") String type, @RequestParam("size") Optional<Integer> size) throws Exception {
    	return doIndexLookup(dataset, indexStructure, text, fields, keys, type, size);
    }
    
   	public ResponseEntity<?> doIndexLookup(String dataset, String indexStructure, String text, Optional<List<String>> fields, Optional<List<String>> keys, String type, Optional<Integer> size) throws Exception {    	
    	
    	IndexLocation indexLoc =  getIndex(dataset, indexStructure);

    	if (indexLoc == null) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Map.Entry<ElasticConfiguration, List<IndexContainer>> imap = indexLoc.indexMap.entrySet().iterator().next(); // one elastic currently
    	
    	ElasticConfiguration ec = imap.getKey();
    	List<IndexContainer> ics = imap.getValue();
    	
    	Set<String> iStructKeys = new HashSet<>();
    	
    	List<String> indexNames = new ArrayList<>();
    	for (IndexContainer ic : ics) {
    		indexNames.add(ic.getLocalIdentifier());
    		
    		iStructKeys.addAll(ic.indexStructure.getKeys());
    	}
    	
    	ElasticsearchClient client = ec.getClient();

    	SearchRequest.Builder searchRequest = new SearchRequest.Builder();
    	searchRequest.index(indexNames);

    	BoolQuery.Builder bool = QueryBuilders.bool();
	    	
		if (!keys.isPresent()) {
			for (String r : iStructKeys) {
				if (type.equals("term")) {
					bool.should(TermQuery.of(m -> m.field(r).value(text))._toQuery());
				} else if (type.equals("match")) {
					bool.should(MatchQuery.of(m -> m.field(r).query(text))._toQuery());
				} else if (type.equals("match-phrase")) {
					bool.should(MatchPhraseQuery.of(m -> m.field(r).query(text))._toQuery());
				} else if (type.equals("match-phrase-prefix")) {
					bool.should(MatchPhrasePrefixQuery.of(m -> m.field(r).query(text))._toQuery());
				}
			}
		} else {
			List<String> queryKeys = keys.get(); 
			for (String r : queryKeys) {
				if (type.equals("term")) {
					bool.should(TermQuery.of(m -> m.field(r).value(text))._toQuery());
				} else if (type.equals("match")) {
					bool.should(MatchQuery.of(m -> m.field(r).query(text))._toQuery());
				} else if (type.equals("match-phrase")) {
					bool.should(MatchPhraseQuery.of(m -> m.field(r).query(text))._toQuery());
				} else if (type.equals("match-phrase-prefix")) {
					bool.should(MatchPhrasePrefixQuery.of(m -> m.field(r).query(text))._toQuery());
				}
			}
		}
			
		if (size.isPresent()) {
			searchRequest.size(size.get());
		} else {
			searchRequest.size(INDEX_RESULTS_SIZE);
		}
		
		searchRequest.query(bool.build()._toQuery());
		
		SearchResponse<Object> searchResponse = client.search(searchRequest.build(), Object.class);
			
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode array = mapper.createArrayNode();

		List<String> ff = new ArrayList<>();
		if (fields.isPresent()) {
			ff = fields.get();
		}
		
		HitsMetadata<Object> hm = searchResponse.hits();
		
		for (Hit<Object> hit : hm.hits()) {
			Map<String, String> map = (Map<String, String>)hit.source();
			
			ObjectNode match = mapper.createObjectNode();
			match.put("uri", map.get("iri").toString());
			match.put("score", hit.score());
				
			for (String f : ff) {
				ArrayNode arr = mapper.createArrayNode();
				Object obj = map.get(f);
				if (obj != null) {
					if (obj instanceof Collection) {
						for (Object s : ((Collection)obj)) {
							arr.add(s.toString());
						}
					} else {
						arr.add(obj.toString());
					}
					
					match.put(f, arr);
				}
			}
			
			array.add(match);
		}
		
		return ResponseEntity.ok(mapper.writeValueAsString(array));
    }

    @PostMapping(value = "/index/{dataset}/search", 
	        produces = "application/json")
	public ResponseEntity<?> postIndexSearch(HttpServletRequest request, @PathVariable("dataset") String dataset, @RequestBody String query,  @RequestParam("fields") Optional<List<String>> fields,  @RequestParam("size") Optional<Integer> size) throws Exception {
    	return doPostIndexSearch(dataset, null, query, fields, size);
    }
    
    @PostMapping(value = "/index/{dataset}/{index-structure}/search", 
	        produces = "application/json")
	public ResponseEntity<?> postIndexSearchStructure(HttpServletRequest request, @PathVariable("dataset") String dataset,  @PathVariable("index-structure") String indexStructure, @RequestBody String query,  @RequestParam("fields") Optional<List<String>> fields,  @RequestParam("size") Optional<Integer> size) throws Exception {
    	return doPostIndexSearch(dataset, indexStructure, query, fields, size);
    }
    
	public ResponseEntity<?> doPostIndexSearch(String dataset, String indexStructure, String query, Optional<List<String>> fields,  Optional<Integer> size) throws Exception {
    	IndexLocation indexLoc =  getIndex(dataset, indexStructure);
		if (indexLoc == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		
    	Map.Entry<ElasticConfiguration, List<IndexContainer>> imap = indexLoc.indexMap.entrySet().iterator().next();
    	
    	ElasticConfiguration ec = imap.getKey();
    	List<IndexContainer> ics = imap.getValue();
    	
    	List<String> indexNames = new ArrayList<>();
    	for (IndexContainer ic : ics) {
    		indexNames.add(ic.getLocalIdentifier());
    	}
    	
		SearchRequest.Builder searchRequest = new SearchRequest.Builder();
		searchRequest.index(indexNames);

		if (size.isPresent()) {
			searchRequest.size(size.get()); 
		} else {
			searchRequest.size(INDEX_RESULTS_SIZE);
		}

    	searchRequest.withJson(new StringReader(query));
    	
    	SearchResponse<Object> searchResponse = ec.getClient().search(searchRequest.build(), Object.class);
			
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode array = mapper.createArrayNode();

		List<String> ff = new ArrayList<>();
		if (fields.isPresent()) {
			ff = fields.get();
		}
		
		HitsMetadata<Object> hm = searchResponse.hits();
		
		for (Hit<Object> hit : hm.hits()) {
			Map<String, String> map = (Map<String, String>)hit.source();
			
			ObjectNode match = mapper.createObjectNode();
			match.put("uri", map.get("iri").toString());
			match.put("score", hit.score());
				
			for (String f : ff) {
				ArrayNode arr = mapper.createArrayNode();
				Object obj = map.get(f);
				if (obj != null) {
					if (obj instanceof Collection) {
						for (Object s : ((Collection)obj)) {
							arr.add(s.toString());
						}
					} else {
						arr.add(obj.toString());
					}
					
					match.put(f, arr);
				}

			}
			
			array.add(match);
		}
		
		return ResponseEntity.ok(mapper.writeValueAsString(array));
	}  

    @PostMapping(value = "/index/{dataset}/count", 
	        produces = "application/json")
	public ResponseEntity<?> postIndexCount(HttpServletRequest request, @PathVariable("dataset") String dataset, @RequestBody String query) throws Exception {
    	return doPostIndexCount(dataset, null, query);
    }
    
    @PostMapping(value = "/index/{dataset}/{index-structure}/count", 
	        produces = "application/json")
	public ResponseEntity<?> postIndexCountStructure(HttpServletRequest request, @PathVariable("dataset") String dataset, @PathVariable("index-structure") String indexStructure, @RequestBody String query) throws Exception {
    	return doPostIndexCount(dataset, indexStructure, query);
    }
		
    public ResponseEntity<?> doPostIndexCount(String dataset, String indexStructure, String query) throws Exception {
    	IndexLocation indexLoc =  getIndex(dataset, indexStructure);
		if (indexLoc == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		
    	Map.Entry<ElasticConfiguration, List<IndexContainer>> imap = indexLoc.indexMap.entrySet().iterator().next();
    	
    	ElasticConfiguration ec = imap.getKey();
    	List<IndexContainer> ics = imap.getValue();
    	
    	List<String> indexNames = new ArrayList<>();
    	for (IndexContainer ic : ics) {
    		indexNames.add(ic.getLocalIdentifier());
    	}
		
    	ElasticsearchClient client = ec.getClient();
    	
    	CountRequest.Builder countRequest = new CountRequest.Builder();
    	countRequest.index(indexNames);
    	countRequest.withJson(new StringReader(query));
    	
    	CountResponse count = client.count(countRequest.build());
    	
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode obj = mapper.createObjectNode();
		obj.put("count", count.count());

		return ResponseEntity.ok(mapper.writeValueAsString(obj));
	} 


    @GetMapping(value = "/{dataset-identifier}/compare/{comparator-identifier}")
 	public ResponseEntity<?> scompare(@PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("comparator-identifier") String comparatorIdentifier, @RequestParam("iris") String[] iris)  {
    	List<ComputationResultWrapper> ciris = new ArrayList<>();
    	ciris.add(new ComputationResultWrapper(iris[0], iris[1]));
    	return icompare(null, null, datasetIdentifier, comparatorIdentifier, ciris);
 	}
    
    @GetMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/compare/{comparator-identifier}")
 	public ResponseEntity<?> scompare(@PathVariable String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("comparator-identifier") String comparatorIdentifier, @RequestParam("iris") String[] iris)  {
    	List<ComputationResultWrapper> ciris = new ArrayList<>();
    	ciris.add(new ComputationResultWrapper(iris[0], iris[1]));
    	return icompare(pu, puIdentifier, datasetIdentifier, comparatorIdentifier, ciris);
 	}

    @PostMapping(value = "/{dataset-identifier}/compare/{comparator-identifier}")
 	public ResponseEntity<?> pcompare(@PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("comparator-identifier") String comparatorIdentifier, @RequestBody List<ComputationResultWrapper> ciris)  {
    	return icompare(null, null, datasetIdentifier, comparatorIdentifier, ciris);
 	}

    
    @PostMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/compare/{comparator-identifier}")
 	public ResponseEntity<?> pcompare(@PathVariable String pu, @PathVariable("pu-identifier") String puIdentifier, @PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("comparator-identifier") String comparatorIdentifier, @RequestBody List<ComputationResultWrapper> ciris)  {
//    	System.out.println(c++ + " " + Arrays.toString(iris));
    	return icompare(pu, puIdentifier, datasetIdentifier, comparatorIdentifier, ciris);
 	}

    @GetMapping(value = "/{pu:[pu]}/{pu-identifier}/{dataset-identifier}/compare/{comparator-identifier}/flush" )
 	public ResponseEntity<?> flush(@PathVariable("dataset-identifier") String datasetIdentifier, @PathVariable("comparator-identifier") String comparatorIdentifier)  {
    	
    	if (fileStream != null) {
    		fileStream.flush();
    		fileStream.close();
    	}
    	
    	return ResponseEntity.ok().build();
 	}
    
    private static int c = 0;
    private static int cc = 0;
    
    static PrintStream  fileStream = null;
    static {
    	try {
//			fileStream = new PrintStream("comparator.txt");
//    		fileStream = System.out;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
 	private ResponseEntity<?> icompare(String pu, String puIdentifier, String datasetIdentifier, String comparatorIdentifier, List<ComputationResultWrapper> miris)  {

// 		System.out.println(c + " > " + miris.size());
// 		for (ComputationResultWrapper mi : miris) {
// 			fileStream.println("CRW: " + c + " " + mi.getIriA() + " " + mi.getIriB());
// 		}
//		fileStream.println(c++);
 		
    	GraphLocation gl = idService.getGraph(pu, puIdentifier, datasetIdentifier);
    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Dataset dataset = gl.getMainGraph();
    	
    	Optional<ComparatorDocument> cdocOpt = comparatorRepository.findByIdentifierAndDatabaseId(comparatorIdentifier, database.getId());
    	
    	if (!cdocOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	DatasetCatalog dcg = schemaService.asCatalog(dataset);

    	TripleStoreConfiguration vc = gl.getMainTripleStore();
    	String fromClause = schemaService.buildFromClause(dcg, false);
    	
    	ComparatorDocument cdoc = cdocOpt.get();
    	
		ClassIndexElement ie = cdoc.getStructure().getElement();
			
		Map<Integer, IndexKeyMetadata> keyMap = AnnotatorDocument.getKeyMetadataMap(cdoc.getStructure().getKeysMetadata());
		
//		String sparql = ie.topElementsListSPARQL(fromClause); 
		SPARQLStructure ss = sparqlService.toSPARQL(ie, keyMap, true);
			
//			System.out.println( ss.getWhereClause());
		
		List<String> keys = new ArrayList<>(ss.getKeys());
		
		List<ComputationResultWrapper> res = new ArrayList<>();
		try {
			for (ComputationResultWrapper ciris : miris) {
				Map<String, Object>[] values = new Map[2];
				
				String[] iris = new String[] { ciris.getIriA(), ciris.getIriB()  };
				
				for (int i = 0; i < 2; i++) {
//					System.out.println("GET " + iris[i]);
					values[i] = readItem(puIdentifier, datasetIdentifier, comparatorIdentifier, vc, ss, fromClause, keys, keyMap, iris[i]);
//					System.out.println("GOT " + values[i]);
				}	
				
				if (fileStream != null) {
					fileStream.println(">> " + iris[0] + " : " + iris[1] + " :: " + (++cc));
				}

				ComputationResult val = cdoc.getComputation().execute(values[0], values[1], fileStream);
				
				ComputationResultWrapper crw = new ComputationResultWrapper(iris[0], iris[1]);
				crw.setComputation(val);
				
				res.add(crw);
			}			
	    	
	    	return ResponseEntity.ok(res);
		} catch (Exception ex) {
			ex.printStackTrace();
			return ResponseEntity.badRequest().build();
		}
 	}
 	
// 	@Cacheable(value="comparatorCache", key="#iri")
 	private Map<String, Object> readItem(String puIdentifier, String datasetIdentifier, String comparatorIdentifier, TripleStoreConfiguration vc, SPARQLStructure ss, String fromClause, List<String> keys, Map<Integer, IndexKeyMetadata> keyMap, String iri) throws IOException {
 		
		Map<String, Object> e = apiCache.get("ICOMPARE:" + puIdentifier + "/" + datasetIdentifier + "/" + comparatorIdentifier + ":" + iri);
 		if (e != null) {
 			return e;
 		} else {
 		
//	 		System.out.println("READING " + iri);
	    	String subjectSparql = ss.construct(fromClause, iri);
//	    	System.out.println(QueryFactory.create(subjectSparql, Syntax.syntaxARQ));
//	    	System.out.println(vc.getSparqlEndpoint());
	    	
	    	Map<String, List<String>> keyGroups = ss.getKeyGroups();
	    	
	        try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(subjectSparql, Syntax.syntaxARQ))) {
	        	
	        	Model model = null;
	        	int k = 0;
	        	while (k < 5) {
		        	try {
		            	model = RDFJenaResults.tryConstructQuery(qe);
		            	
//		            	model.write(System.out, "TTL");
		            	
		            	Map<String, Object> res = null;
		            	for (Map.Entry<String, List<String>> entry : keyGroups.entrySet()) {
		            		String group = entry.getKey();
		            		if (group == null) { // default group
		            			res = buildItem(null, new ResourceImpl(iri), model, ss, entry.getValue(), keyMap);
		            		} else {
				            	List<String> path = new ArrayList<>();
				            	path.add(SPARQLService.prefix + group);
				            	
				            	Set<String> giris = new HashSet<>();
				            	
				    			String sparql = "SELECT ?v WHERE { <" + iri + "> <" + SPARQLService.prefix + group + "> ?v } ";
				    			try (QueryExecution qe2 = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxARQ), model)) {
				    				ResultSet rs = qe2.execSelect();
				    				while (rs.hasNext()) {
				    					QuerySolution sol = rs.next();
				    					
				    					giris.add(sol.get("v").toString());
				    				}
				    			}

				    			for (String giri : giris) {
					            	List<String> pathElements = new ArrayList<>();
					            	pathElements.add(giri);

					            	Map<String, Object> res2  = buildItem(null, new ResourceImpl(iri), path, pathElements, model, ss, entry.getValue(), keyMap);
					            	ArrayList<Map<String, Object>> r = (ArrayList)res.get(group);
					            	if (r == null) {
					            		r = new ArrayList<>();
					            		res.put(group, r);
					            	}
					            	r.add(res2);
				    			}
		            		}
		            	}
		            	
		            	apiCache.put("ICOMPARE:" + puIdentifier + "/" + datasetIdentifier + "/" + comparatorIdentifier + ":" + iri, res);

//		            	System.out.println(res);

		     			return res;
		        	} catch (Exception ex) {
		        		ex.printStackTrace();
		        		try {
		        			k++;
							Thread.sleep(2*k*5*1000);
						} catch (InterruptedException e1) {
							System.err.println("Retrying " + k + " " + iri);
						}
		        	} finally {
		        		if (model != null) {
		        			model.close();
		        		}
		        	}
	        	}
	        }
	        
	        return null;
 		}
 	}
 	

 	private Map<String, Object>  buildItem(String graph, Resource subject, Model model, SPARQLStructure ss, List<String> keys, Map<Integer, IndexKeyMetadata> keyMap) throws IOException {
 		return buildItem(graph, subject, null, null, model, ss, keys, keyMap);
 	}
 		
   	private Map<String, Object>  buildItem(String graph, Resource subject, List<String> path, List<String> pathElement, Model model, SPARQLStructure ss, List<String> keys, Map<Integer, IndexKeyMetadata> keyMap) throws IOException {

   		Map<String, Object> values = new HashMap<>();
   		if (path == null) { //otherwise we are in nested part
   			values.put("iri", subject.getURI());
   		}
		
    	List<Set<Object>> lexicalForms = new ArrayList<>();
    	List<Set<String>> languages = new ArrayList<>();
    	List<List<double[]>> vectors = new ArrayList<>();
    	
    	Map<String, Integer> keysToIndexMap = new HashMap<>();
    	
    	for (int i = 0; i < keys.size(); i++) {
    		String key = keys.get(i);
    		keysToIndexMap.put(key, i);
    		
    		int keyIndex = Integer.parseInt(key.substring(1));
    		IndexKeyMetadata ikm = keyMap.get(keyIndex);
    		Analyzer analyzer = null;
    		if (ikm.getAnalyzer() != null) {
  				analyzer = LuceneFunctions.buildAnalyzer(ikm.getAnalyzer());
    		}

//    		int keyIndex = Integer.parseInt(key.substring(1));
//    		IndexKeyMetadata ikm = keyMap.get(keyIndex);
//    		ikm.getAnalyzer()

//    		System.out.println(key);
    		
//    		System.out.println(res);
    		
        	Set<Object> lexicalForm = new LinkedHashSet<>();
        	lexicalForms.add(lexicalForm);
        	
        	Set<String> language = new LinkedHashSet<>();
        	languages.add(language);
        	
//        	List<String> uri = new ArrayList<>();
//        	uris.add(uri);
		    	
        	List<double[]> vector = new ArrayList<>();
        	vectors.add(vector);

    		List<RDFNode> res  = null;
    		if (path == null) {
    			res = ss.getResultForKey(model, subject, key);
    		} else {
    			res = ss.getResultForKey(model, subject, path, pathElement, key);
    		}
//    		System.out.println(key + " > " + res);

        	for (RDFNode node : res) {
	    		if (node.isResource()) {
	    			lexicalForm.add(node.asResource().toString());
	    		} else if (node.isLiteral()) {
	    			Literal lit = node.asLiteral();
	    			
   	    			if (lit.getDatatypeURI().equals(VectorDatatype.theTypeURI) ) {
   	    	    		
	    				vector.add((double[])lit.getValue());
	    				
	    			} else { 
	    				Object v = lit.getValue();

	    				if (v instanceof String && analyzer != null) {
	    					lexicalForm.add(LuceneFunctions.applyAnalyzer(analyzer, (String)v));
	    				} else {
	    					lexicalForm.add(v);
	    				}
	    			
		    			String lang = lit.getLanguage();
		    			if (lang != null && lang.length() > 0) {
		    				language.add(lang);
		    			}

	    			}
	    		}
	    	}

    	}
    	
    	Map<String, Set<Object>> mergedLexicalForms = new HashMap<>();
    	Map<String, Set<String>> mergedLanguages = new HashMap<>();
    	Map<String, List<double[]>> mergedVectors = new HashMap<>();
    	
    	Map<String, List<String>> fieldsToKeysMap = new HashMap<>();
    	
    	for (int i = 0; i < keys.size(); i++) {
    		String key = keys.get(i);
    		
    		int keyIndex = Integer.parseInt(key.substring(1));
    		IndexKeyMetadata ikm = keyMap.get(keyIndex);
    		
    		String keyName = ikm == null ? key : ikm.getName();
    		
    		List<String> keysForName = fieldsToKeysMap.get(keyName);
    		
    		if (keysForName == null) {
    			keysForName = new ArrayList<>();
    			fieldsToKeysMap.put(keyName, keysForName);
    		}
    		keysForName.add(key);
    	}
    	
    	for (Map.Entry<String, List<String>> entry : fieldsToKeysMap.entrySet()) {
    		String fieldName = entry.getKey();
    		
        	Set<Object> sLexicalForms = new HashSet<>();
        	Set<String> sLanguages = new HashSet<>();
        	List<double[]> sVectors = new ArrayList<>();
        	
        	mergedLexicalForms.put(fieldName, sLexicalForms);
        	mergedLanguages.put(fieldName, sLanguages);
        	mergedVectors.put(fieldName, sVectors);
    	
    		for (String key : entry.getValue()) {
    			int i = keysToIndexMap.get(key);
    			
    			sLexicalForms.addAll(lexicalForms.get(i));
    			sLanguages.addAll(languages.get(i));
    			sVectors.addAll(vectors.get(i));
    		}
    	}
    	
    	for (String keyName : mergedLexicalForms.keySet()) {
    		
    		String firstKey = fieldsToKeysMap.get(keyName).iterator().next();
    		
    		int keyIndex = Integer.parseInt(firstKey.substring(1));
    		IndexKeyMetadata ikm = keyMap.get(keyIndex);

    		Set<Object> keyLexicalForms = mergedLexicalForms.get(keyName);
    		Set<String> keyLanguages = mergedLanguages.get(keyName);
    		
			if (keyLexicalForms.size() > 1) {
				values.put(keyName, Arrays.asList(keyLexicalForms.toArray()));
    		} else if (keyLexicalForms.size() == 1) {
    			values.put(keyName, keyLexicalForms.iterator().next());
    		}
			
			if (ikm.isLanguageField()) {
	    		if (keyLanguages.size() > 1) {
	    			values.put(keyName+ "-lang", Arrays.asList(keyLexicalForms.toArray()));
	    		} else if (keyLanguages.size() == 1) {
	    			values.put(keyName + "-lang", keyLanguages.iterator().next());
	    		}
			}
    		
    	}
    	
//    	System.out.println(values);
    	return values;
	}

   	
}
