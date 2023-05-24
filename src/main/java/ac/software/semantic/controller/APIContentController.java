package ac.software.semantic.controller;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.config.VocabulariesBean;
import ac.software.semantic.config.VocabulariesMap;
import ac.software.semantic.config.VocabularyInfo;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.User;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.constants.SerializationType;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.ListElement;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.IndexStructureRepository;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.repository.UserRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService;
import ac.software.semantic.service.IdentifiersService;
import ac.software.semantic.service.IdentifiersService.GraphLocation;
import ac.software.semantic.service.SchemaService;
import ac.software.semantic.service.FolderService;
import ac.software.semantic.vocs.SEMRVocabulary;
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
    IndexStructureRepository indexStructureRepository;
    
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private MappingRepository mappingsRepository;

	@Autowired
	private DatasetRepository datasetsRepository;

	@Autowired
	private AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	private UserRepository userRepository;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private IdentifiersService idService;

	@Autowired
	private FolderService folderService;

	@Autowired
	private AnnotationEditGroupService aegService;

	@Autowired
	private APIUtils apiUtils;

	@Autowired
	@Qualifier("all-datasets")
	private VocabulariesMap vm;
	
	@Autowired
	@Qualifier("endpoints-cache")
	private Cache endpointsCache;

	@Autowired
	@Qualifier("indices-cache")
	private Cache indicesCache;
	
	
    @GetMapping(value = "/{identifier}/mapping/{uuid}", produces = "text/turtle; charset=UTF-8")
 	public ResponseEntity<?> get(@PathVariable("identifier") String identifier, @PathVariable("uuid") String uuid)  {

    	//check identifier should add dataset uuid to mapping
    	GraphLocation gl = idService.getGraph(identifier);
    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Resource ics = gl.getMainGraph();
    	
    	Optional<Dataset> datasetOpt = datasetsRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(ics.toString())); // needs fixing
    	
    	if (!datasetOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Optional<MappingDocument> mappingOpt = mappingsRepository.findByDatasetIdAndUuid(datasetOpt.get().getId(), uuid);
    	
    	if (!mappingOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	return ResponseEntity.ok(mappingOpt.get().getFileContents());
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
 	public ResponseEntity<?> get(@PathVariable("identifier") String identifier,
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
    	
    	Resource ics = gl.getMainGraph();
    	
    	Optional<Dataset> datasetOpt = datasetsRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(ics.toString())); // needs fixing
    	
    	if (!datasetOpt.isPresent()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	ByteArrayResource resource = null;
    	
    	long start = System.currentTimeMillis();
    	
    	logger.info("Start exporting annotations for dataset " + identifier);
    	
    	if (output.equalsIgnoreCase("zip")) {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				try (ZipOutputStream zos = new ZipOutputStream(baos)) {
    				for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(datasetOpt.get().getUuid(), true)) {
    					logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId());
		    			aegService.exportAnnotations(aeg.getId().toString(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, zos);
			    	}
					zos.finish();

				}
				
		    	resource = new ByteArrayResource(baos.toByteArray());
		    	
			} catch (Exception e) {
				e.printStackTrace();
				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
			} 
    	} else if (output.equalsIgnoreCase("tgz"))	{
    		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
    			try (BufferedOutputStream buffOut = new BufferedOutputStream(baos);
    					GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
    					TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {

    				for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(datasetOpt.get().getUuid(), true)) {
    					logger.info("Exporting annotations for dataset " + identifier + "/" + aeg.getId());

    					aegService.exportAnnotations(aeg.getId().toString(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, tOut);
    				}
    			}
		    	
		    	resource = new ByteArrayResource(baos.toByteArray());
    		} catch (Exception e) {
				e.printStackTrace();
				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
			}
    	}
    	
    	logger.info("Finish exporting annotations for dataset " + identifier + " in " + (System.currentTimeMillis() - start) + " ms.");
    	
    	if (resource != null) {
			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + datasetOpt.get().getUuid() + "." + output.toLowerCase());
	
			return ResponseEntity.ok().headers(headers)
	//	            .contentLength(ffile.length())
					.contentType(MediaType.APPLICATION_OCTET_STREAM).body(resource);
    	} else {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);	
    	}

 	}
    
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
    
    @GetMapping(value = "/{identifier}/distribution/{serialization}")
    public ResponseEntity<StreamingResponseBody> distribution(HttpServletRequest request, @PathVariable String identifier, @PathVariable String serialization) throws Exception {
    	
 		try {
	    	GraphLocation gl = idService.getGraph(identifier);
	    	if (gl == null || !gl.isPublik()) {
	    		return ResponseEntity.notFound().build();
	    	}
	    	
	    	Resource ics = gl.getMainGraph();
	    	
	    	Optional<Dataset> datasetOpt = datasetsRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(ics.toString())); // needs fixing
	    	
	    	if (!datasetOpt.isPresent()) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	    	
	    	Dataset dataset = datasetOpt.get();
	    	
	    	ProcessStateContainer cps = dataset.getCurrentCreateDistributionState(fileSystemConfiguration.getId(), virtuosoConfigurations.values());
	    	
	    	if (cps == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	
	    	ProcessStateContainer pps = dataset.getCurrentPublishState(virtuosoConfigurations.values());
	    	
	    	if (pps == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	
	    	Optional<User> userOpt = userRepository.findById(dataset.getUserId());
	    	
	    	if (!userOpt.isPresent()) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	    	
	    	UserPrincipal user = new UserPrincipal(userOpt.get());
	    	
	    	File file = folderService.getDatasetDistributionFile(user, dataset, (PublishState)pps.getProcessState(), SerializationType.get(serialization));
	    	
	    	return apiUtils.downloadFile(file.getAbsolutePath());
	    	
 		} catch (Exception e) {
 			e.printStackTrace();
 			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
 		}
    }
    
    @GetMapping(value = "/{identifier}/schema/{type}")
    public ResponseEntity<?> schema(@PathVariable("identifier") String identifier, @PathVariable("type") String type,  @RequestParam(defaultValue = "rdf-xml", name = "format") String format)  {

    	GraphLocation gl = idService.getGraph(identifier);

    	if (gl == null || gl.getMainGraph() == null || !gl.isPublik()) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	if (!format.equals("turtle") && !format.equals("trig") && !format.equals("rdf-xml") && !format.equals("nt")) {
    		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    	}

//    	TripleStoreConfiguration vc = gl.getMainTripleStore();
    	Resource ics = gl.getMainGraph();

		Writer sw = new StringWriter();
		Model model = schemaService.readSchema(ics.toString(), type);
		
		if (model != null) {
			if (format.equals("turtle")) {
				RDFDataMgr.write(sw, model, RDFFormat.TURTLE);
				return ResponseEntity.ok().contentType(new MediaType("text","turtle")).body(replacePrefix(sw.toString()));
			} else if (format.equals("trig")) {
				RDFDataMgr.write(sw, model, RDFFormat.TRIG);
				return ResponseEntity.ok().contentType(new MediaType("application","trig")).body(replacePrefix(sw.toString()));
			} else if (format.equals("nt")) {
				RDFDataMgr.write(sw, model, RDFFormat.NT);
				return ResponseEntity.ok().contentType(new MediaType("application","trig")).body(replacePrefix(sw.toString()));
			} else if (format.equals("rdf-xml")) {
				RDFDataMgr.write(sw, model, RDFFormat.RDFXML_PLAIN);
				return ResponseEntity.ok().contentType(new MediaType("application","rdf+xml")).body(replacePrefix(sw.toString()));
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
    
    @GetMapping(value = "/{database}/{order}/{graph}/sparql") // no well supported for multi-datasets
    public ResponseEntity<?> getSparql(HttpServletRequest request, @PathVariable("database") String database, @PathVariable("order") int order, @PathVariable("graph") String graph) throws Exception {
//    	System.out.println("GET 2");
    	return sparql(request, database, order, graph, true);
    }
    
    @PostMapping(value = "/{database}/{order}/{graph}/sparql", consumes = "application/sparql-query") // not well supported for multi-datasets
    public ResponseEntity<?> spostSparql(HttpServletRequest request, @PathVariable("database") String database, @PathVariable("order") int order, @PathVariable("graph") String graph) throws Exception {
//    	System.out.println("POST-SPARQL 2");
    	return sparql(request, database, order, graph, true); 
    }

    @PostMapping(value = "/{database}/{order}/{graph}/sparql", consumes = "application/x-www-form-urlencoded")
    public ResponseEntity<?> xpostSparql(HttpServletRequest request, @PathVariable("database") String database, @PathVariable("order") int order, @PathVariable("graph") String graph) throws Exception {
//    	System.out.println("POST-FORM 2");    	
    	return sparql(request, database, order, graph, false); 
    }
    
    @GetMapping(value = "/{identifier}/sparql") // no well supported for multi-datasets
    public ResponseEntity<?> getSparql(HttpServletRequest request, @PathVariable("identifier") String identifier) throws Exception {
//    	System.out.println("GET");
    	return sparql(request, identifier, true);
    }
    
    @PostMapping(value = "/{identifier}/sparql", consumes = "application/sparql-query") // not well supported for multi-datasets
    public ResponseEntity<?> spostSparql(HttpServletRequest request, @PathVariable("identifier") String identifier) throws Exception {;
//    	System.out.println("POST-SPARQL");
    	return sparql(request, identifier, true); 
    }

    @PostMapping(value = "/{identifier}/sparql", consumes = "application/x-www-form-urlencoded")
    public ResponseEntity<?> xpostSparql(HttpServletRequest request, @PathVariable("identifier") String identifier) throws Exception {
//    	System.out.println("POST-FORM");    	
    	return sparql(request, identifier, false); 
    }

    private ResponseEntity<?> sparql(HttpServletRequest request, String db, int order, String graph, boolean queryStringParams) throws Exception {
    	
//    	System.out.println(db);
//    	System.out.println(order);
//    	System.out.println(graph);
    	
    	if (!db.equals(database.getName())) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);	
    	}
    	
    	TripleStoreConfiguration vc = virtuosoConfigurations.getByOrder(order);
    	if (vc == null) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
//    	System.out.println(vc);
//    	System.out.println(vc.getGraphUri(graph));
    	
    	return doSparql(request, vc, Arrays.asList(new Resource[] { new ResourceImpl(vc.getGraphUri(graph))} ), queryStringParams);
    }
    
    private ResponseEntity<?> sparql(HttpServletRequest request, String identifier, boolean queryStringParams) throws Exception {
   	
//    	System.out.println(request.getQueryString());
    	GraphLocation gl = idService.getGraph(identifier);
    	if (gl == null || !gl.isPublik()) {
    		return ResponseEntity.notFound().build();
    	}
    	
    	
//    	System.out.println(gl.getMainGraph());
//    	System.out.println(gl.getSubGraphMap());

//    	UriComponentsBuilder does not encode correctly!
//		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(virtuosoConfiguration.getSparqlEndpoint());
    	
    	TripleStoreConfiguration vc;  // supports only one triple store
    	List<Resource> ics = new ArrayList<>();
    	
    	if (gl.getMainTripleStore() != null) {
    		vc = gl.getMainTripleStore();
    		ics.add(gl.getMainGraph());
    		
    		List<Resource> list = gl.getSubGraphMap().get(vc);
    		if (list != null) {
    			ics.addAll(list);
    		}

    	} else {
    		Map.Entry<TripleStoreConfiguration, List<Resource>> imap = gl.getSubGraphMap().entrySet().iterator().next();
    		
        	vc = imap.getKey();
        	ics = imap.getValue();
    	}
    	
    	return doSparql(request, vc, ics, queryStringParams);
    }
    
    
    private ResponseEntity<?> doSparql(HttpServletRequest request, TripleStoreConfiguration vc, List<Resource> ics, boolean queryStringParams) throws Exception {
    	StringBuffer uri = new StringBuffer(vc.getSparqlEndpoint());
    	
		String stringBody = null;
		MultiValueMap<String, String> formBody = null;
		
    	if (queryStringParams) {
//    		builder.queryParam("default-graph-uri", graph.toString());
    		int j = 0;
    		for (Resource ic : ics) { // virtuoso does not support multiple parameteres separated with comma // it will fail if length of URI is exceeded!!!
    			if (j == 0) {
    				uri.append("?default-graph-uri=" + URLEncoder.encode(ic.getURI()));
    			} else {
    				uri.append("&default-graph-uri=" + URLEncoder.encode(ic.getURI()));
    			}
    			j++;
    		}
    		
//       	uri.append("?default-graph-uri=" + s); 
//    			
	    	for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
	    		if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
	    			continue;
	    		}
//	    		
//	    		Object[] obj = new Object[entry.getValue().length];
	    		for (int i = 0; i < entry.getValue().length; i++) {
//	    			obj[i] = entry.getValue()[i];
	    			
            		uri.append("&" + entry.getKey() + "=" + URLEncoder.encode(entry.getValue()[i]));
	    		}
//	    		
//	    		builder = builder.queryParam(entry.getKey(), obj);
	    	}
	    	
	    	
			try {
				stringBody = IOUtils.toString(request.getInputStream(), Charset.forName(request.getCharacterEncoding()));
			} catch (IOException e1) {
				e1.printStackTrace();
				return ResponseEntity.badRequest().build();
			}
			
//    		System.out.println("STRING");
//    		System.out.println(stringBody);
//    		System.out.println(builder);

    	} else {
//    		request.getParameterMap();
    		
    		formBody = new LinkedMultiValueMap<String, String>();
    		for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
    			if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
    				continue;
    			}
    			formBody.addAll(entry.getKey(), Arrays.asList(entry.getValue()));
    		}
    		
    		for (Resource ic : ics) {
    			formBody.add("default-graph-uri", ic.getURI());
    		}
    		
//    		System.out.println("FORM");
//    		System.out.println(formBody);
    		
    	}
    	
//    	UriComponents uri = builder.build();
    	
//    	HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory();
//    	clientHttpRequestFactory.setReadTimeout(0);;
//      
//    	RestTemplate restTemplate = new RestTemplate(clientHttpRequestFactory);
    	RestTemplate restTemplate = new RestTemplate();
//    	restTemplate.
    	
//    	System.out.println(" " + uri.toUri());
//    	System.out.println(" " + uri.toString());
//		System.out.println("BODY " + body);
//		System.out.println("METHOD " + request.getMethod());
		
		HttpHeaders headers = new HttpHeaders();

    	Enumeration<String> en = request.getHeaderNames();
    	while (en.hasMoreElements()) {
    		String header = en.nextElement();
    		if (header.equalsIgnoreCase("accept-language") || header.equalsIgnoreCase("accept-encoding")) {// || header.equalsIgnoreCase("connection")) {
    			continue;
    		}

        	headers.add(header, request.getHeader(header));
//        	System.out.println(header + "=" + request.getHeader(header));
    	}
//    	headers.add("connection", "Keep-Alive"); 
    	
        try {
	    	ResponseEntity<String> s = restTemplate.exchange(
//	    			uri.toUri(),
	    			new URI(uri.toString()),
	    			HttpMethod.valueOf(request.getMethod()), 
	    			stringBody != null ? new HttpEntity<>(stringBody, headers) : new HttpEntity<>(formBody, headers)  , 
	    			String.class);
	    	
//	    	System.out.println(s);
	    	return s;
    	
        } catch (final HttpClientErrorException e) {
        	e.printStackTrace();
            return new ResponseEntity<>(e.getResponseBodyAsByteArray(), e.getResponseHeaders(), e.getStatusCode());
        }

    }
    
    @GetMapping(value = "/{identifier}/view", produces = "text/turtle; charset=UTF-8")
    public ResponseEntity<?> view(HttpServletRequest request, @PathVariable("identifier") String identifier, @RequestParam("uri") String uri, @RequestParam(defaultValue = "turtle", name = "format") String format) {

    	GraphLocation gl = idService.getGraph(identifier);
    	if (gl == null || !gl.isPublik())  {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}

    	TripleStoreConfiguration vc;  // supports only one triple store
    	List<Resource> ics = new ArrayList<>();
    	
    	if (gl.getMainTripleStore() != null) {
    		vc = gl.getMainTripleStore();
    		ics.add(gl.getMainGraph());
    		
    		List<Resource> list = gl.getSubGraphMap().get(vc);
    		if (list != null) {
    			ics.addAll(list);
    		}

    	} else {
    		Map.Entry<TripleStoreConfiguration, List<Resource>> imap = gl.getSubGraphMap().entrySet().iterator().next();
    		
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
			
			String s = readEntity(vc, ics.get(0).getURI(), currentUri, uris, usedUris, format);
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
					if (vm.findPrefix(graph, newUri) != null) {
						uris.add(newUri);
					} else {
						usedUris.add(newUri);
					}
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
    }
    
    private IndexLocation getIndex(String identifier) {
 		Element e = indicesCache.get(identifier);

 		if (e != null) {
 			return (IndexLocation)e.getObjectValue();
 		}
 		
 		ElasticConfiguration ec = elasticConfigurations.values().iterator().next(); // supports one elastic;
 		
 		Optional<Dataset> datasetOpt = datasetsRepository.findByIdentifierAndDatabaseId(identifier, database.getId());
 		
 		if (!datasetOpt.isPresent()) {
 			return null;
 		}
 		
 		Dataset dataset = datasetOpt.get();

 		
 		IndexLocation dl = new IndexLocation();
 		
 		if (dataset.getDatasets() != null && dataset.getDatasets().size() > 0) {

 			for (ObjectId subDatasetId : dataset.getDatasets()) {
 		 		Optional<Dataset> subDatasetOpt = datasetsRepository.findById(subDatasetId);
 		 		
 		 		if (!datasetOpt.isPresent()) {
 		 			continue;
 		 		}
 		 		
 		 		Dataset subDataset = subDatasetOpt.get();

 	 	 		IndexState istate = subDataset.checkIndexState(ec.getId()); // supports one elastic;
 	 	 		if (istate == null || istate.getIndexState() != IndexingState.INDEXED) {
 	 	 			continue;
 	 	 		}
 	 	 		
 	 	 		Optional<IndexStructure> indexStructureOpt = indexStructureRepository.findById(istate.getIndexStructureId());
 		 		
 	 	 		if (!indexStructureOpt.isPresent()) {
 	 	 			continue;
 	 	 		}
 	 			
 		 		IndexStructure indexStructure = indexStructureOpt.get();
 		 		
 				dl.add(new IndexContainer(subDataset.getIdentifier(), indexStructure), ec);
 			}
 		
 			
 		} else {
 	 		IndexState istate = dataset.checkIndexState(ec.getId()); // supports one elastic;
 	 		if (istate == null || istate.getIndexState() != IndexingState.INDEXED) {
 	 			return null;
 	 		}

 	 		Optional<IndexStructure> indexStructureOpt = indexStructureRepository.findById(istate.getIndexStructureId());
 	 		
 	 		if (!indexStructureOpt.isPresent()) {
 	 			return null;
 	 		}
 			
	 		IndexStructure indexStructure = indexStructureOpt.get();
	 		
			dl.add(new IndexContainer(dataset.getIdentifier(), indexStructure), ec);
	     	
 		}
 		
 		if (dl != null) {
 			indicesCache.put(new Element(dataset.getIdentifier(), dl));
 		}
 		
 		return dl;


     }
     
    private static int INDEX_RESULTS_SIZE = 10;
    
    @GetMapping(value = "/index/{index}/phrase-prefix-search", 
    		    produces = "application/json")
    public ResponseEntity<?> indexPrefixLookup(HttpServletRequest request, @PathVariable("index") String index, @RequestParam("text") String text,  @RequestParam("keys") List<String> keys, @RequestParam("fields") Optional<List<String>> fields, @RequestParam("size") Optional<Integer> size) throws Exception {
    	
    	IndexLocation indexLoc =  getIndex(index);
    	if (indexLoc == null) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	// support currently one elastic
    	
    	Map.Entry<ElasticConfiguration, List<IndexContainer>> imap = indexLoc.indexMap.entrySet().iterator().next();
    	
    	ElasticConfiguration ec = imap.getKey();
    	List<IndexContainer> ics = imap.getValue();
    	
    	String[] indexNames = new String[ics.size()];
    	for (int i = 0; i < ics.size(); i++) {
    		indexNames[i] = database.getName() + "-" + ics.get(i).datasetIdentifier;
//    		System.out.println("INDEX " + indexNames[i]);
    	}
    	
//    	IndexStructure iStruct = indexLoc.indexStructure;
//    	
//    	Set<String> keys = iStruct.getKeys();
    	
    	try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ec.getIndexIp(), ec.getIndexPort(), "http")))) {
    	
    		
	    	SearchRequest searchRequest = new SearchRequest(indexNames);
			
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
			
//			BoolQueryBuilder bool = QueryBuilders.boolQuery();
			
			String[] ifields = new String[keys.size()];
			int i = 0;
			for (String r : keys) {
//				ifields[i++] = r + "-lexical-form";
				ifields[i++] = r;
			}
			
//			MultiMatchQueryBuilder tqb = QueryBuilders.multiMatchQuery(text, "r4-lexical-form", "r4-lexical-form._2gram", "r4-lexical-form._3gram");
//			tqb.type(Type.BOOL_PREFIX);
			MultiMatchQueryBuilder tqb = QueryBuilders.multiMatchQuery(text, ifields);
			tqb.type(Type.PHRASE_PREFIX);
//			bool.should(tqb);
			
			if (size.isPresent()) {
				sourceBuilder.size(size.get()); 
			} else {
				sourceBuilder.size(INDEX_RESULTS_SIZE);
			}
			
			sourceBuilder.query(tqb);
//			System.out.println(tqb);
			
			searchRequest.source(sourceBuilder);
				
			SearchHits hits = client.search(searchRequest, RequestOptions.DEFAULT).getHits();
				
			ObjectMapper mapper = new ObjectMapper();
			ArrayNode array = mapper.createArrayNode();

			List<String> ff = new ArrayList<>();
			if (fields.isPresent()) {
				ff = fields.get();
			}
			
			for (SearchHit hit : hits.getHits()) {
				Map<String, Object> map = hit.getSourceAsMap();

				ObjectNode match = mapper.createObjectNode();
				match.put("uri", map.get("iri").toString());
				match.put("score", hit.getScore());
				
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
					}
					
					match.put(f, arr);
				}
				
				array.add(match);
			}
			
			return ResponseEntity.ok(mapper.writeValueAsString(array));
    	}
    }
    
    @GetMapping(value = "/index/{index}/search", 
		        produces = "application/json")
    public ResponseEntity<?> indexLookup(HttpServletRequest request, @PathVariable("index") String index, @RequestParam("text") String text,  @RequestParam("fields") Optional<List<String>> fields,  @RequestParam("keys") Optional<List<String>> keys, @RequestParam("type") String type, @RequestParam("size") Optional<Integer> size) throws Exception {
    	
    	IndexLocation indexLoc =  getIndex(index);
    	if (indexLoc == null) {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
    	
    	Map.Entry<ElasticConfiguration, List<IndexContainer>> imap = indexLoc.indexMap.entrySet().iterator().next();
    	
    	ElasticConfiguration ec = imap.getKey();
    	List<IndexContainer> ics = imap.getValue();
    	
    	Set<String> iStructKeys = new HashSet<>();
    	
//    	Set<String> iStructKeys = iStruct.getKeys();
    	
    	String[] indexNames = new String[ics.size()];
    	for (int i = 0; i < ics.size(); i++) {
    		indexNames[i] = database.getName() + "-" + ics.get(i).datasetIdentifier;
    		
    		iStructKeys.addAll(ics.get(i).indexStructure.getKeys());
    	}
    	
    	try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ec.getIndexIp(), ec.getIndexPort(), "http")))) {
    	
	    	SearchRequest searchRequest = new SearchRequest(indexNames);
			
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
			
			BoolQueryBuilder bool = QueryBuilders.boolQuery();
			
			if (!keys.isPresent()) {
				for (String r : iStructKeys) {
					if (type.equals("term")) {
						bool.should(QueryBuilders.termQuery(r + "-lexical-form", text));
					} else if (type.equals("match")) {
						bool.should(QueryBuilders.matchQuery(r + "-lexical-form", text));
					} else if (type.equals("match-phrase")) {
						bool.should(QueryBuilders.matchPhraseQuery(r + "-lexical-form", text));
					}
				}
			} else {
				List<String> queryKeys = keys.get(); 
				for (String r : queryKeys) {
					if (type.equals("term")) {
						bool.should(QueryBuilders.termQuery(r, text));
					} else if (type.equals("match")) {
						bool.should(QueryBuilders.matchQuery(r, text));
					} else if (type.equals("match-phrase")) {
						bool.should(QueryBuilders.matchPhraseQuery(r, text));
					}
				}
			}
			
//			MultiMatchQueryBuilder tqb = QueryBuilders.multiMatchQuery(text, "r4-lexical-form", "r4-lexical-form._2gram", "r4-lexical-form._3gram");
//			tqb.type(Type.BOOL_PREFIX);
			
			if (size.isPresent()) {
				sourceBuilder.size(size.get()); 
			} else {
				sourceBuilder.size(INDEX_RESULTS_SIZE);
			}

			
			sourceBuilder.query(bool);
			
//			System.out.println(bool);
			
			searchRequest.source(sourceBuilder);
				
			SearchHits hits = client.search(searchRequest, RequestOptions.DEFAULT).getHits();
				
			ObjectMapper mapper = new ObjectMapper();
			ArrayNode array = mapper.createArrayNode();

			List<String> ff = new ArrayList<>();
			if (fields.isPresent()) {
				ff = fields.get();
			}
			
			for (SearchHit hit : hits.getHits()) {
				Map<String, Object> map = hit.getSourceAsMap();

				ObjectNode match = mapper.createObjectNode();
				match.put("uri", map.get("iri").toString());
				match.put("score", hit.getScore());
				
				for (String f : ff) {
					ArrayNode arr = mapper.createArrayNode();
					Object obj = map.get(f);
					if (obj instanceof Collection) {
						for (Object s : ((Collection)obj)) {
							arr.add(s.toString());
						}
					} else {
						arr.add(obj.toString());
					}
					
					match.put(f, arr);
				}
				
				array.add(match);
			}
			
			return ResponseEntity.ok(mapper.writeValueAsString(array));
    	}
    }
    
    @PostMapping(value = "/index/{index}/search", 
	        produces = "application/json")
	public ResponseEntity<?> postIndexLookup(HttpServletRequest request, @PathVariable("index") String index, @RequestBody String query,  @RequestParam("fields") Optional<List<String>> fields,  @RequestParam("size") Optional<Integer> size) throws Exception {
		
    	IndexLocation indexLoc =  getIndex(index);
		if (indexLoc == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		
    	Map.Entry<ElasticConfiguration, List<IndexContainer>> imap = indexLoc.indexMap.entrySet().iterator().next();
    	
    	ElasticConfiguration ec = imap.getKey();
    	List<IndexContainer> ics = imap.getValue();
    	
    	String[] indexNames = new String[ics.size()];
    	for (int i = 0; i < ics.size(); i++) {
    		indexNames[i] = database.getName() + "-" + ics.get(i).datasetIdentifier;
//    		System.out.println("INDEX " + indexNames[i]);
    	}
    	
		
		try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ec.getIndexIp(), ec.getIndexPort(), "http")))) {
		
	    	SearchRequest searchRequest = new SearchRequest(indexNames);

			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
			
			if (size.isPresent()) {
				sourceBuilder.size(size.get()); 
			} else {
				sourceBuilder.size(INDEX_RESULTS_SIZE);
			}
	
			
	    	searchRequest.source(
	    	        new SearchSourceBuilder().query(
	    	                QueryBuilders.wrapperQuery(query)
	    	        )
	    	);
	
				
			SearchHits hits = client.search(searchRequest, RequestOptions.DEFAULT).getHits();
				
			ObjectMapper mapper = new ObjectMapper();
			ArrayNode array = mapper.createArrayNode();
	
			List<String> ff = new ArrayList<>();
			if (fields.isPresent()) {
				ff = fields.get();
			}
			
			for (SearchHit hit : hits.getHits()) {
				Map<String, Object> map = hit.getSourceAsMap();
	
				ObjectNode match = mapper.createObjectNode();
				match.put("uri", map.get("iri").toString());
				match.put("score", hit.getScore());
				
				for (String f : ff) {
					ArrayNode arr = mapper.createArrayNode();
					Object obj = map.get(f);
					if (obj instanceof Collection) {
						for (Object s : ((Collection)obj)) {
							arr.add(s.toString());
						}
					} else {
						arr.add(obj.toString());
					}
					
					match.put(f, arr);
				}
				
				array.add(match);
			}
			
			return ResponseEntity.ok(mapper.writeValueAsString(array));
		}
	}    

}
