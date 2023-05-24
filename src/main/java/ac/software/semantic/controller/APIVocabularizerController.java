package ac.software.semantic.controller;

import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Parameter;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.AsyncUtils;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.VocabularizerDocument;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.payload.VocabularizerRequest;
import ac.software.semantic.payload.VocabularizerResponse;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.VocabularizerRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.VocabularizerService;
import ac.software.semantic.vocs.SEMRVocabulary;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Vocabulizer API")
@RestController
@RequestMapping("/api/vocabularizer")
public class APIVocabularizerController {
    
	@Autowired
    private VocabularizerService vocabularizerService;

	@Autowired
    private DatasetRepository datasetRepository;

	@Autowired
    private VocabularizerRepository vocabularizerRepository;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
	
	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
    @Autowired
    private WebSocketService wsService;

	
	
	@PostMapping(value = "/create")
	public ResponseEntity<?> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody VocabularizerRequest vr)  {

		VocabularizerDocument adoc = vocabularizerService.createVocabularizer(currentUser, vr.getDatasetUri(), vr.getOnProperty(), vr.getName(), vr.getSeparator());
		
		return ResponseEntity.ok(modelMapper.vocabularizer2VocabularizerResponse(adoc));
		
	} 
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<?> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		try {
			
			AsyncUtils.supplyAsync(() -> vocabularizerService.executeVocabularizer(currentUser, id, wsService));
			
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

		}
		
	}
//	
    @DeleteMapping(value = "/delete/{id}",
	           produces = "application/json")
	public ResponseEntity<?> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		boolean deleted = vocabularizerService.deleteVocabularizer(currentUser, id);
		
		if (deleted) {
			return ResponseEntity.ok(new APIResponse(true, "Vocabularizer deleted"));
		} else {
			return ResponseEntity.ok(new APIResponse(false, "Current user is not owner of catalog"));
		}
	}

    @PostMapping(value = "/publish/{id}")
	public ResponseEntity<?> publish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
		try {
			
			AsyncUtils.supplyAsync(() -> vocabularizerService.publish(currentUser, id))
			   .exceptionally(ex -> { 
//				   	System.out.println("FAILURE");
					ObjectMapper mapper = new ObjectMapper();
					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
					NotificationObject no = new NotificationObject(NotificationType.publish, DatasetState.PUBLISHING_FAILED.toString(), id, null, null, null);
							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("vocabularizer").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
					wsService.send(NotificationChannel.vocabularizer, currentUser, no);
				   
				   ex.printStackTrace(); 
				   return false; 
				})			   
			   .thenAccept(ok -> {
				   VocabularizerResponse doc = vocabularizerService.getVocabularizer(currentUser, id).get();
				   
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject(NotificationType.publish, DatasetState.PUBLISHED_PUBLIC.toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("vocabularizer").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
				   wsService.send(NotificationChannel.vocabularizer, currentUser, no);
			   });
			
//			System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	} 	
	
	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<?> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
		try {		
			AsyncUtils.supplyAsync(() -> vocabularizerService.unpublish(currentUser, id))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
//					   AnnotatorResponse doc = annotatorService.getAnnotator(currentUser, id).get();

				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject(NotificationType.publish, DatasetState.UNPUBLISHED.toString(), id, null, null, null);
							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("annotator").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
				   wsService.send(NotificationChannel.vocabularizer, currentUser, no);
			   });
			
//			System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
			
		
//		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	} 	


//    @PostMapping(value = "/index/{id}")
//	public ResponseEntity<?> indexDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//		
//		try {
//			AsyncUtils.supplyAsync(() -> vocabularizerService.index(currentUser, id))
//			   .exceptionally(ex -> { 
////				   	System.out.println("FAILURE");
//					ObjectMapper mapper = new ObjectMapper();
//					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//				    
//					NotificationObject no = new NotificationObject("index", IndexingState.INDEXING_FAILED.toString(), id, null, null, null);
//							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("vocabularizer").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				   
//				   ex.printStackTrace(); 
//				   return false; 
//				})
//			   .thenAccept(ok -> {
//				   VocabularizerResponse doc = vocabularizerService.getVocabularizer(currentUser, id).get();
//				   
//				   ObjectMapper mapper = new ObjectMapper();
//				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//				    
//				   NotificationObject no = new NotificationObject("index", IndexingState.INDEXED.toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
//							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("vocabularizer").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//			   });
//			
////			System.out.println("PUBLISHING ACCEPTED");
//			return new ResponseEntity<>(HttpStatus.ACCEPTED);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
//	}     
//    
//    @PostMapping(value = "/unindex/{id}")
//	public ResponseEntity<?> unindexDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//		
//		try {
//			AsyncUtils.supplyAsync(() -> vocabularizerService.unindex(currentUser, id))
//			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
//			   .thenAccept(ok -> {
//				   ObjectMapper mapper = new ObjectMapper();
//				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//				    
//				   NotificationObject no = new NotificationObject("index", IndexingState.NOT_INDEXED.toString(), id, null, null, null);
//							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("vocabularizer").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//			   });
//			
////			System.out.println("UNPUBLISHING ACCEPTED");
//			return new ResponseEntity<>(HttpStatus.ACCEPTED);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	
//		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
//		
//    } 	
    
	@GetMapping(value = "/lastExecution/{id}",
            produces = "text/plain")
	public ResponseEntity<?> lastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> ttl = vocabularizerService.getLastExecution(currentUser, id);
			if (ttl.isPresent()) {
				return ResponseEntity.ok(ttl.get());
			} else {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	
	}
    
    @Autowired
    RestTemplate restTemplate;
    
	@GetMapping(value = "/cleanup/{id}")
	public ResponseEntity<?> cleanup(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		Optional<VocabularizerDocument> adoc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		if (adoc.isPresent()) {
			String graph  = resourceVocabulary.getDatasetAsResource(adoc.get().getUuid()).toString();
			
			String arr = restTemplate.getForObject("http://apps.islab.ntua.gr/inknowledge/api/graph-voc-equiv?graph=" + graph, String.class);
			
//			String sparql = "SELECT ?s ?o WHERE " +
//                            "GRAPH <" + SEMAVocabulary.contentGraph + "> { " + 
//					           "<" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/class>/<http://sw.islab.ntua.gr/apollonis/ms/uri> ?clazz . " +
//                               "<" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> [ " +
//			                         " <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> ; " +
//	                                 " <http://sw.islab.ntua.gr/apollonis/ms/uri> ?label ] } " +
//			                 "GRAPH <" + graph + "> { ?s a ?clazz . ?s ?label ?o } }";
//	                                 
//	
//			QueryExecution qe = QueryExecutionFactory.sparqlService(ApollonisSources.RDF_STORE, QueryFactory.create(sparql));
//	
//			Map<String, Set<String>> map = new HashMap<>();
//			ResultSet rs = qe.execSelect();
//			
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//				String item = sol.get("s").asResource().getURI().toString();
//				String text = sol.get("o").asLiteral().getLexicalForm();
//			
//				Set<String> set = map.get(item);
//				
//				Keyword kw = new Keyword(text);
//				Set<String> resources = keywordsToItems.get(kw);
//				if (resources == null) {
//					resources = new HashSet<>();
//					keywordsToItems.put(kw, resources);
//				}
//				resources.add(item);
//			}
			
			return ResponseEntity.ok(arr);
		}
		
		return ResponseEntity.ok("[]");
		
	} 
	
	@GetMapping(value = "/vocabulary/{id}")
	public ResponseEntity<?> vocabulary(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		Optional<VocabularizerDocument> adoc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		if (adoc.isPresent()) {
			VocabularizerDocument voc = adoc.get();
			
		    Dataset ds = datasetRepository.findByUuid(voc.getDatasetUuid()).get();
		    TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			String graph  = resourceVocabulary.getDatasetAsResource(voc.getUuid()).toString();
			
			String sparql = "CONSTRUCT { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?o } WHERE { " +
                            "GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " + 
					           "<" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/class>/<http://sw.islab.ntua.gr/apollonis/ms/uri> ?clazz . " +
                               "<" + graph + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> [ " +
			                         " <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> ; " +
	                                 " <http://sw.islab.ntua.gr/apollonis/ms/uri> ?label ] } " +
			                 "GRAPH <" + graph + "> { ?s a ?clazz . ?s ?label ?o } }";

			Writer sw = new StringWriter();

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
				Model model = qe.execConstruct();
				RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
			}
			
//			System.out.println(sw);
			
	        return ResponseEntity.ok(sw.toString());
			
		}
		
		return ResponseEntity.ok("[]");
		
	} 	

//	
	@GetMapping(value = "/getAll")
	public ResponseEntity<?> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<VocabularizerResponse> docs = vocabularizerService.getVocabularizers(currentUser, datasetUri);
		
		return ResponseEntity.ok(docs);
		
	}
	
}
