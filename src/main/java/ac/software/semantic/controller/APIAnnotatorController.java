package ac.software.semantic.controller;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import ac.software.semantic.payload.*;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotatorService;
import ac.software.semantic.service.ModelMapper;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Annotator API")

@RestController
@RequestMapping("/api/annotator")
public class APIAnnotatorController {
    
	@Autowired
    private AnnotatorService annotatorService;
	
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private AnnotationEditGroupRepository annotationEditGroupRepository;

	@Autowired
	private DataServiceRepository dataServiceRepository;

    @Autowired
    @Qualifier("database")
	private Database database;

    @Autowired
    @Qualifier("preprocess-functions")
    private Map<Resource, List<String>> functions;
    
    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;
    
	@GetMapping(value = "/services")
	public ResponseEntity<?> services(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		List<DataService> services = dataServiceRepository.findByDatabaseIdAndType(database.getId(), DataServiceType.ANNOTATOR);
        List<DataServiceResponse> response = services.stream()
        		.map(doc -> modelMapper.dataService2DataServiceResponse(doc))
        		.collect(Collectors.toList());

		return ResponseEntity.ok(response);
	}
	
	@GetMapping(value = "/functions")
	public ResponseEntity<?> functions(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		
		ObjectMapper mapper = new ObjectMapper();

		ArrayNode array = mapper.createArrayNode();
		for (Entry<Resource, List<String>> entry : functions.entrySet()) {
			ObjectNode object = mapper.createObjectNode();
			object.put("uri", entry.getKey().toString());
			
			List<String> ps = entry.getValue();
			if (ps.size() == 0) {
				continue;
			}
			
			ArrayNode params = mapper.createArrayNode();
			
			for (String p : entry.getValue()) {
				params.add(p);
			}
			object.put("parameters", params);
			array.add(object);
		}
		
		return ResponseEntity.ok(array);
	}
	
	@PostMapping(value = "/create")
//	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri, @RequestParam("onProperty") List<String> onProperty,  @RequestParam("asProperty") String asProperty, @RequestParam("annotator") String annotator, @RequestParam("thesaurus") Optional<String> thesaurus, @RequestBody List<DataServiceParameterValue> parameters)  {
	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestBody CreateAnnotatorRequest req)  {

		AnnotatorDocument adoc = annotatorService.createAnnotator(currentUser, req.getDatasetUri(), req.getOnProperty(), req.getAsProperty(), req.getAnnotator(), req.getThesaurus(), req.getParameters(), req.getPreprocess(), req.getVariant());
		
		AnnotationEditGroup aeg = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(SEMAVocabulary.getId(req.getDatasetUri()), req.getOnProperty(), req.getAsProperty(), new ObjectId(currentUser.getId())).get();
		
		return ResponseEntity.ok(modelMapper.annotator2AnnotatorResponse(virtuosoConfigurations.values(), adoc, aeg));
		
	} 
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<?> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		try {
			AsyncUtils.supplyAsync(() -> annotatorService.executeAnnotator(currentUser, id, applicationEventPublisher));
			
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

		}
		
	}

	@GetMapping("/{id}")
	public ResponseEntity<?> getAnnotator(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		try {
			Optional<AnnotatorDocumentResponse> annotatorOpt = annotatorService.getAnnotator(currentUser, id);
			if (annotatorOpt.isPresent()) {
				return ResponseEntity.ok(annotatorOpt.get());
			}
			else {
				return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
			}
		} catch (Exception e) {
			e.printStackTrace();

			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@PutMapping("{id}")
	public ResponseEntity<?> updateAnnotator(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody UpdateAnnotatorRequest params)  {
		try {
			AnnotatorDocument updatedAnnotator = annotatorService.updateAnnotator(currentUser, id, params);
			if (updatedAnnotator != null) {
				return ResponseEntity.ok(updatedAnnotator);
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@DeleteMapping(value = "/delete/{id}",
			produces = "application/json")
	public ResponseEntity<?> delete(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id) {

		boolean deleted = annotatorService.deleteAnnotator(currentUser, id);

		if (deleted) {
			return ResponseEntity.ok(new ApiResponse(true, "Annotator deleted"));
		} else {
			return ResponseEntity.ok(new ApiResponse(false, "Current user is not owner of catalog"));
		}
	}
    
  @PostMapping(value = "/publish")
	public ResponseEntity<?> publish(@CurrentUser UserPrincipal currentUser, @RequestParam("id") String ids)  {
		
		try {
			
			String[] idss = ids.split(",");
			
			AsyncUtils.supplyAsync(() -> annotatorService.publish(currentUser, idss))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
				   for (String id : idss) {
					   AnnotatorDocumentResponse doc = annotatorService.getAnnotator(currentUser, id).get();
					   
					   ObjectMapper mapper = new ObjectMapper();
					   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
					   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
					    
					   NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHED_PUBLIC.toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
								
						try {
							SseEventBuilder sse = SseEmitter.event().name("annotator").data(mapper.writeValueAsBytes(no));
						
							applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
						} catch (JsonProcessingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				   }
			   });
			
//			System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}

//		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		
	} 	
	
	@PostMapping(value = "/unpublish")
	public ResponseEntity<?> unpublish(@CurrentUser UserPrincipal currentUser, @RequestParam("id") String ids)  {
		
		String[] idss = ids.split(",");
		
		try {		
			AsyncUtils.supplyAsync(() -> annotatorService.unpublish(currentUser, idss))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
				   for (String id : idss) {
//					   AnnotatorResponse doc = annotatorService.getAnnotator(currentUser, id).get();
	
					   ObjectMapper mapper = new ObjectMapper();
					   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
					   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
					    
					   NotificationObject no = new NotificationObject("publish", DatasetState.UNPUBLISHED.toString(), id, null, null, null);
								
						try {
							SseEventBuilder sse = SseEmitter.event().name("annotator").data(mapper.writeValueAsBytes(no));
						
							applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
						} catch (JsonProcessingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				   }
			   });
			
//			System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
			
		
//		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		
	} 	

	@Autowired
	AnnotatorDocumentRepository annotatorRepository;
	
    @GetMapping(value = "/preview/{id}", produces = "application/json")
    public ResponseEntity<?> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(value="page", defaultValue="1") int page, @RequestParam(value="mode", defaultValue="ALL") AnnotationValidationRequest mode)  {

		Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		try {
	
			if (doc.isPresent()) {
				AnnotatorDocument adoc = doc.get();
				org.apache.jena.query.Dataset rdfDataset = annotatorService.load(currentUser, adoc);
				Collection<ValueAnnotation> res = annotatorService.view(currentUser, doc.get(), rdfDataset, page);

				return ResponseEntity.ok(res);
			} else {
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}			
    } 
    
    @GetMapping(value = "/lastExecution/{id}",
            produces = "text/plain")
	public ResponseEntity<?> previewLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> ttl = annotatorService.getLastExecution(currentUser, id);
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

    
    @GetMapping(value = "/downloadLastExecution/{id}")
	public ResponseEntity<?> downloadLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> zip = annotatorService.downloadLastExecution(currentUser, id);
			
			if (zip.isPresent()) {
				String file = zip.get();
				Path path = Paths.get(file);
				File ffile = new File(file);
		      	
			    ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(path));

			    HttpHeaders headers = new HttpHeaders();
			    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
			    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + ffile.getName());
			    		
			    return ResponseEntity.ok()
			            .headers(headers)
			            .contentLength(ffile.length())
			            .contentType(MediaType.APPLICATION_OCTET_STREAM)
			            .body(resource);

			} else {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}    
	
	@GetMapping(value = "/getAllByUser")
	public ResponseEntity<?> getAllByUser(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotatorDocumentResponse> docs = annotatorService.getAnnotators(currentUser, datasetUri);
		
		return ResponseEntity.ok(docs);
		
	}

	@GetMapping(value = "/getAll")
	public ResponseEntity<?> getAll(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotatorDocumentResponse> docs = annotatorService.getAnnotators(datasetUri);

		return ResponseEntity.ok(docs);

	}
	
	
}
