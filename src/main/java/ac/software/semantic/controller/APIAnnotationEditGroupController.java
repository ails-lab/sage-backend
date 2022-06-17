package ac.software.semantic.controller;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.AnnotationEditGroupResponse;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Annotation Edit Group API")
@RestController
@RequestMapping("/api/annotation-edit-group")
public class APIAnnotationEditGroupController {
    
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

//    @Autowired
//    @Qualifier("virtuoso-configuration")
//    private VirtuosoConfiguration virtuosoConfiguration;

	@Autowired
	private AnnotationEditGroupService aegService;

	@Autowired
	private AnnotationEditGroupRepository aegRepository;
//
//    @Autowired
//    private ApplicationEventPublisher applicationEventPublisher;

	public enum AnnotationValidationRequest {
		ALL,
		ANNOTATED_ONLY,
		UNANNOTATED_ONLY,
	}



	@GetMapping(value = "/getAllByUser")
	public ResponseEntity<?> getAllByUser(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotationEditGroupResponse> docs = aegService.getAnnotationEditGroups(currentUser, datasetUri);
		
		return ResponseEntity.ok(docs);
	}

	@GetMapping(value = "/getAll")
	public ResponseEntity<?> getAll(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotationEditGroupResponse> docs = aegService.getAnnotationEditGroups(datasetUri);

		return ResponseEntity.ok(docs);
	}

    @GetMapping(value = "/view/{id}", produces = "application/json")
    public ResponseEntity<?> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(value="page", defaultValue="1") int page, @RequestParam(value="mode", defaultValue="ALL") AnnotationValidationRequest mode, @RequestParam String annotators)  {

    	Collection<ValueAnnotation> res = aegService.view(currentUser, id, mode, page, annotators);
		
		return ResponseEntity.ok(res);
    } 

//	@PostMapping(value = "/execute/{id}")
//	public ResponseEntity<?> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//		try {
//			AsyncUtils.supplyAsync(() -> aegService.execute(currentUser, id, applicationEventPublisher));
//			
//			return new ResponseEntity<>(HttpStatus.ACCEPTED);
//		} catch (Exception e) {
//			e.printStackTrace();
//			
//			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//		}
//		
//	}
//	
//	  @PostMapping(value = "/publish/{id}")
//		public ResponseEntity<?> publish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//			
//			try {
//				
//				AsyncUtils.supplyAsync(() -> aegService.publish(currentUser, id))
//				   .exceptionally(ex -> { ex.printStackTrace(); return false; })
//				   .thenAccept(ok -> {
//					   AnnotationEditGroup doc = aegRepository.findById(new ObjectId(id)).get();
//					   PublishState ps = doc.getPublishState(virtuosoConfiguration.getDatabaseId());
//					   
//					   ObjectMapper mapper = new ObjectMapper();
//					   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//					   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//					    
//					   NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHED_PUBLIC.toString(), id, null, ps.getPublishStartedAt(), ps.getPublishCompletedAt());
//								
//						try {
//							SseEventBuilder sse = SseEmitter.event().name("edits").data(mapper.writeValueAsBytes(no));
//						
//							applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//						} catch (JsonProcessingException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//				   });
//				
////				System.out.println("PUBLISHING ACCEPTED");
//				return new ResponseEntity<>(HttpStatus.ACCEPTED);
//			} catch (Exception e) {
//				e.printStackTrace();
//				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//			}
//		}
//	  
//		@PostMapping(value = "/unpublish/{id}")
//		public ResponseEntity<?> unpublish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//			
//			try {		
//				AsyncUtils.supplyAsync(() -> aegService.unpublish(currentUser, id))
//				   .exceptionally(ex -> { ex.printStackTrace(); return false; })
//				   .thenAccept(ok -> {
//					   AnnotationEditGroup doc = aegRepository.findById(new ObjectId(id)).get();
//		
//					   ObjectMapper mapper = new ObjectMapper();
//					   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//					   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//						    
//					   NotificationObject no = new NotificationObject("publish", DatasetState.UNPUBLISHED.toString(), id, null, null, null);
//									
//						try {
//							SseEventBuilder sse = SseEmitter.event().name("edits").data(mapper.writeValueAsBytes(no));
//						
//							applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//						} catch (JsonProcessingException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//				   });
//				
////				System.out.println("PUBLISHING ACCEPTED");
//				return new ResponseEntity<>(HttpStatus.ACCEPTED);
//			} catch (Exception e) {
//				e.printStackTrace();
//				
//				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//			}
//		} 		
//	
//    @GetMapping(value = "/lastExecution/{id}",
//            produces = "text/plain")
//	public ResponseEntity<?> lastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//	
//		try {
//			Optional<String> ttl = aegService.getLastExecution(currentUser, id);
//			if (ttl.isPresent()) {
//				return ResponseEntity.ok(ttl.get());
//			} else {
//				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//		}
//    }
    
	@GetMapping(value = "/downloadValues/{id}")
	public ResponseEntity<?> downloadAnnotationValues(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(name = "mode", defaultValue = "ALL") String mode) {

		try {
			ByteArrayResource resource = aegService.downloadAnnotationValues(currentUser, id, mode);
			if (resource != null) {
	
				AnnotationEditGroup aeg = aegRepository.findById(new ObjectId(id)).get();
				
				HttpHeaders headers = new HttpHeaders();
				headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
				headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + aeg.getUuid() + ".zip");
	
				return ResponseEntity.ok().headers(headers)
	//	            .contentLength(ffile.length())
						.contentType(MediaType.APPLICATION_OCTET_STREAM).body(resource);

			} else {
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	
	}
    
}
