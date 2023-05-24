package ac.software.semantic.controller;

import java.util.List;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.payload.IndexDocumentResponse;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.IndexService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Index API")


@RestController
@RequestMapping("/api/index")
public class APIIndexController {

    @Autowired
    private IndexService indexService;

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

//	@PostMapping(value = "/index") // for apollonis compatibility should be removed
//	public ResponseEntity<?> indexOld(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("uri") String uri)  {
//
//		try {
//			
//			AsyncUtils.supplyAsync(() -> indexService.indexCollectionOld(currentUser, uri))
//			   .exceptionally(ex -> { 
////				   ObjectMapper mapper = new ObjectMapper();
////					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
////				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
////				    
////					NotificationObject no = new NotificationObject("index", IndexingState.INDEXING_FAILED.toString(), id, null, null, null);
////							
////					try {
////						SseEventBuilder sse = SseEmitter.event().name("indexer").data(mapper.writeValueAsBytes(no));
////					
////						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
////					} catch (JsonProcessingException e) {
////						// TODO Auto-generated catch block
////						e.printStackTrace();
////					}
////				   
////				   ex.printStackTrace(); 
//				   return false; 
//			   })
//			   .thenAccept(ok -> {
//				   String datasetUuid = SEMAVocabulary.getId(uri);
//				   IndexDocumentResponse doc = indexService.getIndex(currentUser, uri).get();
//				   
//				   ObjectMapper mapper = new ObjectMapper();
//					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//				    
//					NotificationObject no = new NotificationObject("index", IndexingState.INDEXED.toString(), datasetUuid, null, doc.getIndexStartedAt(), doc.getIndexCompletedAt());
//							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("index").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				   
//				   System.out.println("INDEXING COMPLETED");
//					   		    
//			   });
//			
//			return new ResponseEntity<>(HttpStatus.ACCEPTED);
//		} catch (Exception e) {
//			e.printStackTrace();
//			
//			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//
//		}
//		
//	}
//	
//	@PostMapping(value = "/unindex")
//	public ResponseEntity<?> unindex(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("uri") String uri)  {
//
//		try {
//			
//			AsyncUtils.supplyAsync(() -> indexService.unindexCollection(currentUser, uri))
//			   .exceptionally(ex -> { 
//////				   ObjectMapper mapper = new ObjectMapper();
//////					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//////				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//////				    
//////					NotificationObject no = new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), id, null, null, null);
//////							
//////					try {
//////						SseEventBuilder sse = SseEmitter.event().name("vocabularizer").data(mapper.writeValueAsBytes(no));
//////					
//////						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//////					} catch (JsonProcessingException e) {
//////						// TODO Auto-generated catch block
//////						e.printStackTrace();
//////					}
////				   
//				   ex.printStackTrace(); 
//				   return false; 
//			   })
//			   .thenAccept(ok -> {
//				   String datasetUuid = SEMAVocabulary.getId(uri);
//				   IndexDocumentResponse doc = indexService.getIndex(currentUser, uri).get();
//				   
//				   ObjectMapper mapper = new ObjectMapper();
//					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
//				    
//					NotificationObject no = new NotificationObject("index", IndexingState.NOT_INDEXED.toString(), datasetUuid, null, doc.getIndexStartedAt(), doc.getIndexCompletedAt());
//							
//					try {
//						SseEventBuilder sse = SseEmitter.event().name("index").data(mapper.writeValueAsBytes(no));
//					
//						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//					} catch (JsonProcessingException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				   
//				   System.out.println("UNINDEXING COMPLETED");
//					   		    
//			   });
//			
//			return new ResponseEntity<>(HttpStatus.ACCEPTED);
//		} catch (Exception e) {
//			e.printStackTrace();
//			
//			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//
//		}
//		
//	}	

//	
//	@GetMapping(value = "/getAll")
//	public ResponseEntity<?> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {
//
//		IndexDocumentResponse docs = indexService.getIndexes(currentUser, datasetUri);
//		if (docs != null ) {
//			return ResponseEntity.ok(docs);
//		} else {
//			return ResponseEntity.ok("{}");		}
//		
//	}

	@GetMapping(value = "/getAll")
	public ResponseEntity<?> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {

		List<IndexStructure> indices = indexService.getIndices();
		
		return ResponseEntity.ok(indices);
	}
	
}
