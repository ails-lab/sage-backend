package ac.software.semantic.controller;

import java.util.Map;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.FilterValidationUpdateRequest;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FilterAnnotationValidationRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.FilterAnnotationValidationService;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Filter Annotation Validation API")
@RestController
@RequestMapping("/api/filter-annotation-validation")
public class APIFilterAnnotationValidationController {

	@Autowired
	private ModelMapper modelMapper;
	
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;
    
	@Autowired
	private FilterAnnotationValidationService favService;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private FilterAnnotationValidationRepository favRepository;
	
	
	@PostMapping(value = "/create", produces = "application/json")
	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestParam("aegId") String aegId, @RequestBody FilterValidationUpdateRequest fur) {

		FilterAnnotationValidation fav = favService.create(currentUser, aegId, fur.getName(), fur.getFilters());
		
		return ResponseEntity.ok(modelMapper.filterAnnotationValidation2FilterAnnotationValidationResponse(virtuosoConfigurations.values(), fav));
	}

	@PostMapping(value = "/update/{id}", produces = "application/json")
	public ResponseEntity<?> update(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody FilterValidationUpdateRequest fur) {

		FilterAnnotationValidation fav = favService.update(currentUser, id, fur.getName(), fur.getFilters());
		
		return ResponseEntity.ok(modelMapper.filterAnnotationValidation2FilterAnnotationValidationResponse(virtuosoConfigurations.values(), fav));
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<?> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		try {
			AsyncUtils.supplyAsync(() -> favService.executeNoDelete(currentUser, id, applicationEventPublisher));
			
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

		}
	}
	
	@GetMapping(value = "/lastExecution/{id}",  produces = "text/plain")
	public ResponseEntity<?> lastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> ttl = favService.getLastExecution(currentUser, id);
			if (ttl.isPresent()) {
				return ResponseEntity.ok(ttl.get());
			} else {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@PostMapping(value = "/publish/{id}")
	public ResponseEntity<?> publish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
		try {
			AsyncUtils.supplyAsync(() -> favService.republishNoDelete(currentUser, id))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
				   FilterAnnotationValidation doc = favRepository.findById(new ObjectId(id)).get();
				   
				   Dataset ds = datasetRepository.findByUuid(doc.getDatasetUuid()).get();
				   VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

				   PublishState ps = doc.getPublishState(vc.getDatabaseId());
				   
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHED_PUBLIC.toString(), id, null, ps.getPublishStartedAt(), ps.getPublishCompletedAt());
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("filter-annotation-validation").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			   });
			
//				System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<?> unpublish(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
		try {
			AsyncUtils.supplyAsync(() -> favService.unpublishNoDelete(currentUser, id))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
//				   FilterAnnotationValidation doc = favRepository.findById(new ObjectId(id)).get();
				   
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("publish", DatasetState.UNPUBLISHED.toString(), id, null, null, null);
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("filter-annotation-validation").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			   });
			
//				System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

}