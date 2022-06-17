package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import ac.software.semantic.payload.*;
import ac.software.semantic.service.PagedAnnotationValidationPageLocksService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

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

import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.AnnotationEditResponse;
import ac.software.semantic.payload.PagedAnnotationValidatationDataResponse;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepositoryPage;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditService;
import ac.software.semantic.service.PagedAnnotationValidationService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Paged Annotation Validation API")
@RestController
@RequestMapping("/api/paged-annotation-validation")
public class APIPagedAnnotationValidationController {

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	PagedAnnotationValidationRepository pavRepository;

	@Autowired
	PagedAnnotationValidationRepositoryPage pavpRepository;

	@Autowired
	AnnotationEditGroupRepository aegRepository;

	@Autowired
	private PagedAnnotationValidationService pavService;

	@Autowired
	AnnotationEditService annotationEditService;

	@Autowired
	PagedAnnotationValidationPageLocksService locksService;
	
    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;
    
	public enum PageRequestMode {
		UNANNOTATED_ONLY_SPECIFIC_PAGE,
		UNANNOTATED_ONLY_SERIAL,
		ANNOTATED_ONLY_SERIAL,
		ANNOTATED_ONLY_SPECIFIC_PAGE,
		ANNOTATED_ONLY_NOT_COMPLETE,
		ANNOTATED_ONLY_NOT_VALIDATED
	}

	public enum NavigatePageMode {
		RIGHT,
		LEFT
	}

	@PostMapping(value = "/create", produces = "application/json")
	public ResponseEntity<?> create(@CurrentUser UserPrincipal currentUser, @RequestParam("aegId") String aegId) {

		try {
			AsyncUtils.supplyAsync(() -> pavService.createPagedAnnotationValidation(currentUser, aegId));

			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();

			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@Operation(summary = "Get progress on validation procedure", security = @SecurityRequirement(name = "bearerAuth"))
	@GetMapping(value = "/progress/{id}", produces = "application/json")
	public ResponseEntity<?> progress(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id) {
		ProgressResponse progressResponse = pavService.getProgress(currentUser, id);
		if (progressResponse != null) {
			return ResponseEntity.status(HttpStatus.OK).body(progressResponse);
		}
		else {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}

	@Operation(summary = "Get progress of validation on all dataset properties", security = @SecurityRequirement(name = "bearerAuth"))
	@GetMapping(value = "/datasetProgress/{uuid}", produces = "application/json")
	public ResponseEntity<?> datasetProgress(@CurrentUser UserPrincipal currentUser, @PathVariable("uuid") String uuid) {
		List<DatasetProgressResponse> res;
		res = pavService.getDatasetProgress(currentUser, uuid);
		return ResponseEntity.ok(res);
	}

	@Operation(summary = "Request a page to perform validation.", security = @SecurityRequirement(name = "bearerAuth"))
	@GetMapping(value = "/view/{id}", produces = "application/json")
	public ResponseEntity<?> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(required = false) Integer currentPage, @RequestParam PageRequestMode mode,@RequestParam(required = false) NavigatePageMode navigation, @RequestParam(required = false) Integer requestedPage) {
		PagedAnnotationValidatationDataResponse res;

		if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SERIAL) ||
			mode.equals(PageRequestMode.ANNOTATED_ONLY_SERIAL) ||
			mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED) ||
			mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)){
			res = pavService.determinePageSerial(currentUser, id, currentPage, mode, navigation);
		}
		else if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE) || mode.equals(PageRequestMode.ANNOTATED_ONLY_SPECIFIC_PAGE)) {
			res = pavService.getSpecificPage(currentUser, id, requestedPage, mode, currentPage);
		}
		else {
			res = null;
		}

		return ResponseEntity.ok(res);
	}

	@PostMapping(value = "/commit-page/{pavpid}", produces = "application/json")
	public ResponseEntity<?> commitPage(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("pavpid") String pavpId, @RequestParam String lockId, @RequestBody List<RefractoredAnnotationEditResponse> edits) {

		Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findById(pavpId);
		if (!pavpOpt.isPresent()) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

		PagedAnnotationValidationPage pavp = pavpOpt.get();

		if(!locksService.checkForLock(currentUser, pavp.getPagedAnnotationValidationId().toString(), lockId, pavp.getMode(), pavp.getPage())) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(pavp.getAnnotationEditGroupId());
		if (!aegOpt.isPresent()) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

		AnnotationEditResponse tmp;
		List<AnnotationEditResponse> mappedList = new ArrayList<>();
		for (RefractoredAnnotationEditResponse res : edits) {
			for (RefractoredAnnotationEditDetails det : res.getEdits()) {
				tmp = new AnnotationEditResponse(det, res.getPropertyValue());
				mappedList.add(tmp);
			}
		}

		for (AnnotationEditResponse vad : mappedList) {
			annotationEditService.processEdit(currentUser, aegOpt.get(), vad, pavp);
		}

		pavpRepository.save(pavp);

		return new ResponseEntity<>(HttpStatus.OK);
	}

	@PostMapping(value = "/endValidation/{pavid}", produces = "application/json")
	public ResponseEntity<?> endValidation(@CurrentUser UserPrincipal currentUser, @PathVariable("pavid") String pavId) {
		boolean result = pavService.endValidation(pavId);
		if (result) {
			return new ResponseEntity<>(HttpStatus.OK);
		} else {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<?> execute(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		try {
			AsyncUtils.supplyAsync(() -> pavService.executeNoDelete(currentUser, id, applicationEventPublisher));
			
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
			
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

		}
	}
	
	@GetMapping(value = "/lastExecution/{id}",  produces = "text/plain")
	public ResponseEntity<?> lastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> ttl = pavService.getLastExecution(currentUser, id);
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
			AsyncUtils.supplyAsync(() -> pavService.republishNoDelete(currentUser, id))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
				   PagedAnnotationValidation doc = pavRepository.findById(new ObjectId(id)).get();
					ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(doc.getDatasetUuid()).get();
					VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

				   PublishState ps = doc.getPublishState(vc.getDatabaseId());
				   
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHED_PUBLIC.toString(), id, null, ps.getPublishStartedAt(), ps.getPublishCompletedAt());
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("paged-annotation-validation").data(mapper.writeValueAsBytes(no));
					
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
			AsyncUtils.supplyAsync(() -> pavService.unpublishNoDelete(currentUser, id))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
//				   PagedAnnotationValidation doc = pavRepository.findById(new ObjectId(id)).get();
				   
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("publish", DatasetState.UNPUBLISHED.toString(), id, null, null, null);
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("paged-annotation-validation").data(mapper.writeValueAsBytes(no));
					
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