package ac.software.semantic.controller;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.constants.SerializationType;
import ac.software.semantic.payload.AnnotationEditGroupResponse;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueResponseContainer;
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

	@Autowired
	private AnnotationEditGroupService aegService;

	@Autowired
	private AnnotationEditGroupRepository aegRepository;

	public enum AnnotationValidationRequest {
		ALL,
		ANNOTATED_ONLY,
		UNANNOTATED_ONLY,
	}



	@GetMapping(value = "/get-all-by-user")
	public ResponseEntity<?> getAllByUser(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotationEditGroupResponse> docs = aegService.getAnnotationEditGroups(currentUser, datasetUri);
		
		return ResponseEntity.ok(docs);
	}

	@GetMapping(value = "/get-all")
	public ResponseEntity<?> getAll(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri)  {

		List<AnnotationEditGroupResponse> docs = aegService.getAnnotationEditGroups(datasetUri);

		return ResponseEntity.ok(docs);
	}

    @GetMapping(value = "/view/{id}", produces = "application/json")
    public ResponseEntity<?> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(value="page", defaultValue="1") int page, @RequestParam(value="mode", defaultValue="ALL") AnnotationValidationRequest mode, @RequestParam String annotators)  {

    	ValueResponseContainer<ValueAnnotation> res = aegService.view(currentUser, id, mode, page, annotators);
		
		return ResponseEntity.ok(res);
    }
    
    @PostMapping(value = "/update/{id}")
    public ResponseEntity<?> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, boolean autoexportable)  {

    	Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(id));
    	if (aegOpt.isPresent()) {
    		AnnotationEditGroup aeg = aegOpt.get();
    		aeg.setAutoexportable(autoexportable);
    		aegRepository.save(aeg);
    	} else {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
		
		return ResponseEntity.ok().build();
    } 

    
	@GetMapping(value = "/export-annotations-validations/{id}")
	public ResponseEntity<?> downloadAnnotationValues(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, 
			@RequestParam(required = false, defaultValue = "JSON-LD") String serialization, 
			@RequestParam(required = false, defaultValue = "false") boolean onlyReviewed, 
			@RequestParam(required = false, defaultValue = "true") boolean onlyNonRejected,
			@RequestParam(required = false, defaultValue = "true") boolean onlyFresh,
			@RequestParam(required = false, defaultValue = "true") boolean created,
			@RequestParam(required = false, defaultValue = "true") boolean creator,
			@RequestParam(required = false, defaultValue = "true") boolean score,
			@RequestParam(required = false, defaultValue = "true") boolean scope, 
			@RequestParam(required = false, defaultValue = "true") boolean selector, 
//			@RequestParam(required = false) String defaultScope,
			@RequestParam(required = false, defaultValue = "TGZ") String archive
			) {

		try {
//			ByteArrayResource resource = aegService.exportAnnotations(currentUser, id, SerializationType.get(serialization), onlyReviewed, onlyNonRejected, created, creator, score, scope, selector, defaultScope, archive) ;
			ByteArrayResource resource = aegService.exportAnnotations(currentUser, id, SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, archive) ;
			
			if (resource != null) {
	
				AnnotationEditGroup aeg = aegRepository.findById(new ObjectId(id)).get();
				
				HttpHeaders headers = new HttpHeaders();
				headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
				headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + aeg.getUuid() + "." + archive.toLowerCase());
	
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
	
//	@GetMapping(value = "/score-validation-distibution/{id}")
//	public ResponseEntity<?> validationDistribution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(defaultValue = "10") int accuracy) {
//
//		try {
//			List<Map<String, Object>> resource = aegService.computeValidationDistribution(currentUser, id, accuracy);
//			return ResponseEntity.ok(resource);
//		} catch (Exception e) {
//			e.printStackTrace();
//			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//		}
//	
//	}
    
}
