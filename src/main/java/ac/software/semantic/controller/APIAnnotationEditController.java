package ac.software.semantic.controller;

import java.util.List;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.payload.AnnotationEditResponse;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Annotation Edit API")
@RestController
@RequestMapping("/api/annotation")
public class APIAnnotationEditController {
    
	@Autowired
    private AnnotationEditService annotationEditService;

	@Autowired
	private AnnotationEditGroupRepository annotationEditGroupRepository;

	
//    @PostMapping(value = "/addDeleteEdit",
//	           produces = "application/json")
//	public ResponseEntity<?> delete(@CurrentUser UserPrincipal currentUser, @RequestParam("datasetUri") String datasetUri, @RequestParam("onProperty") String onProperty, @RequestParam("propertyValue") String propertyValue, @RequestParam("asProperty") String asProperty, @RequestParam("annotationValue") String annotationValue)  {
//	
////    	Optional<Dataset> doc = datasetRepository.findByUuid(SEMAVocabulary.getId(datasetUri));
//    	
////    	if (doc.isPresent()) {
//			annotationEditService.addDeleteEdit(currentUser, SEMAVocabulary.getId(datasetUri), onProperty, propertyValue, asProperty, annotationValue);
//
//			return new ResponseEntity<>(HttpStatus.OK);
//			
////    	} else {
////    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
////    	}
//	}
    
    @PostMapping(value = "/commitAnnotationEdits/{id}",
	           produces = "application/json")
	public ResponseEntity<?> delete(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody List<AnnotationEditResponse> edits)  {

    	Optional<AnnotationEditGroup> doc = annotationEditGroupRepository.findById(id);
    	
	 	if (doc.isPresent()) {
	 		for(AnnotationEditResponse vad : edits) { 
	 			annotationEditService.processEdit(currentUser, doc.get(), vad);
	 		}
	 	
			return new ResponseEntity<>(HttpStatus.OK);
//				
	 	} else {
	 		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	 	}
	}


}
