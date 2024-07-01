package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.config.ConfigUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.VocabularyResponse;
import ac.software.semantic.repository.root.VocabularyRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "System API")
@RestController
@RequestMapping("/api/system")
public class APISystemController {

	@Autowired
    @Qualifier("database")
    private Database database;
	
	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private VocabularyRepository vocRepository;

	@Lazy
	@Autowired
	private ConfigUtils cfgUtils;

    @GetMapping(value = "/get-vocabularies")
	public ResponseEntity<APIResponse> getVocabularies(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {

    	try {

	    	List<VocabularyResponse> res = new ArrayList<>();
			for (Vocabulary voc : vocRepository.findAll()) {
				res.add(modelMapper.vocabulary2VocabularyResponse(voc));
			}
		
			return APIResponse.result(res).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
    }

    @PostMapping(value = "/add-vocabulary/{id}")
	public ResponseEntity<APIResponse> addVocabulary(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	try {
    		String syncString = "VOCABULARY:" + id.toHexString();
    		
    		Vocabulary voc = null;
    		synchronized (syncString.intern()) {
	
	    		Optional<Vocabulary> vocOpt = vocRepository.findById(id);
	    		if (!vocOpt.isPresent()) {
	    			return APIResponse.notFound().toResponseEntity();
	    		}
	    		
	    		voc = vocOpt.get();
	    		if (!voc.getDatabaseId().contains(database.getId())) {
	    			voc.getDatabaseId().add(database.getId());
	    		}
	    		
	    		vocRepository.save(voc);
    		}
    		
    		cfgUtils.reloadRDFVocabularies();
    		
	    	return APIResponse.result(modelMapper.vocabulary2VocabularyResponse(voc)).toResponseEntity();
    		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
    }
    
    @PostMapping(value = "/remove-vocabulary/{id}")
	public ResponseEntity<APIResponse> removeVocabulary(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	try {
    		String syncString = "VOCABULARY:" + id.toHexString();
    		
    		Vocabulary voc = null;
    		synchronized (syncString.intern()) {
	
	    		Optional<Vocabulary> vocOpt = vocRepository.findById(id);
	    		if (!vocOpt.isPresent()) {
	    			return APIResponse.notFound().toResponseEntity();
	    		}
	    		
	    		voc = vocOpt.get();
    			voc.getDatabaseId().remove(database.getId());
	    		
	    		vocRepository.save(voc);
    		}
    		
    		cfgUtils.reloadRDFVocabularies();
    		
	    	return APIResponse.result(modelMapper.vocabulary2VocabularyResponse(voc)).toResponseEntity();
    		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
    }    

    
}
