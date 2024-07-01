package ac.software.semantic.controller;

import java.util.List;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.VocabularyResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Database API")
@RestController
@RequestMapping("/api/vocabularies")
public class APIVocabulariesController {

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
    @Qualifier("database")
    private Database database;
	
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;

    @GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getVocabularies(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
    	try {
	    	List<VocabularyResponse> res = vocc.getVocsByName().values().stream().map(voc -> modelMapper.vocabulary2VocabularyResponse(voc)).collect(Collectors.toList());
	    	
			return APIResponse.result(res).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
    }
    
    
    @GetMapping(value = "/prefixize",
            produces = "application/json")
	public ResponseEntity<?> prefixize(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam String uri)  {
    	try {
			return ResponseEntity.ok(vocc.arrayPrefixize(uri));
		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
    }
    
}
