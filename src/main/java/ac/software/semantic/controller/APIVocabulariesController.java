package ac.software.semantic.controller;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.VocabularyResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.VocabularyService;
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
    private VocabularyContainer vocc;

	@Autowired
    private VocabularyService vocService;

    @GetMapping(value = "/get-all",
            produces = "application/json")
	public ResponseEntity<?> getVocabularies(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
    	try {
	    	List<VocabularyResponse> res = vocc.getVocsByName().values().stream().map(voc -> modelMapper.vocabulary2VocabularyResponse(voc)).collect(Collectors.toList());
	    	
			return ResponseEntity.ok(res);
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
		}	 
    }
    
    @GetMapping(value = "/prefixize",
            produces = "application/json")
	public ResponseEntity<?> prefixize(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam String uri)  {
    	try {
			return ResponseEntity.ok(vocc.arrayPrefixize(uri));
		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
		}	 
    }
    
}
