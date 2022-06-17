package ac.software.semantic.controller;


import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.OntologyService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Ontology API")
@RestController
@RequestMapping("/api/ontology")
public class APIOntologyController {
    
	@Autowired
    private OntologyService ontologyService;
	
	@GetMapping(value = "/getQueryProperties",
                produces = "application/json")
	public ResponseEntity<?> getProperties(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {

		return ResponseEntity.ok(ontologyService.getProperties());
		
	} 
}
