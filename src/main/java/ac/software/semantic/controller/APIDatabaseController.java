package ac.software.semantic.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.model.Database;
import ac.software.semantic.payload.DatabaseResponse;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Database API")
@RestController
@RequestMapping("/api/database")
public class APIDatabaseController {

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
    @Qualifier("database")
    private Database database;
    
    @GetMapping(value = "/current",
	            produces = "application/json")
	public ResponseEntity<?> getCurrentDatabase()  {
	
    	DatabaseResponse db = modelMapper.database2DatabaseResponse(database); 
				
		return ResponseEntity.ok(db);
	}       

}
