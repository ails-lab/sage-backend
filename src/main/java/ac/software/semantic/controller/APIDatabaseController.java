package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.LodViewConfiguration;
import ac.software.semantic.model.PagedValidationOption;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.DataServiceResponse;
import ac.software.semantic.payload.DataServicesResponse;
import ac.software.semantic.payload.DatabaseResponse;
import ac.software.semantic.payload.IndexStructureResponse;
import ac.software.semantic.payload.VocabularyResponse;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.IndexService;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.Parameter;
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
	
	@Autowired
	@Qualifier("lodview-configuration")
	private LodViewConfiguration lodview;
	
    @Autowired
    private IndexService indexService;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;
    
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer vocc;

    @Autowired
    @Qualifier("paged-validations")
	Map<String, PagedValidationOption> pavOptions;
	
	@Autowired
	private DataServiceRepository dataServiceRepository;
	
    @GetMapping(value = "/current",
	            produces = "application/json")
	public ResponseEntity<?> getCurrentDatabase()  {

		List<String> vcNames = virtuosoConfigurations.values().stream().map(vc -> vc.getName()).collect(Collectors.toList());
		Collections.sort(vcNames);
		if (vcNames.size() == 0) {
			vcNames = null;
		}
		
		List<String> ecNames = elasticConfigurations.values().stream().map(vc -> vc.getName()).collect(Collectors.toList());
		Collections.sort(ecNames);
		if (ecNames.size() == 0) {
			ecNames = null;
		}

    	DatabaseResponse res = modelMapper.database2DatabaseResponse(database, lodview, vcNames, ecNames);
		
    	return ResponseEntity.ok(res);
    	
	}     
    
	@GetMapping(value = "/indices")
	public ResponseEntity<?> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {

		List<IndexStructure> indices = indexService.getIndices();
		List<IndexStructureResponse> res = indices.stream().map(doc -> modelMapper.indexStructure2IndexStructureResponse(doc)).collect(Collectors.toList());
		
		return ResponseEntity.ok(res);
	}
    
//    @GetMapping(value = "/triple-stores",
//            produces = "application/json")
//	public ResponseEntity<?> getDatabases(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
//		
//		List<String> vcNames = virtuosoConfigurations.values().stream().map(vc -> vc.getName()).collect(Collectors.toList());
//		Collections.sort(vcNames);
//		
//		return ResponseEntity.ok(vcNames);
//	}      
    
    @GetMapping(value = "/rdf-vocabularies",
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
    
    @GetMapping(value = "/validation-modes",
            produces = "application/json")
	public ResponseEntity<?> getValidationOptions(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		
    	
    	List<PagedValidationOption> res = new ArrayList<>();
    	
    	for (PagedValidationOption pav : pavOptions.values()) {
    		res.add(new PagedValidationOption(pav.getCode(), pav.getLabel(), pav.getDimensions()));
    	}
    	
		return ResponseEntity.ok(res);
	}  
    
	@GetMapping(value = "/services")
	public ResponseEntity<?> services(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		List<DataService> annotators = dataServiceRepository.findByDatabaseIdAndType(database.getId(), DataServiceType.ANNOTATOR);
		List<DataService> embedders = dataServiceRepository.findByDatabaseIdAndType(database.getId(), DataServiceType.EMBEDDER);
		
        List<DataServiceResponse> annResponse = annotators.stream()
        		.map(doc -> modelMapper.dataService2DataServiceResponse(doc))
        		.collect(Collectors.toList());

        List<DataServiceResponse> embResponse = embedders.stream()
        		.map(doc -> modelMapper.dataService2DataServiceResponse(doc))
        		.collect(Collectors.toList());

        DataServicesResponse res = new DataServicesResponse();
        if (annResponse.size() > 0) {
        	res.setAnnotators(annResponse);
        }
        
        if (embResponse.size() > 0) {
        	res.setEmbedders(embResponse);
        }

		return ResponseEntity.ok(res);
	}
    
}
