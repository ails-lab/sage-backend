package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.LodViewConfiguration;
import ac.software.semantic.model.PagedValidationOption;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.DataServiceResponse;
import ac.software.semantic.payload.response.DataServicesResponse;
import ac.software.semantic.payload.response.TemplateResponse;
import ac.software.semantic.payload.response.TemplatesResponse;
import ac.software.semantic.repository.root.DataServiceRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ElasticSearch;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.TemplateServicesService;
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
    private ElasticSearch indexService;

    @Autowired
    private TemplateServicesService templateService;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;
    
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;

    @Autowired
    @Qualifier("paged-validations")
	Map<String, PagedValidationOption> pavOptions;
	
	@Autowired
	private DataServiceRepository dataServiceRepository;

    @Autowired
    @Qualifier("preprocess-functions")
    private Map<Resource, List<String>> functions;
    
    @Autowired
    @Qualifier("preprocess-operations")
    private Map<Resource, List<String>> operations;
    
    @Autowired
    @Qualifier("annotators")
    private Map<String, DataService> annotators;
    
    @Autowired
    @Qualifier("embedders")
    private Map<String, DataService> embedders;
    
    @Autowired
    @Qualifier("clusterers")
    private Map<String, DataService> clusterers;
	
    @GetMapping(value = "/current", produces = "application/json")
	public ResponseEntity<APIResponse> getCurrentDatabase()  {
    	return APIResponse.result(modelMapper.database2DatabaseResponse(database, lodview, virtuosoConfigurations.names(), elasticConfigurations.names())).toResponseEntity();
	}     
    
//	@GetMapping(value = "/index-structures")
//	public ResponseEntity<APIResponse> indexStructures(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
//    	try {
//    		return APIResponse.result(indexService.getIndices().stream().map(doc -> modelMapper.indexStructure2IndexStructureResponse(doc)).collect(Collectors.toList())).toResponseEntity();
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			return APIResponse.serverError(ex).toResponseEntity();
//		}	 
//	}
    
    
    @GetMapping(value = "/rdf-vocabularies")
	public ResponseEntity<APIResponse> getVocabularies(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
    	try {
    		return APIResponse.result(vocc.getVocsByName().values().stream().map(voc -> modelMapper.vocabulary2VocabularyResponse(voc)).collect(Collectors.toList())).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
    }
    
    @GetMapping(value = "/validation-modes",
            produces = "application/json")
	public ResponseEntity<APIResponse> getValidationOptions(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
    	
    	List<PagedValidationOption> res = new ArrayList<>();
    	
    	for (PagedValidationOption pav : pavOptions.values()) {
    		res.add(new PagedValidationOption(pav.getCode(), pav.getLabel(), pav.getDimensions()));
    	}
    	
		return APIResponse.result(res).toResponseEntity();
	}  
    
	@GetMapping(value = "/services")
	public ResponseEntity<APIResponse> services(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		
        List<DataServiceResponse> annResponse = annotators.values().stream().map(doc -> modelMapper.dataService2DataServiceResponse(doc)).collect(Collectors.toList());
        List<DataServiceResponse> embResponse = embedders.values().stream().map(doc -> modelMapper.dataService2DataServiceResponse(doc)).collect(Collectors.toList());
        List<DataServiceResponse> cluResponse = clusterers.values().stream().map(doc -> modelMapper.dataService2DataServiceResponse(doc)).collect(Collectors.toList());
        
        DataServicesResponse res = new DataServicesResponse();
        if (annResponse.size() > 0) {
        	res.setAnnotators(annResponse);
        }
        
        if (embResponse.size() > 0) {
        	res.setEmbedders(embResponse);
        }
        
        if (cluResponse.size() > 0) {
        	res.setClusterers(cluResponse);
        }

		return APIResponse.result(res).toResponseEntity();
	}
	
	@GetMapping(value = "/templates",
		      produces = "application/json")
	public ResponseEntity<?> getImportTemplates(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
		List<TemplateResponse> catalog = templateService.getCatalogImportTemplates().stream()
		    .map(template -> modelMapper.template2TemplateResponse(template))
		    .collect(Collectors.toList());
		
		if (catalog.size() == 0) {
			catalog = null;
		}
		
		List<TemplateResponse> dataset = templateService.getDatasetImportTemplates().stream()
		    .map(template -> modelMapper.template2TemplateResponse(template))
		    .collect(Collectors.toList());

		if (dataset.size() == 0) {
			dataset = null;
		}

		List<TemplateResponse> mappingSample = templateService.getMappingSampleTemplates().stream()
		    .map(template -> modelMapper.template2TemplateResponse(template))
		    .collect(Collectors.toList());

		if (mappingSample.size() == 0) {
			mappingSample = null;
		}

		TemplatesResponse res = new TemplatesResponse(catalog, dataset, mappingSample);
		
		return APIResponse.result(res).toResponseEntity();
	}
    
	@GetMapping(value = "/functions")
	public ResponseEntity<APIResponse> functions(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		
		ObjectMapper mapper = new ObjectMapper();

		ArrayNode farray = mapper.createArrayNode();
		for (Entry<Resource, List<String>> entry : functions.entrySet()) {
			ObjectNode object = mapper.createObjectNode();
			object.put("uri", entry.getKey().toString());
			
			List<String> ps = entry.getValue();
			if (ps.size() == 0) {
				continue;
			}
			
			ArrayNode params = mapper.createArrayNode();
			
			for (String p : entry.getValue()) {
				params.add(p);
			}
			object.put("parameters", params);
			farray.add(object);
		}
		
		ArrayNode oarray = mapper.createArrayNode();
		for (Entry<Resource, List<String>> entry : operations.entrySet()) {
			ObjectNode object = mapper.createObjectNode();
			object.put("uri", entry.getKey().toString());
			
			List<String> ps = entry.getValue();
			if (ps.size() == 0) {
				continue;
			}
			
			ArrayNode params = mapper.createArrayNode();
			
			for (String p : entry.getValue()) {
				params.add(p);
			}
			object.put("parameters", params);
			oarray.add(object);
		}

		ObjectNode object = mapper.createObjectNode();
		object.put("functions", farray);
		object.put("operations", oarray);
		
		return APIResponse.result(object).toResponseEntity();
	}

	@GetMapping(value = "/system-time")
	public ResponseEntity<APIResponse> systemTime(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		return APIResponse.result(new Date()).toResponseEntity();
	}

}
