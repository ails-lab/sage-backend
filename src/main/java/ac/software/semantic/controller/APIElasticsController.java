package ac.software.semantic.controller;

import java.util.List;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.ElasticResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Triple Stores API")
@RestController
@RequestMapping("/api/elastics")
public class APIElasticsController {

	@Autowired
	private ModelMapper modelMapper;
	
    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;
	
    @GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getElastics(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		try {
			List<ElasticResponse> res = elasticConfigurations.values().stream().map(es -> modelMapper.elastic2ElasticResponse(es)).collect(Collectors.toList());
			
			return APIResponse.result(res).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
	}      
    
    @GetMapping(value = "/get-info/{id}")
	public ResponseEntity<APIResponse> getElasticInfo(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		try {
			
			ElasticConfiguration ec = elasticConfigurations.getById(id);
			
			if (ec == null) {
				return APIResponse.notFound().toResponseEntity();
			}
			
			return APIResponse.result(modelMapper.elastic2ElasticResponse(ec)).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	 
	}  
    
}
