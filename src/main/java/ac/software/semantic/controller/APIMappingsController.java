package ac.software.semantic.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.validation.constraints.NotNull;

import io.swagger.v3.oas.annotations.Parameter;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.controller.utils.AsyncUtils;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.payload.request.MappingInstanceUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.MappingService;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.container.MappingObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.exception.TaskConflictException;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Mappings API")
@RestController
@RequestMapping("/api/mapping")
public class APIMappingsController {
	
    @Autowired
 	private MappingService mappingService;

    @Autowired
 	private DatasetService datasetService;

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private APIUtils apiUtils;

    @PostMapping(value = "/new")
	public ResponseEntity<?> cnew(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId, @RequestPart("json") String json, @RequestPart("file") Optional<MultipartFile> file)  {
    	return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), json, file.orElse(null), mappingService, datasetService).toResponseEntity();
	}   
    
    @GetMapping("/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, mappingService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
    
    @GetMapping("/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	return APIResponse.result(apiUtils.getAllByUser(currentUser, null, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, mappingService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
	}
    
	@GetMapping("/get/{id}")
	public ResponseEntity<APIResponse> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.get(currentUser, new MappingObjectIdentifier(id, null), mappingService).toResponseEntity();
	}	

    @GetMapping("/get-content/{id}")
 	public ResponseEntity<APIResponse> getContent(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.getContent(currentUser, new MappingObjectIdentifier(id, null), mappingService).toResponseEntity();
 	}
    
    @PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestPart("json") String json, @RequestPart("file") Optional<MultipartFile> file)  {
    	return apiUtils.update(currentUser, new MappingObjectIdentifier(id, null), json, file.orElse(null), mappingService).toResponseEntity();
    }
    
    @PostMapping("/change-order/{id}")
 	public ResponseEntity<APIResponse> changeOrder(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam int step)  {
    	return apiUtils.changeOrder(currentUser, new MappingObjectIdentifier(id, null), step, mappingService, datasetService).toResponseEntity();
 	}

    @PostMapping("/change-group/{id}")
 	public ResponseEntity<APIResponse> changeGroup(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam int group)  {
    	return apiUtils.changeGroup(currentUser, new MappingObjectIdentifier(id, null), group, mappingService, datasetService).toResponseEntity();
 	}
    
    @DeleteMapping("/delete/{id}")
    public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam Optional<ObjectId> instanceId)  {
    	return apiUtils.delete(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingService, datasetService).toResponseEntity();
    }
	
    // not update safe !!!  
    @PostMapping("/create-instance/{id}")
	public ResponseEntity<?> createInstance(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable String id, @RequestBody MappingInstanceUpdateRequest ur)  {

    	try {
	    	MappingContainer mc = mappingService.getContainer(currentUser, id);
	    	
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class).toResponseEntity();
	    	}
	    	
	    	MappingInstance mi = mappingService.createParameterBinding(mc, ur);
			
	    	TripleStoreConfiguration vc = mc.getDatasetTripleStoreVirtuosoConfiguration();
	    	
	    	return ResponseEntity.ok(modelMapper.mappingInstance2MappingInstanceResponse(vc, mi));
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
    	}
	}    
    
    // not update safe !!!    
    @PostMapping("/update-instance/{id}")
	public ResponseEntity<?> updateInstance(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam ObjectId instanceId, @RequestBody MappingInstanceUpdateRequest ur)  {

    	try {
	    	MappingContainer mc = mappingService.getContainer(currentUser, id, instanceId);
	    	
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class).toResponseEntity();
	    	}
	    	
	    	MappingInstance mi = mappingService.updateParameterBinding(mc, ur);
	    	
			TripleStoreConfiguration vc = mc.getDatasetTripleStoreVirtuosoConfiguration();
	
	    	return ResponseEntity.ok(modelMapper.mappingInstance2MappingInstanceResponse(vc, mi));
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
    	}    
    }
    

    // not update safe !!!
    @PostMapping(value = "/upload-attachment/{id}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<APIResponse> uploadAttachment(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam Optional<ObjectId> instanceId, @RequestParam MultipartFile file)  {

		try {
	    	MappingContainer mc = mappingService.getContainer(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)));
	    	
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class).toResponseEntity();
	    	}
	    	
	    	mc.saveAttachment(file);
			
	    	return APIResponse.updated(mc).toResponseEntity();
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex).toResponseEntity();
		} catch (Exception ex) {
    		ex.printStackTrace();
    		return APIResponse.serverError(ex).toResponseEntity();
    	}
	}
    
    @GetMapping(value = "/download-attachment/{id}")
	public ResponseEntity<StreamingResponseBody> downloadAttachment(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam Optional<ObjectId> instanceId, @RequestParam String filename)  {

		try {
	    	MappingContainer mc = mappingService.getContainer(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)));
	    	
	    	if (mc == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
	    	
	    	File f = mc.getAttachment(filename);

	    	if (f == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}

			return apiUtils.downloadFile(f.getAbsolutePath(), filename);

		} catch (Exception ex) {
    		ex.printStackTrace();
    		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    	}
	}
    
    @DeleteMapping("/delete-attachment/{id}")
    public ResponseEntity<APIResponse> deleteAttachment(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam Optional<ObjectId> instanceId, @RequestParam String filename)  {

		try {
	    	MappingContainer mc = mappingService.getContainer(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)));
	    	
	    	if (mc == null) {
	    		return APIResponse.notFound(MappingContainer.class).toResponseEntity();
	    	}

	    	mc.removeAttachment(filename);
	    	
	    	return APIResponse.updated(mc).toResponseEntity();
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return APIResponse.serverError(ex).toResponseEntity();
    	}
	}
	
	@PostMapping(value = "/execute/{id}")
	public ResponseEntity<APIResponse> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
		return apiUtils.execute(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingService).toResponseEntity();
	}

	@PostMapping(value = "/stop-execution/{id}")
	public ResponseEntity<APIResponse> stopExecution(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
		return apiUtils.stopExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingService).toResponseEntity();		
	}

    @PostMapping(value = "/clear-execution/{id}")
    public ResponseEntity<APIResponse> clearExecution(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
    	return apiUtils.clearExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingService).toResponseEntity();
 	}		
     
    @GetMapping(value = "/preview-last-execution/{id}")    
	public ResponseEntity<?> previewLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
		return apiUtils.previewLastExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), shard, offset, mappingService);
	}	
    
    @GetMapping(value = "/preview-published-execution/{id}")    
	public ResponseEntity<?> previewPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId, @RequestParam(defaultValue = "0") int shard, @RequestParam(defaultValue = "0") int offset)  {
		return apiUtils.previewPublishedExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), shard, offset, mappingService);
	}	
     
    @GetMapping(value = "/download-last-execution/{id}")
 	public ResponseEntity<StreamingResponseBody> downloadLastExecution(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
     	return apiUtils.downloadLastExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingService, FileType.zip);
 	} 

    @GetMapping(value = "/download-published-execution/{id}")
 	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
     	return apiUtils.downloadPublishedExecution(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingService);
 	} 
     
	@PostMapping(value = "/shacl-validate-published-execution/{id}")
	public ResponseEntity<APIResponse> shaclValidate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("instanceId") Optional<ObjectId> instanceId)  {
		return apiUtils.validate(currentUser, new MappingObjectIdentifier(id, instanceId.orElse(null)), mappingService).toResponseEntity();
	}
	
    // Experimental
    @PostMapping(value = "/unpublish/{id}")
  	public ResponseEntity<?> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable String id, @RequestParam("instanceId") Optional<String> instanceId)  {
  		
 		try {
 			AsyncUtils.supplyAsync(() -> mappingService.unpublish(currentUser, id, instanceId.isPresent() ? instanceId.get() : null));
 			
 			return new ResponseEntity<>(HttpStatus.ACCEPTED);
 		} catch (Exception e) {
 			e.printStackTrace();
 		}

 		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  	}	
    

    @GetMapping(value = "/exists-identifier")
	public ResponseEntity<APIResponse> existsIdentifier(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @Parameter(required = true) @NotNull @RequestParam String identifier, @RequestParam ObjectId datasetId, @RequestParam(required = false) ObjectId mappingId)  {
		
    	try {
    		DatasetContainer dc = datasetService.getContainer(currentUser, datasetId);
	    			
    	    if (dc == null) {
    	    	return APIResponse.notFound(DatasetContainer.class).toResponseEntity();
    	    }
    	    	
    	    MappingDocument doc = null;
    	    if (mappingId == null) {
	    	    doc = new MappingDocument(dc.getObject());
		    	doc.setIdentifier(identifier);
		    	
    	    } else {
    	    	MappingContainer mc = mappingService.getContainer(currentUser, mappingId, null);
    	    	
    	    	doc = mc.getObject();
    	    	doc.clearInstances();
    	    	
    	    	List<ParameterBinding> pb = new ArrayList<>();
    	    	pb.add(new ParameterBinding()); // hack 
    	    	MappingInstance mi = doc.addInstance(pb);
    	    	mi.setIdentifier(identifier);
    	    }
    	    
	    	return apiUtils.identifierConflict(doc, IdentifierType.IDENTIFIER, mappingService).toResponseEntity();

		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}

}
