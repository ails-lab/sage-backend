package ac.software.semantic.controller;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Parameter;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.ApiResponse;
import ac.software.semantic.payload.MappingResponse;
import ac.software.semantic.payload.MappingUpdateRequest;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.MappingsService;
import ac.software.semantic.service.ModelMapper;

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Mappings API")
@RestController
@RequestMapping("/api/mappings")
public class APIMappingsController {

	Logger logger = LoggerFactory.getLogger(APIMappingsController.class);
	
	@Autowired
	private MappingRepository mappingsRepository;

    @Autowired
 	private MappingsService mappingsService;

    @Value("${mapping.execution.folder}")
    private String mappingsFolder;

    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;
    
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
	
    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;
    
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;
    
	@Autowired
	private ModelMapper modelMapper;

    @GetMapping("/getAll")
	public List<MappingResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("datasetId") String datasetId)  {

    	return mappingsService.getMappings(currentUser, datasetId);
	}
    
    @GetMapping("/get/{id}")
 	public ResponseEntity<?> get(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	Optional<MappingResponse> res = mappingsService.getMapping(currentUser, id);
    	if (res.isPresent()) {
    		return ResponseEntity.ok(res); 
    	} else {
    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    	}
 	}
	
    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> deleteMapping(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	boolean deleted = mappingsService.deleteMapping(currentUser, id);
		
    	if (deleted) {
    		return ResponseEntity.ok(new ApiResponse(true, "Resource deleted"));
    	} else {
    		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    	}
	}
    
//    @PostMapping("/create")
//	public ResponseEntity<?> createMapping(@CurrentUser UserPrincipal currentUser, 
//			                               @RequestParam("name") String name, 
//			                               @RequestParam("datasetId") String datasetId,
//			                               @RequestParam("type") String type,
//			                               @RequestParam("parameters") Optional<String> parameters)  {
//
//    	
//    	List<String> params = new ArrayList<>();
//    	if (parameters.isPresent()) {
//    		for (String p : parameters.get().split(",")) {
//    			params.add(p);
//    		}
//    	}
//    	
//    	MappingResponse res = mappingsService.create(currentUser, name, type, datasetId, params);
//	    		
//   		return ResponseEntity.ok(res);
//
//	}
    
//    @PostMapping("/create")
//	public ResponseEntity<?> createMapping(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
//			                               @RequestParam("datasetId") String datasetId,
//			                               @RequestParam("type") String type,
//			                               @RequestBody MappingUpdateRequest mur)  {
//
//    	
//    	MappingResponse res = mappingsService.create(currentUser, datasetId, type, mur.getName(), mur.getD2rml(), mur.getParameters());
//	    		
//   		return ResponseEntity.ok(res);
//
//	}    
    
    private void saveUploadedFile(UserPrincipal currentUser, MappingDocument doc, MultipartFile file) throws IllegalStateException, IOException {
		File folder = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder);

		File newFile = new File(folder, doc.getUuid() + ".ttl");
		file.transferTo(newFile);
    }    
    
    @PostMapping(value = "/create")
	public ResponseEntity<?> createMapping(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			                               @RequestParam("datasetId") String datasetId,
			                               @RequestParam("type") String type,
			                               @RequestPart("body") String body,
			                               @RequestPart("file") MultipartFile file)  {
    	

		try {    	
			ObjectMapper objectMapper = new ObjectMapper();

	        //read json file and convert to customer object
			MappingUpdateRequest mur = objectMapper.readValue(body, MappingUpdateRequest.class);

	        
			MappingDocument doc = mappingsService.create(currentUser, datasetId, type, mur.getName(), mur.getD2rml(), mur.getParameters(), file.getOriginalFilename());
			saveUploadedFile(currentUser, doc, file);
    	
			return ResponseEntity.ok(modelMapper.mapping2MappingResponse(virtuosoConfigurations.values(), doc, currentUser));
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}			

	}        

    @PostMapping(value = "/update/{id}")
	public ResponseEntity<?> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, 
			@RequestPart("body") String body,
            @RequestPart("file") Optional<MultipartFile> file)  {

		try {    	
			ObjectMapper objectMapper = new ObjectMapper();

			MappingUpdateRequest mur = objectMapper.readValue(body, MappingUpdateRequest.class);

	    	boolean updated = mappingsService.updateMapping(currentUser, id, mur.getName(), mur.getD2rml(), mur.getParameters(), file.isPresent() ? file.get().getOriginalFilename() : null);
			if (file != null) {
				
				Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
	
				if (entry.isPresent() && file.isPresent()) {
					saveUploadedFile(currentUser, entry.get(), file.get());
				}
			}
			
			return ResponseEntity.ok(new ApiResponse(true, "Document updated"));
			
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}		
	}
    
    @PostMapping("/createParameterBinding/{id}")
	public ResponseEntity<?> createParameterBinding(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestBody List<ParameterBinding> bindings)  {

    	MappingInstance mi = mappingsService.createParameterBinding(currentUser, id, bindings);
		
    	return ResponseEntity.ok(modelMapper.mappingInstance2MappingInstanceResponse(virtuosoConfigurations.values(), mi));
	}    

    @PostMapping(value = "/upload-file/{id}",
    		     consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<?> uploadFile(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam MultipartFile file)  {

		try {
			Optional<MappingDocument> doc = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
    	
//			doc.
			if (doc.isPresent()) {
				MappingDocument mdoc = doc.get();
				
				String folder = fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder + mdoc.getUuid();
				File f = new File(folder);
				if (!f.exists()) {
					logger.info("Creating folder " + f);
					f.mkdir();
				}

				logger.info("Saving " + folder + "/" + file.getOriginalFilename() + " (size: " + file.getSize() + ")");
				file.transferTo(new File(folder + "/" + file.getOriginalFilename()));
			}
			
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    	}
    	
	}
    
//    @RequestMapping(value = "/getUploadedFiles/{id}")
//	public ResponseEntity<?> getuploadFiles(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//	
//		try {
//			List<String> res = new ArrayList<>();
//			
//			File f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder + id);
//			if (f.exists()) {
//				for (File ff : f.listFiles()) {
//					res.add(ff.getName());
//				}
//			}
//			
//			return ResponseEntity.ok(res);
//
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
//		}
//		
//	}
		
    @GetMapping(value = "/execute/{id}")
 	public ResponseEntity<?> execute(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId)  {
 		
		try {
			AsyncUtils.supplyAsync(() -> mappingsService.executeMapping(currentUser, id, instanceId.isPresent() ? instanceId.get() : null, applicationEventPublisher));
			
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
 	}	

    @GetMapping(value = "/clear-execution/{id}")
 	public ResponseEntity<?> clearExecution(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId)  {
 		
		try {
			boolean ok = mappingsService.clearExecution(currentUser, id, instanceId.isPresent() ? instanceId.get() : null);
			
			return new ResponseEntity<>(HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
 	}	
     
     @GetMapping(value = "/download/{id}",
                     produces = "text/plain")
	public ResponseEntity<?> downloadMapping(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
//    	 System.out.println("TTT " + currentUser);
		try {
			String ttl = mappingsService.downloadMapping(currentUser, id);
//			System.out.println(ttl);
			if (ttl != null) {
				return ResponseEntity.ok(ttl);
			} else {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}	 
     
     @GetMapping(value = "/lastExecution/{id}",
                     produces = "text/plain")
	public ResponseEntity<?> lastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId)  {
		
		try {
			String iid = instanceId.isPresent() ? instanceId.get() : null;
			
			Optional<String> ttl = mappingsService.getLastExecution(currentUser, id, iid);
			if (ttl.isPresent()) {
				return ResponseEntity.ok(ttl.get());
			} else {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}	  
     
     @GetMapping(value = "/downloadLastExecution/{id}")
 	public ResponseEntity<?> downloadLastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId)  {
 	
 		try {
 			String iid = instanceId.isPresent() ? instanceId.get() : null;
 			
 			Optional<String> zip = mappingsService.downloadLastExecution(currentUser, id, iid);
 			
 			if (zip.isPresent()) {
 				String file = zip.get();
 				Path path = Paths.get(file);
 				File ffile = new File(file);
 		      	
 			    ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(path));

 			    HttpHeaders headers = new HttpHeaders();
 			    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
 			    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + ffile.getName());
 			    		
 			    return ResponseEntity.ok()
 			            .headers(headers)
 			            .contentLength(ffile.length())
 			            .contentType(MediaType.APPLICATION_OCTET_STREAM)
 			            .body(resource);

 			} else {
 				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
 			}
 		} catch (Exception e) {
 			e.printStackTrace();
 			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
 		}
 	}    
     
    @PostMapping(value = "/unpublish/{id}")
  	public ResponseEntity<?> unpublish(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") Optional<String> instanceId)  {
  		
 		try {
 			AsyncUtils.supplyAsync(() -> mappingsService.unpublish(currentUser, id, instanceId.isPresent() ? instanceId.get() : null));
 			
 			return new ResponseEntity<>(HttpStatus.ACCEPTED);
 		} catch (Exception e) {
 			e.printStackTrace();
 		}

 		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  	}	
     
     
     @DeleteMapping(value = "/deleteParameterBinding/{id}",
             produces = "text/plain")
	public ResponseEntity<?> deleteParameterBinding(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("instanceId") String instanceId)  {
	
		try {
			boolean deleted = mappingsService.deleteParameterBinding(currentUser, id, instanceId);
			return new ResponseEntity<>(HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
     }	


}
