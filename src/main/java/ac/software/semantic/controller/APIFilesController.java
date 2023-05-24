package ac.software.semantic.controller;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.swagger.v3.oas.annotations.Parameter;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.FileResponse;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.FileService;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Files API")
@RestController
@RequestMapping("/api/files")
public class APIFilesController {

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private FileService fileService;

    @Autowired
 	private ModelMapper modelMapper;
    
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
	    
	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
    @GetMapping("/getAll")
	public List<FileResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("datasetId") String datasetId)  {

    	List<FileDocument> files = fileService.getFiles(currentUser, datasetId);
        
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return new ArrayList<>();
		}
		
		Dataset dataset = datasetOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
		final TripleStoreConfiguration vc;
		if (psv != null) {
			vc = psv.getTripleStoreConfiguration();
		} else {
			vc = null;
		}

    	return files.stream()
		   .map(doc -> modelMapper.file2FileResponse(vc, dataset, doc, currentUser))
  		   .collect(Collectors.toList());
	}
    
//    @GetMapping("/get/{id}")
// 	public ResponseEntity<?> get(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//
//    	Optional<MappingResponse> res = mappingsService.getMapping(currentUser, id);
//    	if (res.isPresent()) {
//    		return ResponseEntity.ok(res); 
//    	} else {
//    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
//    	}
// 	}
//	
    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	boolean deleted = fileService.deleteFile(currentUser, id);
		
    	if (deleted) {
    		return ResponseEntity.ok(new APIResponse(true, "Resource deleted"));
    	} else {
    		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    	}
	}
 

    @GetMapping(value = "/preview-last/{id}",
                produces = "text/plain")
	public ResponseEntity<?> previewLast(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> ttl = fileService.previewLast(currentUser, id);
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
    
    @GetMapping(value = "/preview-published/{id}",
            produces = "text/plain")
	public ResponseEntity<?> previewPublished(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> ttl = fileService.previewPublished(currentUser, id);
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
    
    @GetMapping(value = "/download-last/{id}")
 	public ResponseEntity<?> downloadLast(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
 	
 		try {
 			Optional<String> zip = fileService.downloadLast(currentUser, id);
 			
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
    
    @GetMapping(value = "/download-published/{id}")
	public ResponseEntity<?> downloadPublished(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id) {
		try {
			Optional<String> zip = fileService.downloadPublished(currentUser, id);
			
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
    
    @PostMapping(value = "/create",
		         consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
		         produces = "application/json")
	public ResponseEntity<?> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("name") String name, @RequestParam("datasetId") String datasetId, @RequestParam MultipartFile file)  {
	
		try {
			FileDocument fd = fileService.create(currentUser, name, datasetId, file);

			Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));

			if (!datasetOpt.isPresent()) {
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
			
			Dataset dataset = datasetOpt.get();
			
			ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
			final TripleStoreConfiguration vc;
			if (psv != null) {
				vc = psv.getTripleStoreConfiguration();
			} else {
				vc = null;
			}
			
			return ResponseEntity.ok(modelMapper.file2FileResponse(vc, dataset, fd, currentUser));
			
		} catch (Exception ex) {
			ex.printStackTrace();

			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}
		
    @PostMapping(value = "/update/{id}",
	         consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
	         produces = "application/json")
	public ResponseEntity<?> update(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam String name, @RequestParam Optional<MultipartFile> file)  {
	
    	
		try {
			FileDocument fd = fileService.update(currentUser, new ObjectId(id), name, file.isPresent() ? file.get() : null);

			Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(fd.getDatasetId(), new ObjectId(currentUser.getId()));

			if (!datasetOpt.isPresent()) {
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
			
			Dataset dataset = datasetOpt.get();
			
			ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
			final TripleStoreConfiguration vc;
			if (psv != null) {
				vc = psv.getTripleStoreConfiguration();
			} else {
				vc = null;
			}

			
			return ResponseEntity.ok(modelMapper.file2FileResponse(vc, dataset, fd, currentUser));
			
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}

}
