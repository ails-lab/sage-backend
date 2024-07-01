package ac.software.semantic.controller;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Parameter;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.FileService;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Files API")
@RestController
@RequestMapping("/api/files")
public class APIFilesController {

	@Autowired
	private FileService fileService;
	
	@Autowired
	private DatasetService datasetService;

	@Autowired
	private APIUtils apiUtils;
	
	@GetMapping("/get/{id}")
	public ResponseEntity<?> get(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		return apiUtils.get(currentUser, new SimpleObjectIdentifier(id), fileService).toResponseEntity();
	}
	
    @GetMapping("/get-all-my")
	public ResponseEntity<APIResponse> getAllMy(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	if (datasetId != null) {
    		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, Arrays.asList(new ObjectId[] {datasetId}), fileService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
    	} else {
    		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, null, fileService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
    	}
	}

    @GetMapping("/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, 
			                                  @RequestParam ObjectId datasetId,
			                                  @RequestParam(required = false) Integer page,
			                                  @RequestParam(defaultValue = "${api.pagination.size}") int size)  {
    	
    	if (datasetId != null) {
    		return APIResponse.result(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId}), fileService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
    	} else {
    		return APIResponse.result(apiUtils.getAllByUser(currentUser, null, null, fileService, datasetService, apiUtils.pageable(page, size))).toResponseEntity();
    	}
	}

    @PostMapping(value = "/create")
	public ResponseEntity<APIResponse> create(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId, @RequestPart("json") String json, @RequestPart Optional<MultipartFile> file)  {
    	return apiUtils.cnew(currentUser, new SimpleObjectIdentifier(datasetId), json, file.orElse(null), fileService, datasetService).toResponseEntity();
	}   
    
	@PostMapping(value = "/update/{id}")
	public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestPart String json, @RequestPart Optional<MultipartFile> file) {
		return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), json, file.orElse(null), fileService).toResponseEntity();
	}

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<APIResponse> delete(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
    	return apiUtils.delete(currentUser, new SimpleObjectIdentifier(id), fileService, datasetService).toResponseEntity();
	}
    
    @PostMapping("/change-group/{id}")
 	public ResponseEntity<APIResponse> changeGroup(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam int group)  {
    	return apiUtils.changeGroup(currentUser, new SimpleObjectIdentifier(id), group, fileService, datasetService).toResponseEntity();
 	}
    
    @PostMapping("/change-order/{id}")
 	public ResponseEntity<APIResponse> changeOrder(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam int step)  {
    	return apiUtils.changeOrder(currentUser, new SimpleObjectIdentifier(id), step, fileService, datasetService).toResponseEntity();
 	}

    @GetMapping(value = "/preview-last/{id}",
                produces = "text/plain")
	public ResponseEntity<?> previewLast(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable String id)  {
	
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
 	public ResponseEntity<?> downloadLast(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable String id)  {
 	
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
	public ResponseEntity<?> downloadPublished(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable String id) {
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


}
