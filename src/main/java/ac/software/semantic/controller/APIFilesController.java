package ac.software.semantic.controller;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import io.swagger.v3.oas.annotations.Parameter;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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

import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.ApiResponse;
import ac.software.semantic.payload.FileResponse;
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
	private FileService fileService;

    @Autowired
 	private ModelMapper modelMapper;
    
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
	    
	@Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;
	
    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;
    
//    @Autowired
//    private ApplicationEventPublisher applicationEventPublisher;

    @GetMapping("/getAll")
	public List<FileResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("datasetId") String datasetId)  {

    	return fileService.getFiles(currentUser, datasetId);
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
    public ResponseEntity<?> deleteFile(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

    	boolean deleted = fileService.deleteFile(currentUser, id);
		
    	if (deleted) {
    		return ResponseEntity.ok(new ApiResponse(true, "Resource deleted"));
    	} else {
    		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    	}
	}
    
 

//    @PostMapping("/update/{id}")
//	public ResponseEntity<?> updateD2RMLEntry(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam("name") Optional<String> name, @RequestBody String d2rml)  {
//
//    	boolean updated = mappingsService.updateMapping(currentUser, id, name, d2rml);
//		
////    	if (updated) {
//    		return ResponseEntity.ok(new ApiResponse(true, "Document updated"));
////    	} else {
////    		return ResponseEntity.
////    	}
//	}


    @GetMapping(value = "/view/{id}",
            produces = "text/plain")
	public ResponseEntity<?> lastExecution(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		try {
			Optional<String> ttl = fileService.getContent(currentUser, id);
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

    
    @PostMapping(value = "/create",
		         consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
		         produces = "application/json")
	public ResponseEntity<?> file(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("name") String name, @RequestParam("datasetId") String datasetId, @RequestParam MultipartFile file)  {
	
		try {
			FileDocument doc = fileService.create(currentUser, name, datasetId, file.getOriginalFilename());
			
			saveUploadedFile(currentUser, doc, file);

			return ResponseEntity.ok(modelMapper.file2FileResponse(virtuosoConfigurations.values(), doc, currentUser));
			
		} catch (Exception ex) {
			ex.printStackTrace();

			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}
		
    private void saveUploadedFile(UserPrincipal currentUser, FileDocument doc, MultipartFile file) throws IllegalStateException, IOException {
		File folder = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder, doc.getId().toString());
		if (!folder.exists()) {
			folder.mkdir();
		} else {
			for (File ff : folder.listFiles()) {
				ff.delete();
			}
		}

		File newFile = new File(folder, file.getOriginalFilename());
		file.transferTo(newFile);
		if (newFile.getName().endsWith(".zip")) {
	        byte[] buffer = new byte[2048];
	        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(newFile))) {
		        ZipEntry zipEntry = zis.getNextEntry();
		        while (zipEntry != null) {
		        	File destFile = new File(folder, zipEntry.getName());
		            
		        	try (FileOutputStream fos = new FileOutputStream(destFile)) {
			            int len;
			            while ((len = zis.read(buffer)) > 0) {
			                fos.write(buffer, 0, len);
			            }
		        	}
		            zipEntry = zis.getNextEntry();
		        }
		        zis.closeEntry();
	        }
	        newFile.delete();
	    } else	if (newFile.getName().endsWith(".bz2")) {
	    	byte[] buffer = new byte[2048];
	        try (BZip2CompressorInputStream input = new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(newFile)))) {
	        	File destFile = new File(folder, newFile.getName().substring(0, newFile.getName().length()-4));

       		try (FileOutputStream fos = new FileOutputStream(destFile)) {
       			int len;
	            
       			while ((len = input.read(buffer)) > 0) {
       				fos.write(buffer, 0, len);
		            }
	        	}
	        }
	        newFile.delete();
	    }

    }
    
    @PostMapping(value = "/update/{id}",
	         consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
	         produces = "application/json")
	public ResponseEntity<?> update(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam String name, @RequestParam Optional<MultipartFile> file)  {
	
		try {
			FileDocument doc = fileService.update(currentUser, new ObjectId(id), name, file.isPresent() ? file.get().getOriginalFilename() : null);
			
			if (file.isPresent()) {
				saveUploadedFile(currentUser, doc, file.get());
			}
			
			return ResponseEntity.ok(modelMapper.file2FileResponse(virtuosoConfigurations.values(), doc, currentUser));
			
		} catch (Exception ex) {
			ex.printStackTrace();
	
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}


     
//     @RequestMapping(value = "/download/{id}",
//                     produces = "text/plain")
//	public ResponseEntity<?> downloadMapping(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
//		
////    	 System.out.println("TTT " + currentUser);
//		try {
//			String ttl = mappingsService.downloadMapping(currentUser, id);
//			System.out.println(ttl);
//			if (ttl != null) {
//				return ResponseEntity.ok(ttl);
//			} else {
//				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
//		}
//
//	}	 
     

}
