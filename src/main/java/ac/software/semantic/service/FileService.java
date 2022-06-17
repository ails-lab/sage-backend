
package ac.software.semantic.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ac.software.semantic.controller.AsyncUtils;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.FileResponse;
import ac.software.semantic.repository.FileRepository;
import ac.software.semantic.security.UserPrincipal;

@Service
public class FileService {

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private FileRepository fileRepository;
	
    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;
    
	@Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfiguration;

	
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
    
    public FileDocument create(UserPrincipal currentUser, String name, String datasetId, String fileName) {
    	String uuid = UUID.randomUUID().toString();
    	
		FileDocument doc = fileRepository.save(
				new FileDocument(new ObjectId(currentUser.getId()), name, uuid, new ObjectId(datasetId), fileName));
		
		return doc;
    }
    
    public FileDocument update(UserPrincipal currentUser, ObjectId id, String name, String fileName) {
    	
    	Optional<FileDocument> fileOpt = fileRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId()));
    	if (!fileOpt.isPresent()) {
    		return null;
    	}
    			
		FileDocument doc = fileOpt.get();
		doc.setName(name);
		if (fileName != null) {
			doc.setFileName(fileName);
		}
		
		fileRepository.save(doc);
		
		return doc;
    }
    
	public List<FileResponse> getFiles(UserPrincipal currentUser, String datasetId) {

        List<FileDocument> docs = fileRepository.findByDatasetIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));

        List<FileResponse> response = docs.stream()
        		.map(doc -> modelMapper.file2FileResponse(virtuosoConfiguration.values(), doc, currentUser))
        		.collect(Collectors.toList());

        return response;
    }	
	
	public boolean deleteFile(UserPrincipal currentUser, String mappingId) {

        Optional<FileDocument> doc = fileRepository.findById(new ObjectId(mappingId));

        if (doc.isPresent()) {
        	FileDocument map = doc.get();
        	fileRepository.delete(map);
        	
        	File folder = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder, map.getId().toString());
			if (folder.exists()) {
				for (File ff : folder.listFiles()) {
					ff.delete();
				}
				folder.delete();
			}
			
        	return true;
        } else {
        	return false;
        }
    }	
	
	public Optional<String> getContent(UserPrincipal currentUser, String id) throws IOException {

        Optional<FileDocument> doc = fileRepository.findById(new ObjectId(id));

        if (doc.isPresent()) {
        	FileDocument map = doc.get();
        	
        	File folder = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder, map.getId().toString());
			if (folder.exists()) {
				for (File ff : folder.listFiles()) {
//					String file = new String(Files.readAllBytes(Paths.get(ff.getCanonicalPath())));
		        	String file = AsyncUtils.readFileBeginning(Paths.get(ff.getCanonicalPath()));

					return Optional.of(file);
				}
			}
        } 
        
        return Optional.empty();
	}


}
