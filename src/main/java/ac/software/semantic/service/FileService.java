package ac.software.semantic.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.FileUtils;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FileRepository;
import ac.software.semantic.security.UserPrincipal;

@Service
public class FileService {

	Logger logger = LoggerFactory.getLogger(FileService.class);

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private FileRepository fileRepository;
	
    @Autowired
    private FolderService folderService;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
	
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
    
    public FileDocument create(UserPrincipal currentUser, String name, String datasetId, MultipartFile file) throws IllegalStateException, IOException {
    	
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return null;
		}
		
		Dataset dataset = datasetOpt.get();

    	String uuid = UUID.randomUUID().toString();

    	Date date = new Date(System.currentTimeMillis());

    	FileDocument fd = new FileDocument();
    	fd.setUpdatedAt(date);
    	
    	fd.setUserId(new ObjectId(currentUser.getId()));
    	fd.setName(name);
    	fd.setUuid(uuid);
    	fd.setDatasetId(new ObjectId(datasetId));

		FileExecuteState es = new FileExecuteState();
		fd.setExecute(es);

		es.setDatabaseConfigurationId(fileSystemConfiguration.getId());
		es.setFileName(file.getOriginalFilename());
		es.setExecuteState(MappingState.EXECUTING);
		es.setExecuteStartedAt(date);
		es.setExecuteCompletedAt(null);
		
		fd = fileRepository.save(fd);
		
		List<String> fileNames = folderService.saveUploadedFile(currentUser, dataset, fd, file);
		es.setContentFileNames(fileNames);
		es.setExecuteState(MappingState.EXECUTED);
		es.setExecuteCompletedAt(new Date(System.currentTimeMillis()));
		
		fd = fileRepository.save(fd);
		
		zipUploadedFile(currentUser, dataset, fd, es);

		return fd;
    }
    

	public boolean clearFile(UserPrincipal currentUser, Dataset dataset, FileDocument fd, FileExecuteState es) {
		
		ProcessStateContainer psv = fd.getCurrentPublishState(virtuosoConfigurations.values());
		
		if (psv != null) {
			FilePublishState ps = (FilePublishState)psv.getProcessState();
			FileExecuteState pes = ps.getExecute();
	
			// do not clear published execution
			if (pes != null && pes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) == 0 && pes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {	
				return false;
			} 
		}
		
		List<File> files = folderService.getUploadedFiles(currentUser, dataset, fd, es);
		if (files != null) {
			for (File f : files) {
				try {
					boolean ok = false;
					if (f != null) {
						ok = f.delete();
						if (ok) {
							logger.info("Deleted file " + f.getAbsolutePath());
						}
					}
					if (!ok) {
						logger.warn("Failed to delete file for " + fd.getUuid());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
			
		try {
			File f = folderService.getUploadedZipFile(currentUser, dataset, fd, es);
			boolean ok = false;
			if (f.exists()) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
			}
			if (!ok) {
				logger.warn("Failed to delete zipped file for " + fd.getUuid());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		folderService.deleteUploadedFilesFolderIfEmpty(currentUser, dataset, fd);
		
		if (fd.getExecute() == es) {
			fd.setExecute(null);
		}
		
		fileRepository.save(fd);

		return true;
	}	
	
    public FileDocument update(UserPrincipal currentUser, ObjectId id, String name, MultipartFile file) throws IllegalStateException, IOException {
    	
    	Optional<FileDocument> fileOpt = fileRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId()));
    	
    	if (!fileOpt.isPresent()) {
    		return null;
    	}
    			
		FileDocument fd = fileOpt.get();
		
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(fd.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return null;
		}
		
		Dataset dataset = datasetOpt.get();
		
		if (name != null) {
			fd.setName(name);
		}
		
		if (file != null) {
			clearFile(currentUser, dataset, fd, fd.getExecute());
			
			Date date = new Date(System.currentTimeMillis());
			fd.setUpdatedAt(date);
			
			FileExecuteState es = new FileExecuteState();
			es.setDatabaseConfigurationId(fileSystemConfiguration.getId());
			es.setFileName(file.getOriginalFilename());
			es.setExecuteState(MappingState.EXECUTING);
			es.setExecuteStartedAt(date);
			es.setExecuteCompletedAt(null);

			fd.setExecute(es);
			fd = fileRepository.save(fd);
			
			List<String> fileNames = folderService.saveUploadedFile(currentUser, dataset, fd, file);
			es.setContentFileNames(fileNames);
			es.setExecuteState(MappingState.EXECUTED);
			es.setExecuteCompletedAt(new Date(System.currentTimeMillis()));
			
			zipUploadedFile(currentUser, dataset, fd, es);
		}
		
		fileRepository.save(fd);
		
		return fd;
    }
    
	public Optional<String> downloadLast(UserPrincipal currentUser, String id) throws IOException {
    	Optional<FileDocument> fileOpt = fileRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
    	
    	if (!fileOpt.isPresent()) {
    		return Optional.empty();
    	}
    			
		FileDocument fd = fileOpt.get();
		
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(fd.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return Optional.empty();
		}
		
		Dataset dataset = datasetOpt.get();
		FileExecuteState es = fd.getExecute();
		
		File file = folderService.getUploadedZipFile(currentUser, dataset, fd, es);

		if (file.exists()) {
			return Optional.of(file.getAbsolutePath());
		} else {
			return Optional.empty();
		}
	
	}
	
	public Optional<String> downloadPublished(UserPrincipal currentUser, String id) throws IOException {
    	Optional<FileDocument> fileOpt = fileRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
    	
    	if (!fileOpt.isPresent()) {
    		return Optional.empty();
    	}
    			
		FileDocument fd = fileOpt.get();
		
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(fd.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return Optional.empty();
		}
		
		Dataset dataset = datasetOpt.get();

		ProcessStateContainer psv = fd.getCurrentPublishState(virtuosoConfigurations.values());
		if (psv == null) {
			return Optional.empty();			
		}
		
		FilePublishState ps = (FilePublishState)psv.getProcessState();
		FileExecuteState pes = ps.getExecute();
		
		if (!pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
			return Optional.empty();
		}
		
		File file = folderService.getUploadedZipFile(currentUser, dataset, fd, pes);

		if (file.exists()) {
			return Optional.of(file.getAbsolutePath());
		} else {
			return Optional.empty();
		}
		
		
	}
	
	private File zipUploadedFile(UserPrincipal currentUser, Dataset dataset, FileDocument fd, FileExecuteState es) throws IOException {
		
		File file = folderService.getUploadedZipFile(currentUser, dataset, fd, es);
		
		try (FileOutputStream fos = new FileOutputStream(file);
				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
			for (File fileToZip : folderService.getUploadedFiles(currentUser, dataset, fd, es)) {
	            try (FileInputStream fis = new FileInputStream(fileToZip)) {
		            ZipEntry zipEntry = new ZipEntry(fileToZip.getName().substring(es.getExecuteStartStamp().length() + 1));
		            zipOut.putNextEntry(zipEntry);
		 
		            byte[] bytes = new byte[1024];
		            int length;
		            while((length = fis.read(bytes)) >= 0) {
		                zipOut.write(bytes, 0, length);
		            }
	            }
	        }
        }
		
		return file;
	}
    
	public List<FileDocument> getFiles(UserPrincipal currentUser, String datasetId) {
        return fileRepository.findByDatasetIdAndFileSystemConfigurationIdAndUserId(new ObjectId(datasetId), fileSystemConfiguration.getId(), new ObjectId(currentUser.getId()));
    }	
	
	public boolean deleteFile(UserPrincipal currentUser, String id) {

        Optional<FileDocument> doc = fileRepository.findById(new ObjectId(id));

        if (!doc.isPresent()) {
        	return false;
        }
        
        FileDocument fd = doc.get();
        
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(fd.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return false;
		}
		
		Dataset dataset = datasetOpt.get();

		clearFile(currentUser, dataset, fd, fd.getExecute());
		
		if (fd.getExecute() == null) {
			fileRepository.delete(fd);
		}
        
        return true;
    }	
	
	public Optional<String> previewLast(UserPrincipal currentUser, String id) throws IOException {

        Optional<FileDocument> doc = fileRepository.findById(new ObjectId(id));

        if (!doc.isPresent()) {
        	return Optional.empty();
        }
        
        FileDocument fd = doc.get();
        FileExecuteState es = fd.getExecute();

		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(fd.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return Optional.empty();
		}
		
		Dataset dataset = datasetOpt.get();

        List<File> f = folderService.getUploadedFiles(currentUser, dataset, fd, es);
        	
		if (f != null && f.size() > 0) {
			return Optional.of(FileUtils.readFileBeginning(Paths.get(f.get(0).getCanonicalPath())));
        } 
        
        return Optional.empty();
	}

	public Optional<String> previewPublished(UserPrincipal currentUser, String id) throws IOException {

        Optional<FileDocument> doc = fileRepository.findById(new ObjectId(id));

        if (!doc.isPresent()) {
        	return Optional.empty();
        }
        
        FileDocument fd = doc.get();
		ProcessStateContainer psv = fd.getCurrentPublishState(virtuosoConfigurations.values());
		if (psv == null) {
			return Optional.empty();			
		}
		
		FilePublishState ps = (FilePublishState)psv.getProcessState();
		ExecuteState pes = ps.getExecute();
		
		if (!pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
			return Optional.empty();
		}
		
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(fd.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return Optional.empty();
		}

		Dataset dataset = datasetOpt.get();
		
        List<File> f = folderService.getUploadedFiles(currentUser, dataset, fd, pes);
        	
		if (f != null && f.size() > 0) {
			return Optional.of(FileUtils.readFileBeginning(Paths.get(f.get(0).getCanonicalPath())));
        } 
        
        return Optional.empty();
	}


}
