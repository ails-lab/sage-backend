package ac.software.semantic.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.FileUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.payload.request.FileUpdateRequest;
import ac.software.semantic.payload.response.FileResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.FileDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.lookup.FileLookupProperties;

@Service
public class FileService implements EnclosedCreatableLookupService<FileDocument, FileResponse, FileUpdateRequest, Dataset, FileLookupProperties>,
                                    PublishingService<FileDocument,FileResponse> {

	private Logger logger = LoggerFactory.getLogger(FileService.class);

    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private DatasetRepository datasetRepository;

	@Lazy
	@Autowired
	private DatasetService datasetService;

	@Autowired
	private FileDocumentRepository fileRepository;
	
	
    @Autowired
    private FolderService folderService;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
	
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
    
	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private ServiceUtils serviceUtils;

	@Autowired
	private TripleStore tripleStore;

	@Override
	public Class<? extends EnclosedObjectContainer<FileDocument,FileResponse,Dataset>> getContainerClass() {
		return FileContainer.class;
	}
	
	@Override 
	public DocumentRepository<FileDocument> getRepository() {
		return fileRepository;
	}
	
	public class FileContainer extends EnclosedObjectContainer<FileDocument,FileResponse,Dataset> 
	                           implements ExecutableContainer<FileDocument,FileResponse,FileExecuteState,Dataset>, 
	                                      PublishableContainer<FileDocument,FileResponse,FileExecuteState, FilePublishState, Dataset>,
	                                      UpdatableContainer<FileDocument,FileResponse,FileUpdateRequest> {
		private ObjectId fileId;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		private FileContainer(UserPrincipal currentUser, ObjectId fileId) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.fileId = fileId;
		
			load();
		}
		
		private FileContainer(UserPrincipal currentUser, FileDocument fdoc) {
			this(currentUser, fdoc, null);
		}
		
		private FileContainer(UserPrincipal currentUser, FileDocument fdoc, Dataset dataset) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;

			this.fileId = fdoc.getId();
			this.object = fdoc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return fileId;
		}
		
		@Override 
		public DocumentRepository<FileDocument> getRepository() {
			return fileRepository;
		}
		
		@Override
		public FileService getService() {
			return FileService.this;
		}
		
		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findById(object.getDatasetId());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			setEnclosingObject(datasetOpt.get());
		}

//		@Override
//		public ExecutableDocument<FileExecuteState> getExecuteDocument() {
//			return getObject();
//		}
//
//		@Override
//		public PublishableDocument<FileExecuteState, FilePublishState> getPublishDocument() {
//			return getObject();
//		}

		@Override
	    public FileDocument update(FileUpdateRequest ur) throws Exception {

	    	return update(ifc -> {
	    		FileDocument fdoc = ((FileContainer)ifc).getObject();
	    		
	   			fdoc.setName(ur.getName());
	   			fdoc.setDescription(ur.getDescription());
	   			fdoc.setUrl(ur.getUrl());

	    		if (ur.getFile() != null) {
	    			
	    			MultipartFile file = ur.getFile();
	    			
	    			clearFile(getCurrentUser(), getEnclosingObject(), fdoc, fdoc.getExecuteState(fileSystemConfiguration.getId()));
	    			
	    			FileExecuteState es = new FileExecuteState();
	    			es.setDatabaseConfigurationId(fileSystemConfiguration.getId());
	    			es.setFileName(file.getOriginalFilename());
	    			es.setExecuteState(MappingState.EXECUTING);
	    			es.setExecuteStartedAt(new Date());
	    			es.setExecuteCompletedAt(null);
	    			es.setExecuteMessage(null);

	    			fdoc.setExecute(Arrays.asList(new FileExecuteState[] {es}));
	    			fdoc = fileRepository.save(fdoc);
	    			
	    			List<String> fileNames = folderService.saveUploadedFile(getCurrentUser(), getEnclosingObject(), fdoc, file);
	    			es.setContentFileNames(fileNames);
	    			es.setExecuteState(MappingState.EXECUTED);
	    			es.setExecuteCompletedAt(new Date(System.currentTimeMillis()));
	    			
	    			zipUploadedFile(getCurrentUser(), getEnclosingObject(), fdoc, es);
	    		} else {
	    			// TODO : should update file from URL
	    		}
	    		
				if (ur.isActive() != null) {
					fdoc.setActive(ur.isActive().booleanValue());
				}

	    	});
	    }
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				clearExecution();
					
				fileRepository.delete(object);
	
				return true;
			}
		}

		@Override
		public String localSynchronizationString() {
			return getContainerFileSystemConfiguration().getId().toString() + ":" + getPrimaryId().toString();
		}

		@Override
		public FileSystemConfiguration getContainerFileSystemConfiguration() {
			return containerFileSystemConfiguration;
		}

		@Override
		public boolean clearExecution() throws Exception {
			return clearFile(currentUser, dataset, object, object.checkExecuteState(fileSystemConfiguration.getId()));
//			return serviceUtils.clearExecution(this);
		}

		@Override
		public boolean clearExecution(FileExecuteState es) throws Exception {
//			return serviceUtils.clearExecution(this, es);
			return clearFile(currentUser, dataset, object, es);
		}
		
		@Override
		public FileResponse asResponse() {
			
	    	FileResponse response = new FileResponse();
	    	response.setId(object.getId().toString());
	    	response.setName(object.getName());
	    	response.setDescription(object.getDescription());
	    	response.setUrl(object.getUrl());
	    	response.setDatasetId(object.getDatasetId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	response.setActive(object.isActive());
	    	response.setOrder(object.getOrder());
	    	
	    	response.setGroup(object.getGroup());
	    	
	    	if (currentUser != null) {
	    		response.setOwnedByUser(currentUser.getId().equals(object.getUserId().toString()));
	    	}
	    	
	    	response.copyStates(object, getDatasetTripleStoreVirtuosoConfiguration(), fileSystemConfiguration);
	    	
	        return response;
		}
		
		@Override
		public String getDescription() {
			return object.getName();
		}
		
		@Override
		public TaskType getExecuteTask() {
			return TaskType.FILE_EXECUTE;
		}

		@Override
		public TaskType getClearLastExecutionTask() {
			return TaskType.FILE_CLEAR_LAST_EXECUTION;
		}

		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByFileIdAndFileSystemConfigurationId(getObject().getId(), getContainerFileSystemConfiguration().getId(), type).orElse(null);
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}

		@Override
		public void publish(Properties props) throws Exception {
			tripleStore.publish(currentUser, datasetService.getContainer(currentUser, dataset), this);
		}

		@Override
		public void unpublish(Properties props) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public TaskType getPublishTask() {
			return TaskType.FILE_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return null;
		}

		@Override
		public TaskType getRepublishTask() {
			return null;
		}

	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	@Override
	public FileContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		FileContainer fc = new FileContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (fc.getObject() == null || fc.getEnclosingObject() == null) {
			return null;
		} else {
			return fc;
		}
	}
	
	public FileContainer getContainer(UserPrincipal currentUser, FileDocument fdoc, Dataset dataset) {
		FileContainer fc = new FileContainer(currentUser, fdoc, dataset);

		if (fc.getObject() == null || fc.getEnclosingObject() == null) {
			return null;
		} else {
			return fc;
		}
	}
    
	
	@Override
	public FileDocument create(UserPrincipal currentUser, Dataset dataset, FileUpdateRequest ur) throws Exception {
		
    	Date date = new Date(System.currentTimeMillis());

    	FileDocument fd = new FileDocument(dataset);
    	fd.setUserId(new ObjectId(currentUser.getId()));
    	fd.setName(ur.getName());
    	fd.setDescription(ur.getDescription());
    	fd.setUrl(ur.getUrl());
    	
    	fd.setActive(ur.isActive());
    	fd.setGroup(ur.getGroup());

		FileExecuteState es = new FileExecuteState();
		fd.setExecute(Arrays.asList(new FileExecuteState[] {es}));

		MultipartFile file = null;
		if (ur.getFile() != null) {
			file = ur.getFile();
		} else if (ur.getUrl() != null) {
			file = serviceUtils.download(ur.getUrl());
		}
		
		es.setDatabaseConfigurationId(fileSystemConfiguration.getId());
		es.setFileName(file.getOriginalFilename());
		es.setExecuteState(MappingState.EXECUTING);
		es.setExecuteStartedAt(date);
		es.setExecuteCompletedAt(null);
		es.setExecuteMessage(null);
		
		fd = fileRepository.save(fd);
		
		List<String> fileNames = folderService.saveUploadedFile(currentUser, dataset, fd, file);
		es.setContentFileNames(fileNames);
		es.setExecuteState(MappingState.EXECUTED);
		es.setExecuteCompletedAt(new Date(System.currentTimeMillis()));
		es.setExecuteMessage(null);
		
		fd = create(fd);
		
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
		
		if (fd.getExecuteState(fileSystemConfiguration.getId()) == es) {
			fd.setExecute(null);
		}
		
		fileRepository.save(fd);

		return true;
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
		FileExecuteState es = fd.getExecuteState(fileSystemConfiguration.getId());
		
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
    
	public Optional<String> previewLast(UserPrincipal currentUser, String id) throws IOException {

        Optional<FileDocument> doc = fileRepository.findById(new ObjectId(id));

        if (!doc.isPresent()) {
        	return Optional.empty();
        }
        
        FileDocument fd = doc.get();
        FileExecuteState es = fd.getExecuteState(fileSystemConfiguration.getId());

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

// 	public List<FileResponse> getFiles(UserPrincipal currentUser, ObjectId datasetId) {
//
//		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(datasetId, new ObjectId(currentUser.getId()));
//
//		if (!datasetOpt.isPresent()) {
//			return new ArrayList<>();
//		}
//		
//		Dataset dataset = datasetOpt.get();
//		
//		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
//
//		List<FileDocument> fdocs = fileRepository.findByDatasetIdAndFileSystemConfigurationIdAndUserId(datasetId, fileSystemConfiguration.getId(), new ObjectId(currentUser.getId()));
//
//		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;
//		
//		List<FileResponse> response = new ArrayList<>();
//		for (FileDocument fdoc : fdocs) {
//			response.add(modelMapper.file2FileResponse(vc, fdoc));
//		}
//		
//		return response;
//	}	

	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return serviceUtils.publish(tdescr, wsService);
	}

	@Override
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return null;
	}
	
	@Override
	public ListPage<FileDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, null, page);
	}

	@Override
	public ListPage<FileDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		return getAllByUser(dataset, userId, null, page);
	}

	@Override
	public ListPage<FileDocument> getAll(FileLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);
	}

	@Override
	public ListPage<FileDocument> getAllByUser(ObjectId userId, FileLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<FileDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, FileLookupProperties lp, Pageable page) {
		if (lp == null) {
			lp = new FileLookupProperties();
		}
		
		lp.setFileSystemConfigurationId(fileSystemConfiguration.getId());
		
		if (page == null) {
			return ListPage.create(fileRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(fileRepository.find(userId, dataset, lp, database.getId(), page));
		}	
	}
	
	@Override
	public FileLookupProperties createLookupProperties() {
		return new FileLookupProperties();
	}

}
