package ac.software.semantic.service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.bson.types.ObjectId;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DependencyBinding;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.MappingDataFile;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TemplateService;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.ValidationResult;
import ac.software.semantic.model.base.ExecutableDocument;
import ac.software.semantic.model.base.PublishableDocument;
import ac.software.semantic.model.base.ValidatableDocument;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.constants.type.TemplateType;
import ac.software.semantic.model.state.DatasetPublishState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.state.ValidateState;
import ac.software.semantic.payload.notification.ExecuteNotificationObject;
import ac.software.semantic.payload.notification.PublishNotificationObject;
import ac.software.semantic.payload.request.MappingInstanceUpdateRequest;
import ac.software.semantic.payload.request.MappingUpdateRequest;
import ac.software.semantic.payload.response.D2RMLValidateResponse;
import ac.software.semantic.payload.response.MappingInstanceResponse;
import ac.software.semantic.payload.response.MappingResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.MappingDocumentRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.root.TemplateServiceRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.PrototypeService.PrototypeContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.MappingObjectIdentifier;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.container.ValidatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.lookup.MappingLookupProperties;
import ac.software.semantic.service.monitor.ExecuteMonitor;
import ac.software.semantic.service.monitor.GenericMonitor;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.util.SerializationTransformation;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.FileSystemPlainTextOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;

@Service
public class MappingService implements EnclosedCreatableLookupService<MappingDocument, MappingResponse,MappingUpdateRequest, Dataset, MappingLookupProperties>, 
									   EnclosingService<MappingDocument, MappingResponse,Dataset>,
                                       ExecutingPublishingService<MappingDocument,MappingResponse>, 
                                       ValidatingService<MappingDocument,MappingResponse>,
                                       IdentifiableDocumentService<MappingDocument, MappingResponse> {

	private Logger logger = LoggerFactory.getLogger(MappingService.class);

    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private D2RMLService d2rmlService;
	
	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private MappingDocumentRepository mappingRepository;

	@Autowired
	private PrototypeDocumentRepository prototypesRepository;

	@Autowired
	private TemplateServiceRepository templateRepository;

	@Autowired
	private TemplateServicesService templateService;

	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Lazy
	@Autowired
	private DatasetService datasetService;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Value("${d2rml.execute.request-cache-size}")
	private int restCacheSize;

	@Value("${d2rml.extract.min-size:0}")
	private long extractMinSize; 
	
	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

	@Autowired
	@Qualifier("filesystem-configuration")
	public FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	private FolderService folderService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private TripleStore tripleStore;

	@Autowired
	private ServiceUtils serviceUtils;
	
	@Autowired
	private PrototypeService prototypeService;

	@Lazy
	@Autowired
	private UserService userService;

	@Override
	public Class<? extends EnclosedObjectContainer<MappingDocument,MappingResponse,Dataset>> getContainerClass() {
		return MappingContainer.class;
	}
	
	@Override
	public DocumentRepository<MappingDocument> getRepository() {
		return mappingRepository;
	}
	
	public class MappingContainer extends EnclosedObjectContainer<MappingDocument,MappingResponse,Dataset> 
	                              implements ExecutableContainer<MappingDocument,MappingResponse,MappingExecuteState,Dataset>, 
//	                                         IntermediatePublishableContainer<MappingDocument,MappingExecuteState,MappingPublishState,Dataset>,
	                                         PublishableContainer<MappingDocument,MappingResponse,MappingExecuteState,MappingPublishState,Dataset>,
	                                         ValidatableContainer<MappingDocument,MappingResponse>,
	                                         UpdatableContainer<MappingDocument,MappingResponse,MappingUpdateRequest> {
		private ObjectId mappingId;
		private ObjectId mappingInstanceId;
		
		private MappingInstance mappingInstance; 
		
		private FileSystemConfiguration containerFileSystemConfiguration;
		
		public MappingContainer(UserPrincipal currentUser, Dataset dataset, MappingDocument mappingDocument, MappingInstance mappingInstance) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.mappingId = mappingDocument.getId();
			if (mappingInstance != null) {
				this.mappingInstanceId = mappingInstance.getId();
			}
			
			this.currentUser = currentUser;
			
			this.dataset = dataset;
			
			this.object = mappingDocument;
			this.mappingInstance = mappingInstance;
		}
		
		public MappingContainer(UserPrincipal currentUser, MappingDocument mc, MappingInstance mi) {
			this(currentUser, mc, mi, null);
		}

		public MappingContainer(UserPrincipal currentUser, String mappingId, String mappingInstanceId) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.mappingId = new ObjectId(mappingId);
			this.mappingInstanceId = mappingInstanceId  != null ? new ObjectId(mappingInstanceId) : null;

			this.currentUser = currentUser;
			
			load();
			
			loadDataset();
		}
		
		public MappingContainer(UserPrincipal currentUser, ObjectId mappingId, ObjectId mappingInstanceId) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.mappingId = mappingId;
			this.mappingInstanceId = mappingInstanceId;

			this.currentUser = currentUser;
			
			load();
			
			loadDataset();
		}
		
		public MappingContainer(DatasetContainer dc, String mappingId, String mappingInstanceId) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.mappingId = new ObjectId(mappingId);
			this.mappingInstanceId = mappingInstanceId  != null ? new ObjectId(mappingInstanceId) : null;
			
			this.currentUser = dc.getCurrentUser();

			this.dataset = dc.getObject();

			load();
			
			loadDataset();
		}
		
		public MappingContainer(UserPrincipal currentUser, MappingDocument mc, MappingInstance mi, Dataset dataset) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.mappingId = mc.getId();
			this.mappingInstanceId = mc.hasParameters() && mi != null ? mi.getId() : null;
			
			this.currentUser = currentUser;

			object = mc;
			mappingInstance = mi;
			
			if (dataset != null) {
				loadDataset();
			}
		}
		
		public List<MappingContainer> expandToInstances() {
			List<MappingContainer> res = new ArrayList<>();
		
			MappingDocument mdoc = getObject();
			
			for (MappingInstance mi : mdoc.getInstances()) {
				res.add(new MappingContainer(currentUser, mdoc, mi, getEnclosingObject()));
			}
			
			return res;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return mappingId;
		}

		@Override
		public DocumentRepository<MappingDocument> getRepository() {
			return mappingRepository;
		}

		@Override
		public MappingService getService() {
			return MappingService.this;
		}
		
		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}

		@Override
		public void load() {
			Optional<MappingDocument> mappingOpt = mappingRepository.findById(mappingId);

			if (!mappingOpt.isPresent()) {
				return;
			}

			object = mappingOpt.get();
			mappingInstance = findMappingInstance(object, mappingInstanceId != null ? mappingInstanceId.toString() : null);
			if (mappingInstance != null) {
				mappingInstanceId = mappingInstance.getId(); // !important correct null mappingInstanceId for non parametric mappings 
			}
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findById(object.getDatasetId());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			dataset = datasetOpt.get();			
		}
		
		@Override
		public MappingDocument update(MappingUpdateRequest mur) throws Exception {
			
			return update(iec -> {
				MappingDocument mdoc = iec.getObject();

				String fileName = null;
				String fileContents = "";
				ObjectId templateId = null; 
				ObjectId d2rmlId = null;
				
				PrototypeContainer pdoc = null;
				List<DataServiceParameter> parameters = mur.getParameters();
				List<DependencyBinding> dependencies = null;

				if (mur.getFile() != null) {
					fileName = mur.getFile().getOriginalFilename();
					fileContents = new String(mur.getFile().getBytes());
					
					D2RMLValidateResponse res = d2rmlService.validate(fileContents);
					parameters = res.getParameters();
					
					if (res.getDependencies() != null && res.getDependencies().size() > 0) {
						dependencies = new ArrayList<>();
						for (String s : res.getDependencies()) {
							DependencyBinding dp = new DependencyBinding();
							dp.setName(s);
							dependencies.add(dp);
						}
					}

					templateId = null;
					d2rmlId = null;
					
				} else if (mur.getTemplateId() != null) {
					templateId = new ObjectId(mur.getTemplateId());
					
					fileName = null;
					d2rmlId = null;
					
					// TODO get parameters
				} else if (mur.getD2rmlId() != null) {
					d2rmlId = new ObjectId(mur.getD2rmlId());
					
					pdoc = prototypeService.getContainer(null, new SimpleObjectIdentifier(d2rmlId));
					if (pdoc != null) {
						parameters = pdoc.getObject().getParameters();
						dependencies = pdoc.getObject().getDependencies();
					} else {
						d2rmlId = null;
					}
					
					fileName = null;
					templateId = null;

				} else {
					fileName = mdoc.getFileName();
					fileContents = mdoc.getFileContents();
					templateId = mdoc.getTemplateId();
					d2rmlId = mdoc.getD2rmlId();
					parameters = mdoc.getParametersMetadata();
					dependencies = mdoc.getDependencies();
				}
				
				mdoc.setName(mur.getName());
				mdoc.setDescription(mur.getDescription());
				mdoc.setIdentifier(mur.getIdentifier());
				mdoc.setGroupIdentifiers(mur.getGroupIdentifiers());
//				mdoc.setParameters(parameters);
				mdoc.applyParameters(parameters);
				mdoc.setDependencies(dependencies);
				
				if (fileName != null && fileName.length() == 0) {
					fileName = null;
				}
				mdoc.setFileName(fileName);
				
				mdoc.setTemplateId(templateId);
				
				mdoc.setD2rmlId(d2rmlId);
				if (d2rmlId != null || templateId != null) {
					mdoc.setD2rmlIdBound(mur.isD2rmlIdBound());
				} else {
					mdoc.setD2rmlIdBound(null);
				}
				
				if (mur.isActive() != null) {
					mdoc.setActive(mur.isActive().booleanValue());
				}
				
				List<ObjectId> shacl = null;
				if (mur.getShaclId() != null) {
					shacl = new ArrayList<>();
					for (String s : mur.getShaclId()) {
						shacl.add(new ObjectId(s));	
					}
				}
				mdoc.setShaclId(shacl);
				
				if (templateId == null && d2rmlId == null) {
					mdoc.setFileContents(fileContents);
				} else if (templateId != null) {
					Optional<TemplateService> tempOpt = templateRepository.findById(templateId);
					
					if (!tempOpt.isPresent()) {
						mdoc.setFileContents("");
					} else {
						String d2rml = templateService.getEffectiveTemplateString(tempOpt.get());
								
						d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(mdoc.getUuid()).toString());
	//					d2rml = d2rml.replace("{@@SAGE_TEMPLATE_DATASET_URI@@}", resourceVocabulary.getDatasetAsResource(ds.getUuid()).toString());
						d2rml = d2rml.replace("{@@SAGE_TEMPLATE_ITEM_BASE@@}", resourceVocabulary.getItemBaseResource().toString());
	
						mdoc.setFileContents(d2rml);
					}
				} else if (d2rmlId != null) {
					mdoc.setFileContents(pdoc.getObject().getContent());
					
				}
			});
		}
		
		public MappingDocument saveAttachment(MultipartFile file) throws Exception {
			
			return update(iec -> {
				if (mappingInstanceId == null || !object.hasParameters()) {
					object.addDataFile(new MappingDataFile(file.getOriginalFilename(), fileSystemConfiguration.getId()));
					try {
						folderService.saveAttachment(getObjectOwner(), object, file);
					} catch (Exception ex) {
						folderService.deleteAttachment(getObjectOwner(), object, null, file.getOriginalFilename());
						throw ex;
					}
				} else {
					MappingInstance mi = getMappingInstance();
					
					mi.addDataFile(new MappingDataFile(file.getOriginalFilename(), fileSystemConfiguration.getId()));
					try {
						folderService.saveAttachment(getObjectOwner(), object, mi, file);
					} catch (Exception ex) {
						folderService.deleteAttachment(getObjectOwner(), object, mi, file.getOriginalFilename());
						throw ex;
					}
				}
			});				
		}
		
		public MappingDocument removeAttachment(String filename) throws Exception {
		
			return update(iec -> {
				if (mappingInstanceId == null || !object.hasParameters()) {
					folderService.deleteAttachment(getObjectOwner(), object, null, filename);
	
					object.removeDataFile(new MappingDataFile(filename, fileSystemConfiguration.getId()));
				} else {
					MappingInstance mi = getMappingInstance();
					folderService.deleteAttachment(getObjectOwner(), object, mi, filename);
					
					mi.removeDataFile(new MappingDataFile(filename, fileSystemConfiguration.getId()));
				}
			});				
		}

		public File getAttachment(String filename) throws Exception {
			return folderService.getAttachment(getObjectOwner(), object, mappingInstanceId != null &&  object.hasParameters() ? getMappingInstance() : null, filename);
		}    
		
		@Override
		public boolean isPublished() {
			if (mappingInstance != null) {
				return PublishableContainer.super.isPublished();
			} else {
				List<MappingInstance> instanceList = object.getInstances();
				if (instanceList != null) {
					for (MappingInstance mi : instanceList) {
						MappingContainer mc = getContainer(currentUser, dataset, object, mi);
						if (mc.isPublished()) {
							return true;
						}
					}
				}
			}
			
			return false;
		}
		
		@Override
		public PublishableDocument<MappingExecuteState,MappingPublishState> getPublishDocument() {
			return getMappingInstance();
		}
		
		@Override
		public ExecutableDocument<MappingExecuteState> getExecuteDocument() {
			return getMappingInstance();
		}

		@Override
		public ValidatableDocument<ValidateState> getValidateDocument() {
			return getMappingInstance();
		}

		@Override
		public ObjectId getSecondaryId() {
			return getMappingInstanceId();
		}
		
		public void setMappingId(ObjectId mappingId) {
			this.mappingId = mappingId;
		}

		public ObjectId getMappingInstanceId() {
			return mappingInstanceId;
		}

		public void setMappingInstanceId(ObjectId mappingInstanceId) {
			this.mappingInstanceId = mappingInstanceId;
		}
		
		public String idsToString() {
			return mappingId + (mappingInstanceId != null ? ("_" + mappingInstanceId) : "");
		}
		
		public MappingInstance getMappingInstance() {
			return mappingInstance;
		}
		
		@Override
		public String localSynchronizationString() {
			return getContainerFileSystemConfiguration().getId() + ":" + mappingId + (mappingInstanceId != null ? ":" + mappingInstanceId : "");
		}

		@Override
		public FileSystemConfiguration getContainerFileSystemConfiguration() {
			return containerFileSystemConfiguration;
		}

		@Override
		public boolean clearExecution() {
			return MappingService.this.clearExecution(this);
		}
		
		@Override
		public boolean clearExecution(MappingExecuteState es) {
			return MappingService.this.clearExecution(this, es);
		}
		
		@Override
		public MappingResponse asResponse() {
			ObjectId templateId = object.getTemplateId();
			TemplateService template = null;
			if (templateId != null) {
				Optional<TemplateService> tempOpt = templateRepository.findById(templateId);
				if (tempOpt.isPresent()) {
					template = tempOpt.get();
				}
			}
			
	    	MappingResponse response = new MappingResponse();
	    	response.setId(object.getId().toString());
	    	response.setName(object.getName());
	    	response.setDescription(object.getDescription());
//	    	response.setD2RML(doc.getD2RML());
	    	response.setDatasetId(object.getDatasetId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setIdentifier(object.getIdentifier());
	    	response.setGroupIdentifiers(object.getGroupIdentifiers());
	    	response.setType(object.getType().toString());
	    	response.applyParameters(object.getParameters(), object.getParametersMetadata());
	    	response.setFileName(object.getFileName());
//	    	response.setFileContents(doc.getFileContents());
	    	response.setTemplate(object.getTemplateId() != null && template.getType() != TemplateType.MAPPING_SAMPLE);
	    	response.setTemplateId(object.getTemplateId() != null ? object.getTemplateId().toString() : null);
	    	response.setD2rmlId(object.getD2rmlId() != null ? object.getD2rmlId().toString() : null);
	   		response.setD2rmlIdBound(object.getD2rmlIdBound());
	   		
	   		if (object.getD2rmlId() != null && object.getD2rmlIdBound() != null && object.getD2rmlIdBound()) {
	   			Optional<PrototypeDocument> pdoc  = prototypesRepository.findById(object.getD2rmlId());
	   			if (pdoc.isPresent()) {
	   				response.setD2rmlName(pdoc.get().getName());
	   			}
	   		}

	   		if (object.getTemplateId() != null && object.getD2rmlIdBound() != null && object.getD2rmlIdBound()) {
				Optional<TemplateService> tempOpt = templateRepository.findById(object.getTemplateId());
	   			if (tempOpt.isPresent()) {
	   				response.setD2rmlName(tempOpt.get().getName());
	   			}
	   		}
	   		
	    	if (currentUser != null) {
	    		response.setOwnedByUser(currentUser.getId().equals(object.getUserId().toString()));
	    	}
	    	
	    	response.setGroup(object.getGroup());
	    	
	    	List<MappingDataFile> mdFiles = object.checkDataFiles(fileSystemConfiguration);
	    	if (mdFiles != null) {
	    		List<String> sFiles = new ArrayList<>();
	    		for (MappingDataFile mdf : mdFiles) {
	    			sFiles.add(mdf.getFilename());
	    		}
	    		response.setDataFiles(sFiles);
	    	}
	    	response.setActive(object.isActive());
	    	response.setOrder(object.getOrder());
	    	
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	if (object.getShaclId() != null) {
	    		List<String> shid = new ArrayList<>();
	    		for (ObjectId sh : object.getShaclId()) {
	    			shid.add(sh.toString());
	    		}
	    		response.setShaclId(shid);
	    	}
	    	
	    	List<MappingInstanceResponse> instances = new ArrayList<>();
	    	for (MappingInstance inst : object.getInstances()) {
	    		instances.add(modelMapper.mappingInstance2MappingInstanceResponse(getDatasetTripleStoreVirtuosoConfiguration(), inst));
	    	}
	    	
	    	response.setInstances(instances);
	    	
	    	ArrayList<String> files = new ArrayList<>();
	    	
//	    	List<File> ff = folderService.getUploadedFiles(currentUser, object);
	    	List<File> ff = folderService.getUploadedFiles(getObjectOwner(), object);
	    	if (ff != null) {
	    		for (File f : ff) {
					files.add(f.getName());
				}
			}
	    	
	    	response.setFiles(files);

	        return response;

		}

		@Override
		public String getDescription() {
			return object.getName();
		}
		
		@Override
		public TaskType getExecuteTask() {
			return TaskType.MAPPING_EXECUTE;
		}

		@Override
		public TaskType getClearLastExecutionTask() {
			return TaskType.MAPPING_CLEAR_LAST_EXECUTION;
		}

		@Override
		public TaskType getValidateTask() {
			return TaskType.MAPPING_SHACL_VALIDATE_LAST_EXECUTION;
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
			return TaskType.MAPPING_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return null;
		}

		@Override
		public TaskType getRepublishTask() {
			return null;
		}
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByMappingIdAndMappingInstanceIdAndFileSystemConfigurationId(getObject().getId(), getMappingInstanceId(), getContainerFileSystemConfiguration().getId(), type).orElse(null);
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}	
		
		@Override
		public boolean delete() throws Exception {   
			
			synchronized (saveSyncString()) {

				if (mappingInstance != null) {
					MappingService.this.clearExecution(this);
						
					if (mappingInstance.getDataFiles() != null) {
						for (MappingDataFile mdf : mappingInstance.checkDataFiles(fileSystemConfiguration)) {
							folderService.deleteAttachment(currentUser, object, mappingInstance, mdf.getFilename());
						}		
					}
					
					if (!object.hasParameters()) { // mapping is not parametric
						
						if (object.getDataFiles() != null) {
							for (MappingDataFile mdf : object.checkDataFiles(fileSystemConfiguration)) {
								folderService.deleteAttachment(currentUser, object, null, mdf.getFilename());
							}
						}
						
						mappingRepository.delete(object);
						return true;
					
					} else {
						
						update(imc -> {
							MappingContainer mc = (MappingContainer)imc;
							
							List<MappingInstance> instanceList = mc.getObject().getInstances();
							for (int k = 0; k < instanceList.size(); k++) {
								MappingInstance mi = instanceList.get(k);
								if (mi.getId().equals(mappingInstance.getId())) {
									instanceList.remove(k);
									break;
								}
							}
						});
						
						return false;
					}

				} else { // entire mapping
					
					List<MappingInstance> instanceList = object.getInstances();
					if (instanceList != null) {
						for (MappingInstance instance : instanceList) {
							MappingService.this.clearExecution(currentUser, mappingId, instance.getId());
							
							if (instance.getDataFiles() != null) {
								for (MappingDataFile mdf : instance.checkDataFiles(fileSystemConfiguration)) {
									folderService.deleteAttachment(currentUser, object, instance, mdf.getFilename());
								}		
							}
						}
					}
					
					if (object.getDataFiles() != null) {
						for (MappingDataFile mdf : object.checkDataFiles(fileSystemConfiguration)) {
							folderService.deleteAttachment(currentUser, object, null, mdf.getFilename());
						}
					}
					
					mappingRepository.delete(object);
					return true;
				}
				
				
			}
		}

		@Override
		public List<PrototypeDocument> getValidatorDocument() throws Exception {
			List<PrototypeDocument> res = new ArrayList<>();
		
			if (object.getShaclId() != null) {
				for (ObjectId shaclId : object.getShaclId()) {
					Optional<PrototypeDocument> pdocOpt = prototypesRepository.findById(shaclId);
					
					if (!pdocOpt.isPresent()) {
						throw new Exception("SHACL document not found");
					}
					
					res.add(pdocOpt.get());
				}
			}
			
			return res;
		}
		
		@Override
		public ValidationResult validate() throws Exception {
			Graph dataGraph = serviceUtils.loadAsGraph(this);
			Model shapesModel = ModelFactory.createDefaultModel();
	
			for (PrototypeDocument pdoc : getValidatorDocument()) {
				try (InputStream is = new ByteArrayInputStream(pdoc.getContent().getBytes())) {
					RDFDataMgr.read(shapesModel, is, null, Lang.TTL);
				}
			}
			
			Shapes shapes = Shapes.parse(shapesModel);
			
			ValidationReport report = ShaclValidator.get().validate(shapes, dataGraph);
			
			try (StringWriter sw = new StringWriter()) {
				RDFDataMgr.write(sw, report.getModel(), Lang.TTL);
				
				return new ValidationResult(report.conforms(), sw.toString());
			} 
		}
		
	}

	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}

	
	public List<MappingContainer> getMappingContainers(DatasetContainer dc) {
		List<MappingContainer> res = new ArrayList<>();
		
		for (MappingDocument mdoc : mappingRepository.findByDatasetId(dc.getDatasetId())) {
			for (MappingInstance mi : mdoc.getInstances()) {
				res.add(new MappingContainer(dc.getCurrentUser(), mdoc, mi));
			}
		}
		
		return res;
	}

	
	@Override
	public MappingDocument create(UserPrincipal currentUser, Dataset dataset, MappingUpdateRequest ur) throws Exception {
		
		String uuid = UUID.randomUUID().toString();
		
		MappingDocument mdoc = new MappingDocument(dataset);
		mdoc.setUserId(new ObjectId(currentUser.getId()));
		mdoc.setType(ur.getType());
		mdoc.setUuid(uuid);
		mdoc.setIdentifier(ur.getIdentifier());
		mdoc.setGroupIdentifiers(ur.getGroupIdentifiers());
		
		mdoc.setName(ur.getName());
		mdoc.setDescription(ur.getDescription());
		
		mdoc.setActive(ur.isActive());
		mdoc.setGroup(ur.getGroup());
		
		String fileName = null;
		String fileContents = "";
		ObjectId templateId = null;
		ObjectId d2rmlId = null;
		List<DataServiceParameter> parameters = ur.getParameters();
		List<DependencyBinding> dependencies = null;
		
		PrototypeContainer pdoc = null;
		
		if (ur.getFile() != null) {
			fileName = ur.getFile().getOriginalFilename();
			fileContents = new String(ur.getFile().getBytes());
			
			D2RMLValidateResponse res = d2rmlService.validate(fileContents);
			parameters = res.getParameters();
			
			if (res.getDependencies() != null && res.getDependencies().size() > 0) {
				dependencies = new ArrayList<>();
				for (String s : res.getDependencies()) {
					DependencyBinding dp = new DependencyBinding();
					dp.setName(s);
					dependencies.add(dp);
				}
			}
			
		} else if (ur.getTemplateId() != null) {
			templateId = new ObjectId(ur.getTemplateId());
			
			// TODO get parameters
			
		} else if (ur.getD2rmlId() != null) {
			d2rmlId = new ObjectId(ur.getD2rmlId());
			
			pdoc = prototypeService.getContainer(null, new SimpleObjectIdentifier(d2rmlId));
			if (pdoc != null) {
				parameters = pdoc.getObject().getParameters();
				dependencies = pdoc.getObject().getDependencies();
			} else {
				d2rmlId = null;
			}
		}
		
//		mdoc.setParameters(parameters);
		mdoc.applyParameters(parameters);
		mdoc.setDependencies(dependencies);
		
		if (fileName != null && fileName.length() == 0) {
			fileName = null;
		}
		mdoc.setFileName(fileName);
		
		if (templateId == null && d2rmlId == null) {
			mdoc.setFileContents(fileContents);
		} else if (templateId != null) {
			Optional<TemplateService> tempOpt = templateRepository.findById(templateId);
			
			if (!tempOpt.isPresent()) {
				mdoc.setFileContents("");
			} else {
				String d2rml = templateService.getEffectiveTemplateString(tempOpt.get());
						
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(uuid).toString());
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_DATASET_URI@@}", resourceVocabulary.getDatasetContentAsResource(dataset).toString()); // is this needed ?
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_ITEM_BASE@@}", resourceVocabulary.getItemBaseResource().toString());

				mdoc.setFileContents(d2rml);
				
				mdoc.setTemplateId(templateId);
			}
		} else if (d2rmlId != null) {
			mdoc.setFileContents(pdoc.getObject().getContent());
			mdoc.setD2rmlId(d2rmlId);
		}
		
		if (d2rmlId != null || templateId != null) {
			mdoc.setD2rmlIdBound(ur.isD2rmlIdBound());
		} else {
			mdoc.setD2rmlIdBound(null);
		}
		
		List<ObjectId> shacl = null;
		if (ur.getShaclId() != null) {
			shacl = new ArrayList<>();
			for (String s : ur.getShaclId()) {
				shacl.add(new ObjectId(s));	
			}
		}
		mdoc.setShaclId(shacl);
		
		return create(mdoc);
	}

	public MappingInstance createParameterBinding(MappingContainer mc, MappingInstanceUpdateRequest ur) {

		MappingDocument doc = mc.getObject();
		MappingInstance mi = doc.addInstance(ur.getBindings());
		mi.setUuid(UUID.randomUUID().toString());
		mi.setIdentifier(ur.getIdentifier());
		mi.setActive(ur.isActive());

		mappingRepository.save(doc);

		return mi;
	}
	
	public MappingInstance updateParameterBinding(MappingContainer mc, MappingInstanceUpdateRequest ur) {

		MappingInstance mi = mc.getMappingInstance();
		if (mi.getUuid() == null) { // to update old components
			mi.setUuid(UUID.randomUUID().toString()); 
		}
		
		if (ur.getBindings() != null) {
			mi.setBinding(ur.getBindings());
		}
		mi.setIdentifier(ur.getIdentifier());
		mi.setActive(ur.isActive());

		mappingRepository.save(mc.getObject());

		return mi;
	}
	
//	public boolean deleteParameterBinding(MappingContainer mc) throws IOException {
//
//		MappingDocument doc = mc.getObject();
//
//		clearExecution(mc);
//		
//		List<MappingInstance> list = doc.getInstances();
//		for (int k = 0; k < list.size(); k++) {
//			MappingInstance mi = list.get(k);
//			if (mi.getId().equals(mc.getMappingInstanceId())) {
//				list.remove(k);
//				break;
//			}
//		}
//
//		mappingRepository.save(doc);
//
//		return true;
//	}

	@Override
	public ListPage<MappingDocument> getAllByUser(ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(mappingRepository.find(userId, null, null, database.getId()));
		} else {
			return ListPage.create(mappingRepository.find(userId, null, null, database.getId(), page));
		}
	}
	
	@Override
	public ListPage<MappingDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(mappingRepository.find(userId, dataset, null, database.getId()));
		} else {
			return ListPage.create(mappingRepository.find(userId, dataset, null, database.getId(), page));
		}
	}

	public MappingInstance findMappingInstance(MappingDocument doc, String instanceId) {
		MappingInstance mi = null;
		if (instanceId == null && (doc.getInstances().size() == 0 || doc.hasParameters())) {
		} else if (instanceId == null) {
			mi = doc.getInstances().get(0);
		} else {
			for (MappingInstance mix : doc.getInstances()) {
				if (mix.getId().toString().equals(instanceId)) {
					mi = mix;
					break;
				}
			}
		}
		
		return mi;
	}
	
	public boolean clearExecution(UserPrincipal currentUser, ObjectId id, ObjectId instanceId) {
		return clearExecution(getContainer(currentUser, id, instanceId));
	}
	
	public boolean clearExecution(MappingContainer mc) {
		MappingExecuteState es = mc.mappingInstance.checkExecuteState(fileSystemConfiguration.getId());
		
		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
			return false;
		}
		
		return clearExecution(mc.getCurrentUser(), mc.getEnclosingObject(), mc.getObject(), mc.getMappingInstance(), es);
	}
	
	private boolean clearExecution(MappingContainer mc, MappingExecuteState es) {
		return clearExecution(mc.getCurrentUser(), mc.getEnclosingObject(), mc.getObject(), mc.getMappingInstance(), es);
	}
	
	public boolean clearExecution(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, MappingExecuteState es) {
		
		ProcessStateContainer psv = mi.getCurrentPublishState(virtuosoConfigurations.values());
		
		if (psv != null) {
			PublishState<ExecuteState> ps = (PublishState)psv.getProcessState();
			ExecuteState pes = ps.getExecute();
	
			// do not clear published execution
			if (pes != null && pes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) == 0 && pes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {	
				return false;
			} 
		}
		
		// trig files
		if (es.getExecuteShards() != null) {
			for (int i = 0; i < es.getExecuteShards(); i++) {
				try {
					File f = folderService.getMappingExecutionTrigFile(currentUser, dataset, doc, mi, es, i);
					boolean ok = false;
					if (f != null) {
						ok = f.delete();
						if (ok) {
							logger.info("Deleted file " + f.getAbsolutePath());
						}
					}
					if (!ok) {
						logger.warn("Failed to delete trig execution " + i + " for mapping " + doc.getUuid() + (mi.getBinding().size() == 0 ? "" : "/" + mi.getId()));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		// txt files		
		if (es.getSparqlExecuteShards() != null) {
			for (int i = 0; i < es.getSparqlExecuteShards(); i++) {
				try {
					File f = folderService.getMappingExecutionTxtFile(currentUser, dataset, doc, mi, es, i);
					boolean ok = false;
					if (f != null) {
						ok = f.delete();
						if (ok) {
							logger.info("Deleted file " + f.getAbsolutePath());
						}
					}
					if (!ok) {
						logger.warn("Failed to delete txt execution " + i + " for mapping " + doc.getUuid() + (mi.getBinding().size() == 0 ? "" : "/" + mi.getId()));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		try {
			File f = folderService.getMappingExecutionZipFile(currentUser, dataset, doc, mi, es);
			boolean ok = false;
			if (f != null) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
			}
			if (!ok) {
				logger.warn("Failed to delete zipped execution for mapping " + doc.getUuid() + (mi.getBinding().size() == 0 ? "" : "/" + mi.getId()));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		folderService.deleteMappingsExecutionDatasetFolderIfEmpty(currentUser, dataset);
		
		if (mi.checkExecuteState(fileSystemConfiguration.getId()) == es) {
			mi.deleteExecuteState(fileSystemConfiguration.getId());
		
			if (psv != null) {
				MappingPublishState ps = (MappingPublishState)psv.getProcessState();
				MappingExecuteState pes = ps.getExecute();
		
				// do not clear published execution
				if (pes != null && pes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) != 0 && pes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {
					MappingExecuteState nes = mi.getExecuteState(fileSystemConfiguration.getId());
					nes.setCount(pes.getCount());
					nes.setSparqlCount(pes.getSparqlCount());
					nes.setExecuteCompletedAt(pes.getExecuteCompletedAt());
					nes.setExecuteStartedAt(pes.getExecuteStartedAt());
					nes.setExecuteShards(pes.getExecuteShards());
					nes.setExecuteState(pes.getExecuteState());
					nes.setExecuteMessage(pes.getExecuteMessage());
				}
			}
		} 
		
		mappingRepository.save(doc);

		return true;
	}	

	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return serviceUtils.publish(tdescr, wsService);
	}
	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Date prePublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
//		System.out.println("prePublish " + tdescr.getType());
 		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		MappingContainer mc = (MappingContainer)tdescr.getContainer();
		DatasetContainer dc = datasetService.getContainer(null, mc.getEnclosingObject());

		try {
			if (tdescr.getProperties().get(ServiceProperties.IS_FIRST) == Boolean.TRUE) {
				dc.update(idc -> {			
					DatasetPublishState pes = ((DatasetContainer)idc).getPublishState(); 
					pes.startDo(pm, mc.getObject().getGroup());
				});
				
				if (tdescr.getParent() != null) {
					tdescr.getParent().getMonitor().sendMessage(new PublishNotificationObject(dc));
				}
			}

			return new Date();
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);

			try {
				dc.update(ioc -> {
					DatasetPublishState ips = ((DatasetContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm, mc.getObject().getGroup());
					}
				});
				
				if (dc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(dc));
				}
				
			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	@Override
	public Date postPublishSuccess(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
//		System.out.println("postPublishSuccess " + tdescr.getType());
 		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		MappingContainer mc = (MappingContainer)tdescr.getContainer();
		DatasetContainer dc = datasetService.getContainer(null, mc.getEnclosingObject());

		try {
			if (tdescr.getProperties().get(ServiceProperties.IS_LAST) == Boolean.TRUE) {
				dc.update(idc -> {			
					DatasetPublishState pes = ((DatasetContainer)idc).getPublishState(); 
					pes.completeDo(pm, mc.getObject().getGroup());
				});
				
				if (tdescr.getParent() != null) {
					tdescr.getParent().getMonitor().sendMessage(new PublishNotificationObject(dc));
				}
			}		

			return pm.getCompletedAt();
		
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
			try {
				dc.update(ioc -> {
					DatasetPublishState ips = ((DatasetContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm, mc.getObject().getGroup());
					}
				});
				
				if (dc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(dc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	@Override
	public Date postPublishFail(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
//		System.out.println("postPublishFail " + tdescr.getType());
		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		MappingContainer mc = (MappingContainer)tdescr.getContainer();
		DatasetContainer dc = datasetService.getContainer(null, mc.getEnclosingObject());

		try {
			dc.update(idc -> {			
				DatasetPublishState pes = ((DatasetContainer)idc).getPublishState(); 
				pes.failDo(pm, mc.getObject().getGroup());
			});

	 		
			if (dc.checkPublishState() != null) {
				if (tdescr.getParent() != null) {
					tdescr.getParent().getMonitor().sendMessage(new PublishNotificationObject(dc));
				}
			}
			
			return pm.getCompletedAt();
		
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
			try {
				dc.update(ioc -> {
					DatasetPublishState ips = ((DatasetContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm, mc.getObject().getGroup());
					}
				});
				
				if (dc.checkPublishState() != null) {
					if (tdescr.getParent() != null) {
						tdescr.getParent().getMonitor().sendMessage(new PublishNotificationObject(dc));
					}
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	//very experimental and problematic.
	public boolean unpublish(UserPrincipal currentUser, String id, String instanceId) throws Exception {

		Optional<MappingDocument> mappingOpt = mappingRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!mappingOpt.isPresent()) {
			return false;
		}

		MappingDocument mapping = mappingOpt.get();

		Optional<Dataset> dOpt = datasetRepository.findByIdAndUserId(mapping.getDatasetId(), new ObjectId(currentUser.getId()));
		
		if (!dOpt.isPresent()) {
			return false;
		}
		
		Dataset dataset = dOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
		if (psv == null) {
			return false;
		}
		
		MappingInstance mi = findMappingInstance(mapping, instanceId);
		PublishState<?> mappingPs = mi.checkPublishState(psv.getTripleStoreConfiguration().getId());
		
		if (mappingPs == null) {
			return false;
		}
		
		mappingPs.setPublishState(DatasetState.UNPUBLISHING);
		mappingRepository.save(mapping);
		
		tripleStore.unpublish(currentUser, psv.getTripleStoreConfiguration(), dataset, mapping, mi);
		
		mappingPs.setPublishState(DatasetState.UNPUBLISHED);
		mappingRepository.save(mapping);
			
		return true;
	}	
	
	public MappingContainer getContainer(DatasetContainer dc, String mappingId) {
		return getContainer(dc, mappingId, null);
	}
		

	public MappingContainer getContainer(DatasetContainer dc, String mappingId, String mappingInstanceId) {
		MappingContainer mc = new MappingContainer(dc, mappingId, mappingInstanceId);
		if (mappingInstanceId == null) {
			if (mc.getObject() == null || mc.getEnclosingObject() == null) {
				return null;
			} else {
				return mc;
			}
		} else {
			if (mc.getObject() == null || mc.getEnclosingObject() == null || mc.getMappingInstance() == null) {
				return null;
			} else {
				return mc;
			}
		}
	}

	public MappingContainer getContainer(UserPrincipal currentUser, String mappingId) {
		return getContainer(currentUser, mappingId, null);
	}			

	@Override
	public MappingContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		MappingObjectIdentifier moi = (MappingObjectIdentifier)objId;
		
		return getContainer(currentUser, moi.getId(), moi.getInstanceId());
	}
	
	@Override
	public MappingContainer getContainer(UserPrincipal currentUser, MappingDocument mdoc) {
		return new MappingContainer(currentUser, mdoc, null, null);
	}

	@Override
	public MappingContainer getContainer(UserPrincipal currentUser, MappingDocument mdoc, Dataset dataset) {
		return new MappingContainer(currentUser, mdoc, null, dataset);
	}

	public MappingContainer getContainer(UserPrincipal currentUser, ObjectId mappingId, ObjectId mappingInstanceId) {
		return getContainer(currentUser, mappingId.toString(), mappingInstanceId != null ? mappingInstanceId.toString() : null);
	}
	
	public MappingContainer getContainer(UserPrincipal currentUser, String mappingId, String mappingInstanceId) {
		MappingContainer mc = new MappingContainer(currentUser, mappingId, mappingInstanceId);
//		System.out.println(">> " + mc.mappingDocument + " " + mc.getDataset());
		if (mappingInstanceId == null) {
			if (mc.getObject() == null || mc.getEnclosingObject() == null) {
				return null;
			} else {
				return mc;
			}
		} else {
			if (mc.getObject() == null || mc.getEnclosingObject() == null || mc.getMappingInstance() == null) {
				return null;
			} else {
				return mc;
			}
		}
	}

	public MappingContainer getContainer(UserPrincipal currentUser, Dataset dataset, MappingDocument mappingDocument, MappingInstance mappingInstance) {
		MappingContainer mc = new MappingContainer(currentUser, dataset, mappingDocument, mappingInstance);
		if (mc.getObject() == null || mc.getEnclosingObject() == null) {
			return null;
		} else {
			return mc;
		}		
	}

	@Override
	@Async("shaclValidationExecutor")
	public ListenableFuture<Date> validate(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return serviceUtils.validate(tdescr, wsService);
	}
	
	@Override
	@Async("mappingExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();

		MappingContainer mc = (MappingContainer)tdescr.getContainer();
		
		mc.load(); // to keep updated data

		UserPrincipal currentUser = mc.getCurrentUser();

		MappingDocument doc = mc.getObject();
		MappingInstance mi = mc.getMappingInstance();
		
		try {
			if (doc.getD2rmlIdBound() != null && doc.getD2rmlIdBound()) {
				
				if (doc.getD2rmlId() != null) {
					logger.info("Updating " + doc.getName() + " from D2RML schema.");
					MappingUpdateRequest mur = new MappingUpdateRequest();
					mur.setActive(doc.isActive());
					mur.setD2rmlId(doc.getD2rmlId().toString());
					mur.setD2rmlIdBound(true);
					mur.setName(doc.getName());
					mur.setIdentifier(doc.getIdentifier());
					mur.setShaclIdFromObjectIds(doc.getShaclId());
					mur.setType(doc.getType());
					mur.setGroupIdentifiers(doc.getGroupIdentifiers());
					if (doc.getDependencies() != null) {
						mur.setDependencies(doc.getDependencies().stream().map(x -> x.toString()).collect(Collectors.toList()));
					}
					
					mc.update(mur);
				} else {
					logger.info("Updating " + doc.getName() + " from D2RML schema.");
					MappingUpdateRequest mur = new MappingUpdateRequest();
					mur.setActive(doc.isActive());
					mur.setTemplateId(doc.getTemplateId().toString());
					mur.setD2rmlIdBound(true);
					mur.setName(doc.getName());
					mur.setIdentifier(doc.getIdentifier());
					mur.setShaclIdFromObjectIds(doc.getShaclId());
					mur.setType(doc.getType());
					mur.setParameters(doc.getParametersMetadata());
					mur.setGroupIdentifiers(doc.getGroupIdentifiers());
					if (doc.getDependencies() != null) {
						mur.setDependencies(doc.getDependencies().stream().map(x -> x.toString()).collect(Collectors.toList()));
					}
					
					mc.update(mur);
				}
			}
		} catch (Exception ex ) {
			throw new TaskFailureException(ex, new Date());
		}
		
		doc = mc.getObject();
		
		// should be moved within try: Message is not send to monitor !!!!
		Map<String, List<ObjectId>> dependecyMappingIds = new HashMap<>();
		
		if (doc.getDependencies() != null) {
			for (DependencyBinding db : doc.getDependencies()) {
				String dependencyName = db.getName();
				
				if (db.getValue() != null) {
					for(ObjectId dependecyId : db.getValue()) {
						Optional<MappingDocument> dmappingOpt = mappingRepository.findByIdAndUserId(dependecyId, new ObjectId(currentUser.getId()));
						if (!dmappingOpt.isPresent() || !dmappingOpt.get().isExecuted(fileSystemConfiguration.getId())) {
		//					return new AsyncResult<>(false);
							throw new TaskFailureException(new Exception("Mapping dependecies are not executed"), new Date());
						}	
					}
					dependecyMappingIds.put(dependencyName, db.getValue());
				} else {
					for (ParameterBinding pb : mi.getBinding()) {
						if (pb.getName().equals(dependencyName)) {
							List<ObjectId> mapIds = new ArrayList<>();

							String[] identifiers =  pb.getValue().split(",");
							for (int i = 0; i < identifiers.length; i++) {
								identifiers[i] = identifiers[i].trim(); 
							}
							
							List<MappingDocument> dmappings = mappingRepository.findByDatasetIdAndIdentifierIn(doc.getDatasetId(), identifiers);
							for (MappingDocument dmapping : dmappings) {
								if (!dmapping.isExecuted(fileSystemConfiguration.getId())) {
				//					return new AsyncResult<>(false);
									throw new TaskFailureException(new Exception("Mapping dependecies are not executed"), new Date());
								}
								
								mapIds.add(dmapping.getId());
							}
							dependecyMappingIds.put(dependencyName, mapIds);
						}
					}

				}
			}
		}
		
		// Clearing old files. Should be done before updating execute start date
		clearExecution(mc);
		
		//should save before staring execution!
		try {
			mc.update(imc -> {
				MappingExecuteState ies = ((MappingContainer)imc).getMappingInstance().getExecuteState(fileSystemConfiguration.getId());
				ies.startDo(em); // should be here so as to set start date
			});
		} catch (Exception ex) {
			throw new TaskFailureException(ex, new Date());
		}

		doc = mc.getObject();
		
		
		try (FileSystemRDFOutputHandler rdfOuthandler = folderService.createMappingExecutionRDFOutputHandler(mc, shardSize);
				FileSystemPlainTextOutputHandler txtOuthandler = folderService.createMappingExecutionPlainTextOutputHandler(mc)) {

			em.sendMessage(new ExecuteNotificationObject(mc));

			Map<String, Object> fileMap = new HashMap<>();

			if (doc.getDataFiles() != null) {
				for (MappingDataFile mdf : doc.checkDataFiles(fileSystemConfiguration)) {
					fileMap.put(mdf.getFilename(), folderService.getAttachmentPath(mc.getObjectOwner(), doc, mdf.getFilename()));
				}
			}
			if (mi.getDataFiles() != null) {
				for (MappingDataFile mdf : mi.checkDataFiles(fileSystemConfiguration)) {
					fileMap.put(mdf.getFilename(), folderService.getAttachmentPath(mc.getObjectOwner(), doc, mi, mdf.getFilename()));
				}
			}
			
//			System.out.println(fileSystemConfiguration.getId());
//			System.out.println(fileMap);
			
			Map<String, Object> params = new HashMap<>();
			if (!fileMap.isEmpty()) {
				params.put("files", fileMap);
				params.put("vpath", true);
			}

			Map<String, Object> execParams = new HashMap<>();
			for (ParameterBinding binding : mi.getBinding()) {
				execParams.put(binding.getName(), binding.getValue());
			}

			// create dependency datasets
			if (doc.getDependencies() != null) {
				for (DependencyBinding db : doc.getDependencies()) {
					logger.info("Creating tmp dataset " + db.getName() + " " + dependecyMappingIds.get(db.getName()));

					org.apache.jena.query.Dataset paramDataset = DatasetFactory.create();
					executionResultsToModel(paramDataset, currentUser, dependecyMappingIds.get(db.getName()), null);

//					Repository paramDataset = new SailRepository(new MemoryStore());
//					try (RepositoryConnection con = paramDataset.getConnection()) {
//						executionResultsToModel(con, currentUser, dependecyMappingIds.get(db.getName()));
//					}
					
					logger.info("Created tmp dataset " + db.getName());
					
					execParams.put(db.getName(), paramDataset);
				}
			}
			
			execParams.put("SAGE_TEMPLATE_DATASET_URI", resourceVocabulary.getDatasetContentAsResource(mc.getEnclosingObject()).toString()); // is this needed ?
			
			Executor exec = new Executor(rdfOuthandler, txtOuthandler, safeExecute);
			
			folderService.checkCreateExtractTempFolder(currentUser);
			
			try {
				exec.setMonitor(em);

				String baseUri = resourceVocabulary.getMappingAsResource(doc.getUuid()).toString(); 

				D2RMLModel d2rml;
				if (doc.getFileContents() == null)  {
					d2rml = new SerializationTransformation().XtoD2RMLModel(doc.getD2RML(), baseUri, params); // legacy for old json d2rml;
				} else {
					d2rml = d2rmlService.prepare(doc, mc.getEnclosingObject(), baseUri, params);
				}
				
				exec.configureFileExtraction(extractMinSize, folderService.getExtractTempFolder(currentUser), d2rml.usesCaches() ? restCacheSize : 0);

				em.createStructure(d2rml, rdfOuthandler);
				
				logger.info("Mapping started -- id: " + mc.idsToString());

				em.sendMessage(new ExecuteNotificationObject(mc));

//				Thread.sleep(10000);
				exec.execute(d2rml, execParams);
				
				em.complete();
				
				mc.update(imc -> {
					MappingExecuteState ies = ((MappingContainer)imc).getMappingInstance().getExecuteState(fileSystemConfiguration.getId());

					ies.completeDo(em);
					ies.setExecuteShards(rdfOuthandler.getShards());
					ies.setSparqlExecuteShards(txtOuthandler.getShards());
					ies.setCount(rdfOuthandler.getTotalItems());
					ies.setSparqlCount(txtOuthandler.getTotalItems());
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getContent().getProgress());
				});
				
				logger.info("Mapping executed -- id: " + mc.idsToString() + ", shards: " + rdfOuthandler.getShards());

				em.sendMessage(new ExecuteNotificationObject(mc));

				try {
					zipExecution(mc, rdfOuthandler.getShards(), txtOuthandler.getShards());
				} catch (Exception ex) {
					ex.printStackTrace();
					
					logger.info("Zipping mapping execution failed -- id: " + mc.idsToString());
				}
				
				return new AsyncResult<>(em.getCompletedAt());

			} catch (Exception ex) {
//				ex.printStackTrace();
				
				logger.info("Mapping failed -- id: " + mc.idsToString());

				em.currentConfigurationFailed(ex);
				
				throw ex;
			} finally {
				exec.finalizeFileExtraction();
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			em.complete(ex);
			
			try {
				mc.update(imc -> {
					MappingExecuteState ies = ((MappingContainer)imc).getMappingInstance().getExecuteState(fileSystemConfiguration.getId());
	
					ies.failDo(em);
					ies.setExecuteShards(0);
					ies.setSparqlExecuteShards(0);
					ies.setCount(0);
					ies.setSparqlCount(0);
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getContent().getProgress());
				});
				
				em.sendMessage(new ExecuteNotificationObject(mc));
				
			} catch (Exception iex) {
				throw new TaskFailureException(iex, new Date());
			}
			
			throw new TaskFailureException(ex, em.getCompletedAt());
			
		} finally {
			try {
				if (em != null) {
					em.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	// !!! DO NOT DELETE: Not used by not fully covered by generic method
	private File zipExecution(MappingContainer mc, int trigShards, int txtShards) throws IOException {
		return zipExecution(mc.getCurrentUser(), mc.getEnclosingObject(), mc.getObject(), mc.getMappingInstance(), mc.getExecuteState(), trigShards, txtShards);
	}
	
	// !!! DO NOT DELETE: Not used by not fully covered by generic method
	private File zipExecution(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, ExecuteState es, int trigShards, int txtShards) throws IOException {
		
		File file = folderService.createMappingsExecutionZipFile(currentUser, dataset, doc, mi, es);
		
		try (FileOutputStream fos = new FileOutputStream(file);
				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
	        for (int i = 0; i < trigShards; i++) {
	        	File fileToZip = folderService.getMappingExecutionTrigFile(currentUser, dataset, doc, mi, es, i);
	            try (FileInputStream fis = new FileInputStream(fileToZip)) {
		            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
		            zipOut.putNextEntry(zipEntry);
		 
		            byte[] bytes = new byte[1024];
		            int length;
		            while((length = fis.read(bytes)) >= 0) {
		                zipOut.write(bytes, 0, length);
		            }
	            }
	        }
	        
	        //TODO: NOT HANDLED BY ServiceUtils. zip execution !!!!
	        for (int i = 0; i < txtShards; i++) {
	        	File fileToZip = folderService.getMappingExecutionTxtFile(currentUser, dataset, doc, mi, es, i);
	            try (FileInputStream fis = new FileInputStream(fileToZip)) {
		            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
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
	
	
	public void executionResultsToModel(org.apache.jena.query.Dataset ds, UserPrincipal currentUser, List<ObjectId> mappingIds, List<String> mappingInstanceIdentifiers) throws IOException {
		if (ds == null) {
			return;
		}
		
		if (mappingIds != null) {
			for (ObjectId id : mappingIds) {
				MappingContainer mc = this.getContainer(null, id, null);
				if (mc == null) {
					continue;
				}
	
				MappingDocument doc = mc.getObject();
	
				if (currentUser == null) {
					currentUser = userService.getContainer(null, new SimpleObjectIdentifier(doc.getUserId())).asUserPrincipal();
				}
	
				for (MappingInstance mi : doc.getInstances()) {
					if (mappingInstanceIdentifiers != null && (mi.getIdentifier() == null || !mappingInstanceIdentifiers.contains(mi.getIdentifier())) && !mappingInstanceIdentifiers.contains(mi.getUuid())) {
						continue;
					}
					
					MappingExecuteState  es = mi.getExecuteState(fileSystemConfiguration.getId());
		
					if (es.getExecuteState() == MappingState.EXECUTED) {
						if (es.getExecuteShards() != null) {
					        for (int i = 0; i < es.getExecuteShards(); i++) {
					        	File file = folderService.getMappingExecutionTrigFile(currentUser, mc.getEnclosingObject(), doc, mi, es, i);
					        	if (file != null) {
						        	logger.info("Loading file " + file);
						            RDFDataMgr.read(ds, "file:/" + file.getAbsolutePath(), Lang.TRIG);
					        	}
					        }
						}
					}
				}
			}
		}
	}
	
	public void executionResultsToModel(RepositoryConnection ds, UserPrincipal currentUser, List<ObjectId> mappingIds) throws IOException {
		if (ds == null) {
			return;
		}
		
		if (mappingIds != null) {
			for (ObjectId id : mappingIds) {
				MappingContainer mc = this.getContainer(null, id, null);
				if (mc == null) {
					continue;
				}
	
				MappingDocument doc = mc.getObject();
	
				if (currentUser == null) {
					currentUser = userService.getContainer(null, new SimpleObjectIdentifier(doc.getUserId())).asUserPrincipal();
				}
	
				for (MappingInstance mi : doc.getInstances()) {
					MappingExecuteState  es = mi.getExecuteState(fileSystemConfiguration.getId());
		
					if (es.getExecuteState() == MappingState.EXECUTED) {
						if (es.getExecuteShards() != null) {
					        for (int i = 0; i < es.getExecuteShards(); i++) {
					        	File file = folderService.getMappingExecutionTrigFile(currentUser, mc.getEnclosingObject(), doc, mi, es, i);
					        	if (file != null) {
						        	logger.info("Loading file " + file);
						            ds.add(new File(file.getAbsolutePath()), null, RDFFormat.TRIG);
					        	}
					        }
						}
					}
				}
			}
		}
	}

	@Override
	public ListPage<MappingDocument> getAll(MappingLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);

	}

	@Override
	public ListPage<MappingDocument> getAllByUser(ObjectId userId, MappingLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<MappingDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, MappingLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(mappingRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(mappingRepository.find(userId, dataset, lp, database.getId(), page));
		}	
	}
	
	@Override
	public MappingLookupProperties createLookupProperties() {
		return new MappingLookupProperties();
	}

	@Override
	public boolean isValidIdentifier(MappingDocument doc, IdentifierType type) {
		if (type == IdentifierType.IDENTIFIER) {
			if (doc.getInstances().size() == 1 && doc.getInstances().get(0).getBinding() != null && doc.getInstances().get(0).getBinding().size() == 1 &&
					doc.getInstances().get(0).getBinding().get(0).getName() == null && doc.getInstances().get(0).getBinding().get(0).getValue() == null) {
				MappingInstance omi = doc.getInstances().get(0);
				return omi.getIdentifier().matches("^[0-9a-zA-Z\\-\\~\\._:]+$");

			} else {
				return doc.getIdentifier(type).matches("^[0-9a-zA-Z\\-\\~\\._:]+$");
			}
		} else {
			return false;
		}
	}
}
