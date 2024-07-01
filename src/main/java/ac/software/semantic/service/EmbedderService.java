package ac.software.semantic.service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataServiceRank;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.payload.request.EmbedderUpdateRequest;
import ac.software.semantic.payload.response.EmbedderDocumentResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.EmbedderDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.root.DataServiceRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.DataServiceContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.vocs.SEMRVocabulary;

@Service
public class EmbedderService implements ExecutingPublishingService<EmbedderDocument, EmbedderDocumentResponse>, 
                                        EnclosedCreatableService<EmbedderDocument, EmbedderDocumentResponse, EmbedderUpdateRequest, Dataset> {

	private Logger logger = LoggerFactory.getLogger(EmbedderService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Autowired
	private SchemaService schemaService;

	@Value("${d2rml.execute.request-cache-size}")
	private int restCacheSize;

	@Value("${d2rml.extract.min-size:0}")
	private long extractMinSize; 

	@Autowired
	@Qualifier("triplestore-configurations")
	private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Autowired
	private EmbedderDocumentRepository embedderRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private DataServicesService dataServicesService;

	@Autowired
	private SPARQLService sparqlService;

	@Autowired
	private TripleStore tripleStore;
	
	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private ServiceUtils serviceUtils;
	
	@Autowired
	private DataServiceRepository dataServicesRepository;

	@Override
	public Class<? extends EnclosedObjectContainer<EmbedderDocument,EmbedderDocumentResponse,Dataset>> getContainerClass() {
		return EmbedderContainer.class;
	}
	
	@Override 
	public DocumentRepository<EmbedderDocument> getRepository() {
		return embedderRepository;
	}
	
	public class EmbedderContainer extends EnclosedObjectContainer<EmbedderDocument, EmbedderDocumentResponse,Dataset> 
	                               implements DataServiceContainer<EmbedderDocument,EmbedderDocumentResponse,MappingExecuteState,Dataset>, 
	                                          PublishableContainer<EmbedderDocument,EmbedderDocumentResponse,MappingExecuteState, MappingPublishState, Dataset>,
	                                          UpdatableContainer<EmbedderDocument,EmbedderDocumentResponse,EmbedderUpdateRequest> {
		private ObjectId embedderId;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		private EmbedderContainer(UserPrincipal currentUser, ObjectId embedderId) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.embedderId = embedderId;
		
			load();
		}
		
		private EmbedderContainer(UserPrincipal currentUser, EmbedderDocument edoc) {
			this(currentUser, edoc, null);
		}
		
		private EmbedderContainer(UserPrincipal currentUser, EmbedderDocument edoc, Dataset dataset) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;

			this.embedderId = edoc.getId();
			this.object = edoc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return embedderId;
		}
		
		@Override 
		public DocumentRepository<EmbedderDocument> getRepository() {
			return embedderRepository;
		}
		
		@Override
		public EmbedderService getService() {
			return EmbedderService.this;
		}

		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(object.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			setEnclosingObject(datasetOpt.get());
		}
		
		public DataService getDataService() {
			return dataServicesRepository.findByIdentifierAndType(object.getEmbedder(), DataServiceType.EMBEDDER).orElse(null);
		}
		
		@Override
		public EmbedderDocument update(EmbedderUpdateRequest updateEmbedderRequest) throws Exception {

			return update(iec -> {
				EmbedderDocument edoc = iec.getObject();
				edoc.setEmbedder(updateEmbedderRequest.getEmbedder());
				edoc.setVariant(updateEmbedderRequest.getVariant());
				edoc.setOnClass(updateEmbedderRequest.getOnClass());
				edoc.setElement(updateEmbedderRequest.getIndexElement());
//				edoc.setKeys(updateEmbedderRequest.getKeys());
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				clearExecution();
					
				embedderRepository.delete(object);
	
				return true;
			}
		}
		
		@Override
		public String localSynchronizationString() {
			return getContainerFileSystemConfiguration().getId().toString() + ":" + getObject().getId().toString();
		}

		@Override
		public FileSystemConfiguration getContainerFileSystemConfiguration() {
			return containerFileSystemConfiguration;
		}

		@Override 
		public void publish(Properties props) throws Exception {
			tripleStore.publish(getDatasetTripleStoreVirtuosoConfiguration(), this);
		}

		@Override 
		public void unpublish(Properties props) throws Exception {
			tripleStore.unpublish(getDatasetTripleStoreVirtuosoConfiguration(), this);
		}

		@Override
		public boolean clearExecution() throws Exception {
			return serviceUtils.clearExecution(this);
		}

		@Override
		public boolean clearExecution(MappingExecuteState es) throws Exception {
			return serviceUtils.clearExecution(this, es);
		}
		
		@Override
		public EmbedderDocumentResponse asResponse() {
	    	EmbedderDocumentResponse response = new EmbedderDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setElement(object.getElement());
	    	response.setEmbedder(object.getEmbedder());
	    	response.setVariant(object.getVariant());
	    	response.setOnClass(object.getOnClass());
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	if (currentUser != null) {
	    		response.setOwnedByUser(currentUser.getId().equals(object.getUserId().toString()));
	    	}
	    	
	    	response.copyStates(object, getDatasetTripleStoreVirtuosoConfiguration(), fileSystemConfiguration);

	        return response;
		}
		
		@Override
		public String getDescription() {
			return object.getEmbedder();
		}
		
		@Override
		public TaskType getExecuteTask() {
			return TaskType.EMBEDDER_EXECUTE;
		}

		@Override
		public TaskType getClearLastExecutionTask() {
			return TaskType.EMBEDDER_CLEAR_LAST_EXECUTION;
		}

		@Override
		public TaskType getPublishTask() {
			return TaskType.EMBEDDER_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return TaskType.EMBEDDER_UNPUBLISH;
		}

		@Override
		public TaskType getRepublishTask() {
			return TaskType.EMBEDDER_REPUBLISH;
		}
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByEmbedderIdAndFileSystemConfigurationId(getObject().getId(), getContainerFileSystemConfiguration().getId(), type).orElse(null);
		}	

		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}

		@Override
		public ExecutionOptions buildExecutionParameters() {
			Map<String, Object> params = new HashMap<>();
			
			DatasetCatalog dcg = schemaService.asCatalog(getEnclosingObject());
	    	String fromClause = schemaService.buildFromClause(dcg);

			TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

	    	SPARQLStructure ss = sparqlService.toSPARQL(object.getElement(), AnnotatorDocument.getKeyMetadataMap(object.getKeysMetadata()), true);
//	    	System.out.println(ss.construct(fromClause, "{@@id@@}"));
//	    	System.out.println(ss.getWhereClause());
				
			params.put("iigraph", fromClause);
			params.put("iiclass", object.getOnClass());
			params.put("iiquery", ss.construct(fromClause, "{@@id@@}"));
			params.put("iirdfsource", vc.getSparqlEndpoint());
			params.put("iiembedder", object.asResource(resourceVocabulary));

			applyParameters(params);
//
			return new ExecutionOptions(params, null);
		}
		
		@Override
		public String applyPreprocessToMappingDocument(ExecutionOptions eo) throws Exception {
			
			String str = dataServicesService.readMappingDocument(object, eo.getParams(), DataServiceRank.SINGLE);
			
			str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());

			return str;
		}

	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	@Override
	public EmbedderContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		EmbedderContainer ec = new EmbedderContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public ObjectContainer<EmbedderDocument,EmbedderDocumentResponse> getContainer(UserPrincipal currentUser, EmbedderDocument edoc) {
		EmbedderContainer ec = new EmbedderContainer(currentUser, edoc);

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}


	@Override
	public EmbedderContainer getContainer(UserPrincipal currentUser, EmbedderDocument edoc, Dataset dataset) {
		EmbedderContainer ec = new EmbedderContainer(currentUser, edoc, dataset);

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public EmbedderDocument create(UserPrincipal currentUser, Dataset dataset, EmbedderUpdateRequest ur) throws Exception {
		EmbedderDocument edoc = new EmbedderDocument(dataset);
		
		edoc.setUserId(new ObjectId(currentUser.getId()));
		edoc.setEmbedder(ur.getEmbedder());
		edoc.setVariant(ur.getVariant());
		edoc.setOnClass(ur.getOnClass());
		edoc.setElement(ur.getIndexElement());
//		edoc.setKeys(ur.getKeys());

		return create(edoc);
	}

	@Override
	@Async("mappingExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return serviceUtils.execute(tdescr, wsService);
	}
	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return serviceUtils.publish(tdescr, wsService);
	}
	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return serviceUtils.unpublish(tdescr, wsService);
	}

	@Override
	public ListPage<EmbedderDocument> getAllByUser(ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(embedderRepository.findByDatabaseIdAndUserId(database.getId(), userId));
		} else {
			return ListPage.create(embedderRepository.findByDatabaseIdAndUserId(database.getId(), userId, page));
		}
	}
	
	@Override
	public ListPage<EmbedderDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(embedderRepository.findByDatasetIdInAndUserId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId));
		} else {
			return ListPage.create(embedderRepository.findByDatasetIdInAndUserId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId, page));
		}
	}

}
