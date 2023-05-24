package ac.software.semantic.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.ExecuteDocument;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingExecutePublishDocument;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.CreateEmbedderRequest;
import ac.software.semantic.payload.EmbedderDocumentResponse;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.EmbedderDocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import ac.software.semantic.vocs.SEMRVocabulary;

@Service
public class EmbedderService implements ExecutingPublishingService {

	private Logger logger = LoggerFactory.getLogger(EmbedderService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Autowired
	private SchemaService schemaService;

	@Value("${d2rml.execute.request-cache-size}")
	private int restCacheSize;

	@Value("${d2rml.extract.min-size:0}")
	private long extractMinSize; 

	@Value("${embedding.execution.folder}")
	private String embeddingsFolder;

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
	
	@Value("${dataservice.definition.folder}")
	private String dataserviceFolder;

	@Autowired
	FolderService folderService;
	
	@Autowired
	EmbedderDocumentRepository embedderRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	DataServiceRepository dataServiceRepository;
	
	@Autowired
	DataServicesService dataServicesService;

	@Autowired
	SPARQLService sparqlService;

	@Autowired
	TripleStore tripleStore;
	
    @Autowired
    @Qualifier("preprocess-functions")
    private Map<Resource, List<String>> functions;
    
    @Autowired
    @Qualifier("preprocess-operations")
    private Map<Resource, List<String>> operations;

	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends ObjectContainer> getContainerClass() {
		return EmbedderContainer.class;
	}
	
	public class EmbedderContainer extends ObjectContainer implements ExecutableContainer, PublishableContainer {
		private ObjectId embedderId;
		private EmbedderDocument embedderDocument;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		private EmbedderContainer(UserPrincipal currentUser, ObjectId embedderId) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.embedderId = embedderId;
		
			load();
		}
		
		private EmbedderContainer(UserPrincipal currentUser, EmbedderDocument edoc) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;

			this.embedderId = edoc.getId();
			this.embedderDocument = edoc;
		}

		@Override
		protected void load() {
			Optional<EmbedderDocument> embedderOpt = embedderRepository.findByIdAndUserId(embedderId, new ObjectId(currentUser.getId()));

			if (!embedderOpt.isPresent()) {
				return;
			}

			embedderDocument = embedderOpt.get();
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(embedderDocument.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			setDataset(datasetOpt.get());
		}

		@Override
		public ExecuteDocument getExecuteDocument() {
			return getEmbedderDocument();
		}

		@Override
		public MappingExecutePublishDocument<MappingPublishState> getPublishDocument() {
			return getEmbedderDocument();
		}
		
		public EmbedderDocument getEmbedderDocument() {
			return embedderDocument;
		}

		public void setEmbedderDocument(EmbedderDocument embedderDocument) {
			this.embedderDocument = embedderDocument;
		}

		@Override
		public void save(MongoUpdateInterface mui) throws Exception {
			synchronized (saveSyncString()) { 
				load();
			
				mui.update(this);
				
				embedderRepository.save(embedderDocument);
			}
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				clearExecution();
					
				embedderRepository.delete(embedderDocument);
	
				return true;
			}
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return getEmbedderId();
		}
		
		public ObjectId getEmbedderId() {
			return embedderId;
		}

		public void setEmbedderId(ObjectId embedderId) {
			this.embedderId = embedderId;
		}

		@Override
		public String localSynchronizationString() {
			return getContainerFileSystemConfiguration().getId().toString() + ":" + getEmbedderId().toString();
		}

		@Override
		public FileSystemConfiguration getContainerFileSystemConfiguration() {
			return containerFileSystemConfiguration;
		}

		@Override 
		public void publish() throws Exception {
			tripleStore.publish(getDatasetTripleStoreVirtuosoConfiguration(), this);
		}

		@Override 
		public void unpublish() throws Exception {
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
			TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();
		
			return modelMapper.embedder2EmbedderResponse(vc, embedderDocument);
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
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}
		
		@Override
		public String syncString() {
			return ("SYNC:" + containerString() + ":" + embedderId).intern();
		}

		@Override
		public String saveSyncString() {
			return ("SYNC:SAVE:" + containerString() + ":" + embedderId).intern();
		}

	}
	
	@Override
	public String synchronizedString(String id) {
		return syncString(id);
	}
	
	public static String syncString(String id) {
		return ("SYNC:" + EmbedderContainer.class.getName() + ":" + id).intern();
	}
	
	@Override
	public EmbedderContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		EmbedderContainer ec = new EmbedderContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.embedderDocument == null || ec.getDataset() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	public EmbedderContainer getContainer(UserPrincipal currentUser, EmbedderDocument edoc) {
		EmbedderContainer ec = new EmbedderContainer(currentUser, edoc);

		if (ec.embedderDocument == null || ec.getDataset() == null) {
			return null;
		} else {
			return ec;
		}
	}
    
	public EmbedderDocument create(UserPrincipal currentUser, String datasetUri, String embedder, String variant, ClassIndexElement indexElement, String onClass, List<String> keys) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

		Optional<Dataset> ds = datasetRepository.findByUuid(datasetUuid);
		if (ds.isPresent()) {

//			List<String> flatOnPath = PathElement.onPathElementListAsStringList(onPath);
			
			String uuid = UUID.randomUUID().toString();

			EmbedderDocument edoc = new EmbedderDocument();
			edoc.setUserId(new ObjectId(currentUser.getId()));
			edoc.setDatasetUuid(datasetUuid);
			edoc.setUuid(uuid);
			edoc.setDatabaseId(database.getId());
			edoc.setEmbedder(embedder);
			edoc.setVariant(variant);
			edoc.setOnClass(onClass);
			edoc.setElement(indexElement);
			edoc.setKeys(keys);
			edoc.setUpdatedAt(new Date());

			EmbedderDocument doc = embedderRepository.save(edoc);

			return doc;
		} else {
			return null;
		}
	}


	public EmbedderDocument update(UserPrincipal currentUser, String embedderId, CreateEmbedderRequest updateEmbedderRequest) {
		Optional<EmbedderDocument> embedderOpt = embedderRepository.findByIdAndUserId(new ObjectId(embedderId), new ObjectId(currentUser.getId()));
		
		if (!embedderOpt.isPresent()) {
			return null;
		}

		EmbedderDocument edoc = embedderOpt.get();
		edoc.setEmbedder(updateEmbedderRequest.getEmbedder());
		edoc.setVariant(updateEmbedderRequest.getVariant());
		edoc.setOnClass(updateEmbedderRequest.getOnClass());
		edoc.setElement(updateEmbedderRequest.getIndexElement());
		edoc.setKeys(updateEmbedderRequest.getKeys());
		edoc.setUpdatedAt(new Date());
		
		EmbedderDocument res = embedderRepository.save(edoc);
		
		return res;
	}

	private String applyPreprocessToMappingDocument(EmbedderDocument edoc, Map<String, Object> params) throws Exception {
		
		String str = dataServicesService.readMappingDocument(edoc, params);
		
		str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());

		return str;

	}
	
	@Override
	@Async("mappingExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();

		EmbedderContainer ec = (EmbedderContainer)tdescr.getContainer();
		
		try {
			Date executeStart = new Date(System.currentTimeMillis());

			serviceUtils.clearExecution(ec);
			
			ec.save(iec -> {			
				MappingExecuteState ies = ((EmbedderContainer)iec).getEmbedderDocument().getExecuteState(fileSystemConfiguration.getId()); 
		
				ies.setExecuteState(MappingState.EXECUTING);
				ies.setExecuteStartedAt(executeStart);
				ies.setExecuteShards(0);
				ies.setCount(0);
				ies.clearMessages();
			});
		} catch (Exception ex) {
			throw new TaskFailureException(ex, new Date());
		}

		logger.info("Embedder " + ec.getPrimaryId() + " starting");
		
		em.sendMessage(new ExecuteNotificationObject(ec));
		
		try (FileSystemRDFOutputHandler outhandler = folderService.createEmbeddingsExecutionRDFOutputHandler(ec, shardSize)) {
			
			EmbedderDocument edoc = ec.embedderDocument;
	    	DatasetCatalog dcg = schemaService.asCatalog(edoc.getDatasetUuid());
	    	
	    	String fromClause = schemaService.buildFromClause(dcg);

	    	SPARQLStructure ss = sparqlService.toSPARQL(edoc.getElement());
//	    	System.out.println(ss.construct(fromClause, "{@@id@@}"));
//	    	System.out.println(ss.getWhereClause());
	    	
			Map<String, Object> params = new HashMap<>();

			TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
				
			params.put("iigraph", fromClause);
			params.put("iiclass", edoc.getOnClass());
			params.put("iiquery", ss.construct(fromClause, "{@@id@@}"));
			params.put("iirdfsource", vc.getSparqlEndpoint());
			params.put("iiembedder", resourceVocabulary.getEmbedderAsResource(edoc.getUuid()));
//
//			for (DataServiceParameterValue dsp : adoc.getParameters()) {
//				params.put(dsp.getName(), dsp.getValue());
//			}
//
			String str = applyPreprocessToMappingDocument(edoc, params);

//			System.out.println(">> " + str);

			Executor exec = new Executor(outhandler, safeExecute);
			
			folderService.checkCreateExtractTempFolder(ec.getCurrentUser());

			try {
//				tdescr.setMonitor(em);
				exec.setMonitor(em);

				D2RMLModel d2rml = D2RMLModel.readFromString(str);
				
				exec.configureFileExtraction(extractMinSize, folderService.getExtractTempFolder(ec.getCurrentUser()), d2rml.usesCaches() ? restCacheSize : 0);

				em.createStructure(d2rml);

				logger.info("Embedder started -- id: " + ec.getPrimaryId());
				
				em.sendMessage(new ExecuteNotificationObject(ec));
//
				exec.execute(d2rml, params);

				em.complete();

				ec.save(iec -> {
					MappingExecuteState ies = ((EmbedderContainer)iec).getEmbedderDocument().getExecuteState(fileSystemConfiguration.getId());
				
					ies.setExecuteState(MappingState.EXECUTED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(outhandler.getShards());
					ies.setCount(outhandler.getTotalItems());
//					ies.setCount(subjects.size());

					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getD2rmlExecution());
				});
				

				em.sendMessage(new ExecuteNotificationObject(ec));

				logger.info("Embeder executed -- id: " + ec.getPrimaryId() + ", shards: " + outhandler.getShards());

				if (outhandler.getTotalItems() > 0) {
					try {
						serviceUtils.zipExecution(ec, outhandler.getShards());
					} catch (Exception ex) {
						ex.printStackTrace();
						
						logger.info("Zipping embedder execution failed -- id: " + ec.getPrimaryId());
					}
				}
				
				return new AsyncResult<>(em.getCompletedAt());

			} catch (Exception ex) {
				logger.info("Embedder failed -- id: " + ec.getPrimaryId());
				
				em.currentConfigurationFailed(ex);

				throw ex;
			} finally {
				exec.finalizeFileExtraction();
				
				try {
					if (em != null) {
						em.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			em.complete(ex);

			try {
				ec.save(iec -> {
					MappingExecuteState ies = ((EmbedderContainer)iec).getEmbedderDocument().getExecuteState(fileSystemConfiguration.getId());
	
					ies.setExecuteState(MappingState.EXECUTION_FAILED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(0);
					ies.setCount(0);
					ies.setMessage(em.getFailureMessage());
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getD2rmlExecution());
				});
			} catch (Exception iex) {
				throw new TaskFailureException(iex, em.getCompletedAt());
			}
			
			em.sendMessage(new ExecuteNotificationObject(ec));
			
			throw new TaskFailureException(ex, em.getCompletedAt());
		}

	}
	
	public List<EmbedderDocumentResponse> getEmbedders(UserPrincipal currentUser, String datasetUri) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);
		
		List<EmbedderDocument> docs = embedderRepository.findByDatasetUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		Optional<Dataset> datasetOpt = datasetRepository.findByUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return new ArrayList<>();
		}
		
		Dataset dataset = datasetOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
		
		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;
		
		List<EmbedderDocumentResponse> response = docs.stream()
				.map(doc -> modelMapper.embedder2EmbedderResponse(vc, doc))
				.collect(Collectors.toList());

		return response;
	}
	
	public List<EmbedderDocumentResponse> getEmbedders(String datasetUri) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

		List<EmbedderDocument> docs = embedderRepository.findByDatasetUuid(datasetUuid);

		Optional<Dataset> datasetOpt = datasetRepository.findByUuid(datasetUuid);

		if (!datasetOpt.isPresent()) {
			return new ArrayList<>();
		}
		
		Dataset dataset = datasetOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
		
		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;
		
		List<EmbedderDocumentResponse> response = docs.stream()
				.map(doc -> modelMapper.embedder2EmbedderResponse(vc, doc))
				.collect(Collectors.toList());
		return response;
	}
	
	public EmbedderDocument failExecution(EmbedderContainer ec) {			
		EmbedderDocument edoc = ec.getEmbedderDocument();
		
		MappingExecuteState es = edoc.checkExecuteState(ec.getContainerFileSystemConfiguration().getId());
		if (es != null) {
			es.setExecuteState(MappingState.EXECUTION_FAILED);
			es.setExecuteCompletedAt(new Date());
			es.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
			embedderRepository.save(edoc);
		}
		
		return edoc;
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
	
}
