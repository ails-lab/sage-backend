package ac.software.semantic.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
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
import ac.software.semantic.controller.utils.FileUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DependencyBinding;
import ac.software.semantic.model.ExecuteDocument;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingExecutePublishDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.Template;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.MappingType;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.MappingResponse;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.repository.TemplateRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.util.SerializationTransformation;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.FileSystemPlainTextOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;

@Service
public class MappingsService implements ExecutingService {

	private Logger logger = LoggerFactory.getLogger(MappingsService.class);

    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private D2RMLService d2rmlService;
	
	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private MappingRepository mappingsRepository;

	@Autowired
	private TemplateRepository templateRepository;

	@Autowired
	private TemplateService templateService;

	@Autowired
	private DatasetRepository datasetRepository;

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

	@Override
	public Class<? extends ObjectContainer> getContainerClass() {
		return MappingContainer.class;
	}

	public class MappingContainer extends ObjectContainer implements ExecutableContainer, IntermediatePublishableContainer {
		private ObjectId mappingId;
		private ObjectId mappingInstanceId;
		
		private MappingDocument mappingDocument;
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
			
			this.mappingDocument = mappingDocument;
			this.mappingInstance = mappingInstance;
			
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

			this.dataset = dc.getDataset();

			load();
			
			loadDataset();
		}
		
		public MappingContainer(UserPrincipal currentUser, MappingDocument mc, MappingInstance mi) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.mappingId = mc.getId();
			this.mappingInstanceId = mc.hasParameters() ? mi.getId() : null;
			
			this.currentUser = currentUser;

			mappingDocument = mc;
			mappingInstance = mi;
			
			loadDataset();
		}
		
		@Override
		protected void load() {
			Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(mappingId, new ObjectId(currentUser.getId()));

			if (!mappingOpt.isPresent()) {
				return;
			}

			mappingDocument = mappingOpt.get();
			mappingInstance = findMappingInstance(mappingDocument, mappingInstanceId != null ? mappingInstanceId.toString() : null);
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(mappingDocument.getDatasetId(), new ObjectId(currentUser.getId()));

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			dataset = datasetOpt.get();			
		}
		
		@Override
		public void save(MongoUpdateInterface ui) throws Exception {
			synchronized (saveSyncString()) { 
				load();
			
				ui.update(this);
				
				mappingsRepository.save(mappingDocument);
			}
		}

		@Override
		public MappingExecutePublishDocument<MappingPublishState> getPublishDocument() {
			return getMappingInstance();
		}
		
		@Override
		public ExecuteDocument getExecuteDocument() {
			return getMappingInstance();
		}

		public MappingDocument getMappingDocument() {
			return mappingDocument;
		}

		public void setMappingDocument(MappingDocument mappingDocument) {
			this.mappingDocument = mappingDocument;
		}

		@Override
		public ObjectId getPrimaryId() {
			return getMappingId();
		}
		
		@Override
		public ObjectId getSecondaryId() {
			return getMappingInstanceId();
		}
		
		public ObjectId getMappingId() {
			return mappingId;
		}
		
//		@Override
//		public MappingExecuteState getExecuteState() {
//			return getExecuteDocument().getExecuteState(containerFileSystemConfiguration.getId());
//		}
//
//		@Override
//		public MappingExecuteState checkExecuteState() {
//			return mappingInstance.checkExecuteState(containerFileSystemConfiguration.getId());
//		}
//
////		@Override
//		public void deleteExecuteState() {
//			mappingInstance.deleteExecuteState(containerFileSystemConfiguration.getId());
//		}
		
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
			return MappingsService.this.clearExecution(this);
		}
		
		@Override
		public boolean clearExecution(MappingExecuteState es) {
			return MappingsService.this.clearExecution(this, es);
		}
		
		@Override
		public MappingResponse asResponse() {
			return modelMapper.mapping2MappingResponse(getDatasetTripleStoreVirtuosoConfiguration(), mappingDocument, currentUser);
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
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}	
		
		@Override
		public String syncString() {
			return ("SYNC:" + containerString() + ":" + mappingId + (mappingInstanceId != null ? ":" + mappingInstanceId : "")).intern();
		}
		
		@Override
		public String saveSyncString() {
			return ("SYNC:SAVE:" + containerString() + ":" + mappingId).intern();
		}

		@Override
		public boolean delete() throws Exception {
			throw new Exception("Not implemented");
		}

	}
	
	public static String syncString(String id, String instanceId) {
		return ("SYNC:" + MappingContainer.class.getName() + ":" + id + (instanceId != null ? instanceId : "")).intern();
	}

	@Override
	public String synchronizedString(String id) {
		// TODO Auto-generated method stub
		throw new Error("NOT IMPLEMENTED");
	}

	
	public List<MappingContainer> getMappingContainers(DatasetContainer dc) {
		List<MappingContainer> res = new ArrayList<>();
		
		for (MappingDocument mdoc : mappingsRepository.findByDatasetId(dc.getDatasetId())) {
			for (MappingInstance mi : mdoc.getInstances()) {
				res.add(new MappingContainer(dc.getCurrentUser(), mdoc, mi));
			}
		}
		
		return res;
	}
	
	public MappingDocument create(UserPrincipal currentUser, String datasetId, MappingType type, String name, List<String> params, String templateId) throws Exception  {
		return create(currentUser, datasetId, type, name, params, null, "", templateId);
	}
	
	public MappingDocument create(UserPrincipal currentUser, String datasetId, MappingType type, String name, List<String> params, String fileName, String fileContents) throws Exception {
		return create(currentUser, datasetId, type, name, params, fileName, fileContents, null);
	}
	
	public MappingDocument create(UserPrincipal currentUser, String datasetId, MappingType type, String name, List<String> params, String fileName, String fileContents, String templateId) throws Exception {
		String uuid = UUID.randomUUID().toString();
		MappingDocument map = new MappingDocument();
		map.setUserId(new ObjectId(currentUser.getId()));
		map.setDatasetId(new ObjectId(datasetId));
		map.setDatabaseId(database.getId());
		map.setType(type);
		map.setUuid(uuid);
		map.setName(name);
//		if (d2rml != null) {
//			map.setD2RML(d2rml.replaceAll("__X_SAGE_MAPPING_UUID__", uuid)); //should find a better way
//		}
		map.setParameters(params);
		map.setFileName(fileName);
		if (templateId == null) {
			map.setFileContents(fileContents);
		} else {
			Optional<Template> tempOpt = templateRepository.findById(new ObjectId(templateId));
			
			if (!tempOpt.isPresent()) {
				map.setFileContents("");
			} else {
				map.setFileContents(templateService.getEffectiveTemplateString(tempOpt.get()));
			}
		}
		
		map.setUpdatedAt(new Date());
		
		map = mappingsRepository.save(map);

		return map;
		
	}

	public MappingDocument create(UserPrincipal currentUser, String datasetId, MappingType type, String name, List<String> params, String fileName, String fileContents, String uuid, Template template) {
		MappingDocument map = new MappingDocument();
		map.setUserId(new ObjectId(currentUser.getId()));
		map.setDatasetId(new ObjectId(datasetId));
		map.setDatabaseId(database.getId());
		map.setType(type);
		map.setUuid(uuid);
		map.setName(name);
//		if (d2rml != null) {
//			map.setD2RML(d2rml.replaceAll("__X_SAGE_MAPPING_UUID__", uuid)); //should find a better way
//		} else {
//			map.setD2RML(null); //shouldn't be the case
//		}
		map.setParameters(params);
		map.setFileName(fileName);
		map.setFileContents(fileContents);
		map.setUpdatedAt(new Date());
		map.setTemplateId(template.getId());
		

		map = mappingsRepository.save(map);

		return map;
	}

	public boolean updateMapping(UserPrincipal currentUser, String id, String name, List<String> parameters, String fileName, String fileContents) {

		Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!entry.isPresent()) {
			return false;
		}
		
		MappingDocument doc = entry.get();
		if (name != null) {
			doc.setName(name);
		}
	
//			doc.setD2RML(null); // remove legacy d2rml content
	
//			if (d2rml != null) {
//				doc.setD2RML(d2rml.replaceAll("__X_SAGE_MAPPING_UUID__", doc.getUuid())); //should find a better way
		doc.setUpdatedAt(new Date());
//			}
		
		if (fileName != null) {
			doc.setFileName(fileName);
			doc.setFileContents(fileContents);
		}
		
		doc.setParameters(parameters);

		mappingsRepository.save(doc);
		
		return true;
	}

	public MappingInstance createParameterBinding(MappingContainer mc, List<ParameterBinding> bindings) {

		MappingDocument doc = mc.getMappingDocument();
		MappingInstance mi = doc.addInstance(bindings);

		mappingsRepository.save(doc);

		return mi;
	}
	
	public MappingInstance updateParameterBinding(MappingContainer mc, List<ParameterBinding> bindings) {

		MappingInstance mi = mc.getMappingInstance();
		mi.setBinding(bindings);

		mappingsRepository.save(mc.getMappingDocument());

		return mi;
	}
	
	public boolean deleteParameterBinding(MappingContainer mc) throws IOException {

		MappingDocument doc = mc.getMappingDocument();

		clearExecution(mc);
		
		List<MappingInstance> list = doc.getInstances();
		for (int k = 0; k < list.size(); k++) {
			MappingInstance mi = list.get(k);
			if (mi.getId().equals(mc.getMappingInstanceId())) {
				list.remove(k);
				break;
			}
		}

		mappingsRepository.save(doc);

		return true;
	}

	
	public List<MappingResponse> getMappings(UserPrincipal currentUser, String datasetId) {

		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return new ArrayList<>();
		}
		
		Dataset dataset = datasetOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());

		List<MappingDocument> docs = mappingsRepository.findByDatasetIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));

		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;
		
		List<MappingResponse> response = docs.stream().map(doc -> modelMapper.mapping2MappingResponse(vc, doc, currentUser))
				.collect(Collectors.toList());

		return response;
	}

	public List<MappingDocument> getHeaderMappings(UserPrincipal currentUser, ObjectId datasetId) {
		return mappingsRepository.findByUserIdAndDatasetIdAndType(new ObjectId(currentUser.getId()), datasetId, "HEADER");
	}

	// TODO should delete also relevant uploaded files!
	public boolean deleteMapping(UserPrincipal currentUser, String mappingId) throws IOException {

		Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(mappingId), new ObjectId(currentUser.getId()));

		if (!entry.isPresent()) {
			return false;
		}
		
		MappingDocument doc = entry.get();

		// Clear execution of all mapping instances of the mapping
		List<MappingInstance> instanceList = doc.getInstances();
		boolean hasParameters = !doc.getParameters().isEmpty();
		if (hasParameters) {
			for (MappingInstance instance : instanceList) {
				clearExecution(currentUser, mappingId, instance.getId().toString());
			}
		}
		else {
			clearExecution(currentUser, mappingId, null);
		}
		
		if (doc.getDataFiles() != null) {
			for (String s : doc.getDataFiles()) {
				folderService.deleteAttachment(currentUser, doc, s);
			}
		}
		
		// Now delete the mapping
		mappingsRepository.delete(doc);

		return true;
	}


//	public Optional<MappingResponse> getMapping(UserPrincipal currentUser, String mappingId) {
//
//		Optional<MappingDocument> dopt = mappingsRepository.findByIdAndUserId(new ObjectId(mappingId), new ObjectId(currentUser.getId()));
//
//		if (!dopt.isPresent()) {
//			return Optional.empty();
//		}
//		
//		MappingDocument doc = dopt.get();
//		
//		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));
//
//		if (!datasetOpt.isPresent()) {
//			return Optional.empty();
//		}
//		
//		Dataset dataset = datasetOpt.get();
//		
//		PublishStateVirtuoso psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
//
//		final TripleStoreConfiguration vc = psv != null ? psv.getVirtuosoConfiguration() : null;
//
//		MappingResponse mr = modelMapper.mapping2MappingResponse(vc, doc, currentUser);
//
//		// for compatibility <--
////		for (int i = 0; i < mr.getInstances().size(); i++) {
////			MappingInstanceResponse mir = mr.getInstances().get(i);
////			if (mir.isLegacy()) {
////				File f = folderService.getMappingExecutionTrigFile(currentUser, dataset, doc, doc.getInstances().get(i), doc.getInstances().get(i).checkExecuteState(fileSystemConfiguration.getId()), 0);
////				if (f != null) {
////					mir.setPublishedFromCurrentFileSystem(true);
////				}
////			}
////		}
//		// for compatibility -->
//		
//		return Optional.of(mr);
//	}

	
//	public Optional<String> previewLastExecution(UserPrincipal currentUser, String id, String instanceId) throws IOException {
//		
//		MappingContainer mc = new MappingContainer(currentUser, id, instanceId);
//
//		if (mc.mappingDocument == null) {
//			return Optional.empty();
//		}
//		
//		ExecuteState es = mc.mappingInstance.getExecuteState(fileSystemConfiguration.getId());
//
//		if (es.getExecuteState() != MappingState.EXECUTED) {
//			return Optional.empty();
//		}
//
//		File file = folderService.getMappingExecutionTrigFile(currentUser, mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), es, 0);
//		
//		if (file != null) {
//			String res = FileUtils.readFileBeginning(Paths.get(file.getAbsolutePath()));
//			return Optional.of(res);
//		} else {
//			logger.error("Execution file for mapping " + mc.mappingDocument.getUuid() + (instanceId == null ? "" : "/" + instanceId) + " not found.");
//			return Optional.of("Execution file for mapping '" + mc.mappingDocument.getName() + (instanceId == null ? "" : "/" + instanceId) + "' not found.");
//		}
//	}
	
	
//	public Optional<String> previewPublishedExecution(UserPrincipal currentUser, String id, String instanceId) throws IOException {
//		MappingContainer mc = new MappingContainer(currentUser, id, instanceId);
//
//		if (mc.mappingDocument == null) {
//			return Optional.empty();
//		}
//		
//		ProcessStateContainer psv = mc.mappingInstance.getCurrentPublishState(virtuosoConfigurations.values());
//		if (psv == null) {
//			return Optional.empty();			
//		}
//		
//		MappingPublishState ps = (MappingPublishState)psv.getProcessState();
//		ExecuteState pes = ps.getExecute();
//		
//		if (!pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
//			return Optional.empty();
//		}
//		
//		if (pes.getExecuteState() != MappingState.EXECUTED) {
//			return Optional.empty();
//		}
//
//		File file = folderService.getMappingExecutionTrigFile(currentUser, mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), pes, 0);
//		
//		if (file != null) {
////	     	String res = new String(Files.readAllBytes(path));
//			String res = FileUtils.readFileBeginning(Paths.get(file.getAbsolutePath()));
//			return Optional.of(res);
//		} else {
//			logger.error("Execution file for mapping " + mc.mappingDocument.getUuid() + (instanceId == null ? "" : "/" + instanceId) + " not found.");
//			return Optional.of("Execution file for mapping '" + mc.mappingDocument.getName() + (instanceId == null ? "" : "/" + instanceId) + "' not found.");
//		}
//	}	

//	public Optional<String> downloadLastExecution(UserPrincipal currentUser, String id, String instanceId) throws IOException {
//		MappingContainer mc = new MappingContainer(currentUser, id, instanceId);
//
//		if (mc.mappingDocument == null) {
//			return Optional.empty();
//		}
//		
//		MappingExecuteState es = mc.mappingInstance.getExecuteState(fileSystemConfiguration.getId());
//
//		if (es.getExecuteState() != MappingState.EXECUTED) {
//			return Optional.empty();
//		}
//		
//		File file = folderService.getMappingExecutionZipFile(currentUser, mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), es);
//
//		// for compatibility
//		if (file == null) {
//			file = zipExecution(currentUser, mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), es, es.getExecuteShards() == 0 ? 1 : es.getExecuteShards(), es.getSparqlExecuteShards());
//		}			
//		
//		return Optional.of(file.getAbsolutePath());
//	}
	
//	public Optional<String> downloadPublishedExecution(UserPrincipal currentUser, String id, String instanceId) throws IOException {
//		MappingContainer mc = new MappingContainer(currentUser, id, instanceId);
//
//		if (mc.mappingDocument == null) {
//			return Optional.empty();
//		}
//
//		ProcessStateContainer psv = mc.mappingInstance.getCurrentPublishState(virtuosoConfigurations.values());
//		if (psv == null) {
//			return Optional.empty();			
//		}
//		
//		MappingPublishState ps = (MappingPublishState)psv.getProcessState();
//		MappingExecuteState pes = ps.getExecute();
//		
//		if (!pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
//			return Optional.empty();
//		}
//		
//		if (pes.getExecuteState() != MappingState.EXECUTED) {
//			return Optional.empty();
//		}
//		
//		File file = folderService.getMappingExecutionZipFile(currentUser, mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), pes);
//
//		// for compatibility
//		if (file == null) {
//			file = zipExecution(currentUser, mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), pes, pes.getExecuteShards() == 0 ? 1 : pes.getExecuteShards(), pes.getSparqlExecuteShards());
//		}			
//		
//		return Optional.of(file.getAbsolutePath());
//	}
	
	public MappingInstance findMappingInstance(MappingDocument doc, String instanceId) {
		MappingInstance mi = null;
		if (instanceId == null && doc.getInstances().size() == 0) {
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
	
	public boolean clearExecution(UserPrincipal currentUser, String id, String instanceId) {
		return clearExecution(getContainer(currentUser, id, instanceId));
	}
	
	public boolean clearExecution(MappingContainer mc) {
		MappingExecuteState es = mc.mappingInstance.checkExecuteState(fileSystemConfiguration.getId());
		
		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
			return false;
		}
		
		return clearExecution(mc.getCurrentUser(), mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), es);
	}
	
	private boolean clearExecution(MappingContainer mc, MappingExecuteState es) {
		return clearExecution(mc.getCurrentUser(), mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), es);
	}
	
	public boolean clearExecution(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, MappingExecuteState es) {
		
		ProcessStateContainer psv = mi.getCurrentPublishState(virtuosoConfigurations.values());
		
		if (psv != null) {
			MappingPublishState ps = (MappingPublishState)psv.getProcessState();
			MappingExecuteState pes = ps.getExecute();
	
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
				}
			}
		} 
		
		mappingsRepository.save(doc);

		return true;
	}	


	//very experimental and problematic.
	public boolean unpublish(UserPrincipal currentUser, String id, String instanceId) throws Exception {

		Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

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
		PublishState mappingPs = mi.checkPublishState(psv.getTripleStoreConfiguration().getId());
		
		if (mappingPs == null) {
			return false;
		}
		
		mappingPs.setPublishState(DatasetState.UNPUBLISHING);
		mappingsRepository.save(mapping);
		
		tripleStore.unpublishMapping(currentUser, psv.getTripleStoreConfiguration(), dataset, mapping, mi);
		
		mappingPs.setPublishState(DatasetState.UNPUBLISHED);
		mappingsRepository.save(mapping);
			
		return true;
	}	
	
	public MappingContainer getContainer(DatasetContainer dc, String mappingId) {
		return getContainer(dc, mappingId, null);
	}
		

	public MappingContainer getContainer(DatasetContainer dc, String mappingId, String mappingInstanceId) {
		MappingContainer mc = new MappingContainer(dc, mappingId, mappingInstanceId);
		if (mappingInstanceId == null) {
			if (mc.mappingDocument == null || mc.getDataset() == null) {
				return null;
			} else {
				return mc;
			}
		} else {
			if (mc.mappingDocument == null || mc.getDataset() == null || mc.getMappingInstance() == null) {
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
	
	public MappingContainer getContainer(UserPrincipal currentUser, ObjectId mappingId, ObjectId mappingInstanceId) {
		return getContainer(currentUser, mappingId.toString(), mappingInstanceId != null ? mappingInstanceId.toString() : null);
	}
	
	public MappingContainer getContainer(UserPrincipal currentUser, String mappingId, String mappingInstanceId) {
		MappingContainer mc = new MappingContainer(currentUser, mappingId, mappingInstanceId);
//		System.out.println(">> " + mc.mappingDocument + " " + mc.getDataset());
		if (mappingInstanceId == null) {
			if (mc.mappingDocument == null || mc.getDataset() == null) {
				return null;
			} else {
				return mc;
			}
		} else {
			if (mc.mappingDocument == null || mc.getDataset() == null || mc.getMappingInstance() == null) {
				return null;
			} else {
				return mc;
			}
		}
	}

	public MappingContainer getContainer(UserPrincipal currentUser, Dataset dataset, MappingDocument mappingDocument, MappingInstance mappingInstance) {
		MappingContainer mc = new MappingContainer(currentUser, dataset, mappingDocument, mappingInstance);
		if (mc.mappingDocument == null || mc.getDataset() == null) {
			return null;
		} else {
			return mc;
		}		
	}

	@Override
	@Async("mappingExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();

		MappingContainer mc = (MappingContainer)tdescr.getContainer();
		
		mc.load(); // to keep updated data

		Dataset dataset = mc.getDataset();
		UserPrincipal currentUser = mc.getCurrentUser();

		MappingDocument doc = mc.getMappingDocument();
		MappingInstance mi;
		
		// should be moved within try: Message is not send to monitor !!!!
		for (DependencyBinding db : doc.getDependencies()) {
			for(ObjectId dependecyId : db.getValue()) {
				Optional<MappingDocument> dmappingOpt = mappingsRepository.findByIdAndUserId(dependecyId, new ObjectId(currentUser.getId()));
				if (!dmappingOpt.isPresent() || !dmappingOpt.get().isExecuted(fileSystemConfiguration.getId())) {
//					return new AsyncResult<>(false);
					throw new TaskFailureException(new Exception("Mapping dependecies are not executed"), new Date());
				}	
			}
		}
		
		// Clearing old files. Should be done before updating execute start date
		clearExecution(mc);
		
		//should save before staring execution!
		try {
			mc.save(imc -> {
				MappingExecuteState ies = ((MappingContainer)imc).getMappingInstance().getExecuteState(fileSystemConfiguration.getId());
				ies.startDo(em); // should be here so as to set start date
			});
		} catch (Exception ex) {
			throw new TaskFailureException(ex, new Date());
		}

		doc = mc.getMappingDocument();
		mi = mc.getMappingInstance();
		
		try (FileSystemRDFOutputHandler rdfOuthandler = folderService.createMappingExecutionRDFOutputHandler(mc, shardSize);
				FileSystemPlainTextOutputHandler txtOuthandler = folderService.createMappingExecutionPlainTextOutputHandler(mc)) {

			em.sendMessage(new ExecuteNotificationObject(mc));

			Map<String, Object> fileMap = new HashMap<>();

			if (doc.getDataFiles() != null) {
				for (String s : doc.getDataFiles()) {
					fileMap.put(s, folderService.getAttachmentPath(currentUser, doc, s));
				}
			}
			if (mi.getDataFiles() != null) {
				for (String s : mi.getDataFiles()) {
					fileMap.put(s, folderService.getAttachmentPath(currentUser, doc, mi, s));
				}
			}
			
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
			for (DependencyBinding db : doc.getDependencies()) {
				org.apache.jena.query.Dataset paramDataset = DatasetFactory.create();
				logger.info("Creating tmp dataset " + db.getName());

				// has been checked above
//				for (ObjectId dependecyId : db.getValue()) {
//					Optional<MappingDocument> dmappingOpt = mappingsRepository.findByIdAndUserId(dependecyId, new ObjectId(currentUser.getId()));
//					if (!dmappingOpt.isPresent() || !dmappingOpt.get().isExecuted(fileSystemConfiguration.getId())) {
//						return new AsyncResult<>(false);
//					}	
//				}
				
				executionResultsToModel(paramDataset, currentUser, db.getValue());
				
				logger.info("Created tmp dataset " + db.getName());
				
				execParams.put(db.getName(), paramDataset);
			}
			
			Executor exec = new Executor(rdfOuthandler, txtOuthandler, safeExecute);
			
			folderService.checkCreateExtractTempFolder(currentUser);
			
			try {
				exec.setMonitor(em);

				String baseUri = resourceVocabulary.getMappingAsResource(doc.getUuid()).toString(); 

				D2RMLModel d2rml;
				if (doc.getFileContents() == null)  {
					d2rml = new SerializationTransformation().XtoD2RMLModel(doc.getD2RML(), baseUri, params); // legacy for old json d2rml;
				} else {
					d2rml = d2rmlService.prepare(doc, mc.getDataset(), baseUri, params);
				}
				
				exec.configureFileExtraction(extractMinSize, folderService.getExtractTempFolder(currentUser), d2rml.usesCaches() ? restCacheSize : 0);

				em.createStructure(d2rml);
				
				logger.info("Mapping started -- id: " + mc.idsToString());

				em.sendMessage(new ExecuteNotificationObject(mc));

				exec.execute(d2rml, execParams);
				
				em.complete();
				
				mc.save(imc -> {
					MappingExecuteState ies = ((MappingContainer)imc).getMappingInstance().getExecuteState(fileSystemConfiguration.getId());

					ies.completeDo(em);
					ies.setExecuteShards(rdfOuthandler.getShards());
					ies.setSparqlExecuteShards(txtOuthandler.getShards());
					ies.setCount(rdfOuthandler.getTotalItems());
					ies.setSparqlCount(txtOuthandler.getTotalItems());
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getD2rmlExecution());
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
				mc.save(imc -> {
					MappingExecuteState ies = ((MappingContainer)imc).getMappingInstance().getExecuteState(fileSystemConfiguration.getId());
	
					ies.failDo(em);
					ies.setExecuteShards(0);
					ies.setSparqlExecuteShards(0);
					ies.setCount(0);
					ies.setSparqlCount(0);
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getD2rmlExecution());
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
		return zipExecution(mc.getCurrentUser(), mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), mc.getExecuteState(), trigShards, txtShards);
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
	
	
	public void executionResultsToModel(org.apache.jena.query.Dataset ds, UserPrincipal currentUser, List<ObjectId> ids) throws IOException {
		if (ds == null) {
			return;
		}
		
		for (ObjectId id : ids) {
			Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId()));

			if (!mappingOpt.isPresent()) {
				continue;
			}

			MappingDocument doc = mappingOpt.get();

			Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));

			if (!datasetOpt.isPresent()) {
				continue;
			}
			
			Dataset dataset = datasetOpt.get();
			
			for (MappingInstance mi : doc.getInstances()) {
				MappingExecuteState  es = mi.getExecuteState(fileSystemConfiguration.getId());
	
				if (es.getExecuteState() == MappingState.EXECUTED) {
					if (es.getExecuteShards() != null) {
				        for (int i = 0; i < es.getExecuteShards(); i++) {
				        	File file = folderService.getMappingExecutionTrigFile(currentUser, dataset, doc, mi, es, i);
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
	
	public MappingDocument failExecution(MappingContainer mc) {			
		MappingDocument mdoc = mc.getMappingDocument();
		MappingInstance mi = mc.getMappingInstance();
		
		MappingExecuteState es = mi.checkExecuteState(mc.getContainerFileSystemConfiguration().getId());
		if (es != null) {
			es.setExecuteState(MappingState.EXECUTION_FAILED);
			es.setExecuteCompletedAt(new Date());
			es.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
			mappingsRepository.save(mdoc);
		}
		
		return mdoc;
	}


//	public void failExecution(String id, String instanceId, Throwable ex, Date date) {
//		MappingDocument doc = mappingsRepository.findById(id).get();
//		MappingInstance mi = this.findMappingInstance(doc, instanceId);
//		
//		MappingExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());
//		
//		if (es != null) {
//			es.setExecuteState(MappingState.EXECUTION_FAILED);
//			es.setExecuteCompletedAt(date);
//			if (ex != null) {
//				es.setMessage(new NotificationMessage(MessageType.ERROR, ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage()));
//			}
//	
//			mappingsRepository.save(doc);
//		}
//	}
}
