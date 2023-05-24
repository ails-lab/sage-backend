package ac.software.semantic.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import ac.software.semantic.payload.UpdateAnnotatorRequest;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueResponseContainer;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.AnnotatorPrepareStatus;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.constants.ThesaurusLoadStatus;
import ac.software.semantic.model.state.AnnotatorPublishState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.ExecuteDocument;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingExecutePublishDocument;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.PreprocessInstruction;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.AnnotatorDocumentResponse;
import ac.software.semantic.payload.PublishNotificationObject;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.IdentifiersService.GraphLocation;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.d2rml.monitor.SimpleMonitor;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.RDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.StringOutputHandler;
import edu.ntua.isci.ac.d2rml.output.StringRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class AnnotatorService implements ExecutingPublishingService {

	private Logger logger = LoggerFactory.getLogger(AnnotatorService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	private SchemaService schemaService;

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;

	@Value("${d2rml.execute.request-cache-size}")
	private int restCacheSize;

	@Value("${d2rml.extract.min-size:0}")
	private long extractMinSize; 

	@Value("${annotation.execution.folder}")
	private String annotationsFolder;

	@Value("${mapping.temp.folder}")
	private String tempFolder;

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
	private FolderService folderService;
	
	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private AnnotationEditRepository annotationEditRepository;

	@Autowired
	private AnnotationEditGroupRepository annotationEditGroupRepository;

	@Autowired
	private DataServicesService dataServicesService;

	@Autowired
	private DataServiceRepository dataServicesRepository;

	@Autowired
	private TripleStore tripleStore;
	
	@Autowired
	private IdentifiersService idService;
	
    @Autowired
    @Qualifier("preprocess-functions")
    private Map<Resource, List<String>> functions;
    
    @Autowired
    @Qualifier("preprocess-operations")
    private Map<Resource, List<String>> operations;
        
	@Autowired
	private ResourceLoader resourceLoader;

	public Class<? extends ObjectContainer> getContainerClass() {
		return AnnotatorContainer.class;
	}
	
	public class AnnotatorContainer extends ObjectContainer implements ExecutableContainer, PublishableContainer {
		private ObjectId annotatorId;
		
		private AnnotatorDocument annotatorDocument;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		private AnnotatorContainer(UserPrincipal currentUser, ObjectId annotatorId) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.annotatorId = annotatorId;
			this.currentUser = currentUser;
			
			load();
		}

		@Override
		protected void load() {
			Optional<AnnotatorDocument> annotatorOpt = annotatorRepository.findByIdAndUserId(annotatorId, new ObjectId(currentUser.getId()));

			if (!annotatorOpt.isPresent()) {
				return;
			}

			annotatorDocument = annotatorOpt.get();
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(annotatorDocument.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			dataset = datasetOpt.get();
		}

		@Override
		public ExecuteDocument getExecuteDocument() {
			return getAnnotatorDocument();
		}
		
		@Override
		public MappingExecutePublishDocument<AnnotatorPublishState> getPublishDocument() {
			return getAnnotatorDocument();
		}
		
		public AnnotatorDocument getAnnotatorDocument() {
			return annotatorDocument;
		}

		public void setAnnotatorDocument(AnnotatorDocument annotatorDocument) {
			this.annotatorDocument = annotatorDocument;
		}
		
		public DataService getDataService() {
			return dataServicesRepository.findByIdentifierAndType(annotatorDocument.getAnnotator(), DataServiceType.ANNOTATOR).orElse(null);
		}

		@Override
		public void save(MongoUpdateInterface mui) throws Exception {
			synchronized (saveSyncString()) { 
				load();
			
				mui.update(this);
				
				annotatorRepository.save(annotatorDocument);
			}
		}
		
		public boolean delete() {
			
			synchronized (saveSyncString()) {
				clearExecution();
					
				annotatorRepository.delete(annotatorDocument);

				if (annotatorRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(annotatorDocument.getDatasetUuid(), annotatorDocument.getOnProperty().toArray(new String[] {}), annotatorDocument.getAsProperty(), new ObjectId(currentUser.getId())).isEmpty()) {
					annotationEditRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(annotatorDocument.getDatasetUuid(), annotatorDocument.getOnProperty(), annotatorDocument.getAsProperty(), new ObjectId(currentUser.getId()));
					annotationEditGroupRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(annotatorDocument.getDatasetUuid(), annotatorDocument.getOnProperty().toArray(new String[] {}), annotatorDocument.getAsProperty(), new ObjectId(currentUser.getId()));
				}
	
				return true;
			}
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return getAnnotatorId();
		}
		
		public ObjectId getAnnotatorId() {
			return annotatorId;
		}

		public void setAnnotatorId(ObjectId annotatorId) {
			this.annotatorId = annotatorId;
		}

		@Override
		public String localSynchronizationString() {
			return getContainerFileSystemConfiguration().getId().toString() + ":" + getAnnotatorId().toString();
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
		public boolean clearExecution() {
			return AnnotatorService.this.clearExecution(this);
		}

		@Override
		public boolean clearExecution(MappingExecuteState es) {
			return AnnotatorService.this.clearExecution(this, es);
		}
		
		@Override
		public AnnotatorDocumentResponse asResponse() {
			TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();
			AnnotationEditGroup aeg = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(annotatorDocument.getDatasetUuid(), annotatorDocument.getOnProperty(), annotatorDocument.getAsProperty(), new ObjectId(currentUser.getId())).get();
		
			return modelMapper.annotator2AnnotatorResponse(vc, annotatorDocument, aeg);
		}
		
		@Override
		public TaskType getExecuteTask() {
			return TaskType.ANNOTATOR_EXECUTE;
		}

		@Override
		public TaskType getClearLastExecutionTask() {
			return TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION;
		}

		@Override
		public TaskType getPublishTask() {
			return TaskType.ANNOTATOR_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return TaskType.ANNOTATOR_UNPUBLISH;
		}

		@Override
		public TaskType getRepublishTask() {
			return TaskType.ANNOTATOR_REPUBLISH;
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}		

		@Override
		public String syncString() {
			return ("SYNC:" + containerString() + ":" + annotatorId).intern();
		}

		@Override
		public String saveSyncString() {
			return ("SYNC:SAVE:" + containerString() + ":" + annotatorId).intern();
		}		
	}
	
	@Override
	public String synchronizedString(String id) {
		return syncString(id);
	}
	
	public static String syncString(String id) {
		return ("SYNC:" + AnnotatorContainer.class.getName() + ":" + id).intern();
	}
	
	public AnnotatorDocument createAnnotator(UserPrincipal currentUser, String datasetUri, List<PathElement> onPath,
			String asProperty, String annotator, String thesaurus, List<DataServiceParameterValue> parameters,
			List<PreprocessInstruction> preprocess, String variant, String defaultTarget) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

		Optional<Dataset> ds = datasetRepository.findByUuid(datasetUuid);
		if (ds.isPresent()) {

			List<String> flatOnPath = PathElement.onPathElementListAsStringList(onPath);
			
			String uuid = UUID.randomUUID().toString();

			AnnotatorDocument adoc = new AnnotatorDocument();
			adoc.setUserId(new ObjectId(currentUser.getId()));
			adoc.setDatasetUuid(datasetUuid);
			adoc.setUuid(uuid);
			adoc.setDatabaseId(database.getId());
			adoc.setOnProperty(flatOnPath);
			adoc.setAnnotator(annotator);
			adoc.setVariant(variant);
			adoc.setAsProperty(asProperty);
			adoc.setParameters(parameters);
			adoc.setThesaurus(thesaurus);
			adoc.setPreprocess(preprocess);
			adoc.setDefaultTarget(defaultTarget);
			adoc.setUpdatedAt(new Date());

			AnnotationEditGroup aeg;
			Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(datasetUuid, flatOnPath, asProperty, new ObjectId(currentUser.getId()));
			if (!aegOpt.isPresent()) {
//				aeg = new AnnotationEditGroup(datasetUuid, onProperty, asProperty, new ObjectId(currentUser.getId()));
				aeg = new AnnotationEditGroup();
				aeg.setDatasetUuid(datasetUuid);
				aeg.setOnProperty(flatOnPath);
				aeg.setAsProperty(asProperty);
				aeg.setUserId(new ObjectId(currentUser.getId()));
				aeg.setUuid(UUID.randomUUID().toString());
				annotationEditGroupRepository.save(aeg);
			} else {
				aeg = aegOpt.get();
			}
			adoc.setAnnotatorEditGroupId(aeg.getId());

			AnnotatorDocument doc = annotatorRepository.save(adoc);

			return doc;
		} else {
			return null;
		}
	}

	//	@Autowired
//	private Environment env;
//
//	private static String currentTime() {
//		SimpleDateFormat srcFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//		return srcFormatter.format(new Date());
//	}
	public AnnotatorDocument updateAnnotator(UserPrincipal currentUser, String annotatorId, UpdateAnnotatorRequest updateAnnotatorRequest) {
		Optional<AnnotatorDocument> annotatorOpt = annotatorRepository.findByIdAndUserId(new ObjectId(annotatorId), new ObjectId(currentUser.getId()));
		
		if (!annotatorOpt.isPresent()) {
			return null;
		}

		AnnotatorDocument adoc = annotatorOpt.get();
		adoc.setAnnotator(updateAnnotatorRequest.getAnnotator());
		adoc.setVariant(updateAnnotatorRequest.getVariant());
		adoc.setAsProperty(updateAnnotatorRequest.getAsProperty());
		adoc.setParameters(updateAnnotatorRequest.getParameters());
		adoc.setThesaurus(updateAnnotatorRequest.getThesaurus());
		adoc.setPreprocess(updateAnnotatorRequest.getPreprocess());
		adoc.setDefaultTarget(updateAnnotatorRequest.getDefaultTarget());
		adoc.setUpdatedAt(new Date());
		
		AnnotationEditGroup aeg;
		Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
		if (!aegOpt.isPresent()) {
//			aeg = new AnnotationEditGroup(datasetUuid, onProperty, asProperty, new ObjectId(currentUser.getId()));
			aeg = new AnnotationEditGroup();
			aeg.setDatasetUuid(adoc.getDatasetUuid());
			aeg.setOnProperty(adoc.getOnProperty());
			aeg.setAsProperty(adoc.getAsProperty());
			aeg.setUserId(new ObjectId(currentUser.getId()));
			aeg.setUuid(UUID.randomUUID().toString());
			annotationEditGroupRepository.save(aeg);
		} else {
			aeg = aegOpt.get();
		}
		adoc.setAnnotatorEditGroupId(aeg.getId());
		
//		MappingExecuteState es = annotator.getExecuteState(fileSystemConfiguration.getId());

//	**	// Clearing old files 
//		if (es.getExecuteState() == MappingState.EXECUTED) {
//			if (es.getExecuteShards() != null) {
//	
//				for (int i = 0; i < es.getExecuteShards(); i++) {
//					(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + annotator.getUuid()
//							+ (i == 0 ? "" : "_#" + i) + ".trig")).delete();
//				}
//				new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + annotator.getUuid()
//						+ "_catalog.trig").delete();
//				new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + annotator.getUuid() + ".zip").delete();
//			}
//		}
//		annotator.setExecute(null);
		
		AnnotatorDocument res = annotatorRepository.save(adoc);
		return res;
	}

//	public Optional<String> previewLastExecution(AnnotatorContainer ac) throws IOException {
//		
//		ExecuteState es = ac.annotatorDocument.getExecuteState(fileSystemConfiguration.getId());
//
//		if (es.getExecuteState() != MappingState.EXECUTED) {
//			return Optional.empty();
//		}
//
//		File file = folderService.getAnnotatorExecutionTrigFile(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), es, 0);
//		
//		if (file != null) {
////	     	String res = new String(Files.readAllBytes(path));
//			String res = AsyncUtils.readFileBeginning(Paths.get(file.getAbsolutePath()));
//			return Optional.of(res);
//		} else {
//			logger.error("Execution file for annotator " + ac.annotatorDocument.getUuid() + " not found.");
//			return Optional.of("Execution file for annotator '" + ac.annotatorDocument.getUuid() + "' not found.");
//		}
//	}
//	
//	public Optional<String> previewPublishedExecution(AnnotatorContainer ac) throws IOException {
//		
//		ProcessStateContainer psv = ac.getAnnotatorDocument().getCurrentPublishState(virtuosoConfigurations.values());
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
//		File file = folderService.getAnnotatorExecutionTrigFile(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), pes, 0);
//		
//		if (file != null) {
////	     	String res = new String(Files.readAllBytes(path));
//			String res = AsyncUtils.readFileBeginning(Paths.get(file.getAbsolutePath()));
//			return Optional.of(res);
//		} else {
//			logger.error("Execution file for mapping " + ac.annotatorDocument.getUuid()  + " not found.");
//			return Optional.of("Execution file for mapping '" + ac.annotatorDocument  + "' not found.");
//		}
//	}	
	
	
//	public Optional<String> downloadLastExecution(AnnotatorContainer ac) throws IOException {
//
//		MappingExecuteState  es = ac.getAnnotatorDocument().getExecuteState(fileSystemConfiguration.getId());
//			
//		File file = folderService.getAnnotatorExecutionZipFile(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), es);
//		
//		// for compatibility
//		if (file == null) {
//			file = zipExecution(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), es, es.getExecuteShards() == 0 ? 1 : es.getExecuteShards());
//		}	
//			
//		return Optional.of(file.getAbsolutePath());
//
//	}
	
//	public Optional<String> downloadPublishedExecution(AnnotatorContainer ac) throws IOException {
//		
//		ProcessStateContainer psv = ac.annotatorDocument.getCurrentPublishState(virtuosoConfigurations.values());
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
//		File file = folderService.getAnnotatorExecutionZipFile(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), pes);
//
//		// for compatibility
//		if (file == null) {
//			file = zipExecution(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), pes, pes.getExecuteShards() == 0 ? 1 : pes.getExecuteShards());
//		}			
//		
//		return Optional.of(file.getAbsolutePath());
//	}


	
	private String applyPreprocessToMappingDocument(AnnotatorDocument adoc, Map<String, Object> params) throws Exception {
		
		String str = dataServicesService.readMappingDocument(adoc, params);
		
		str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
		
		StringBuffer preprocess = new StringBuffer();
		List<PreprocessInstruction> pis = adoc.getPreprocess();
		if (pis != null && pis.size() > 0) {

			Model model = ModelFactory.createDefaultModel();
					
			for (int i = 0; i < pis.size(); i++) {
				PreprocessInstruction pi = pis.get(i);
				
				preprocess.append("dr:definedColumn \n");
				
				if (functions.keySet().contains(model.createResource(pi.getFunction()))) {
					preprocess.append("[ dr:name \"PREPROCESS-LEXICALVALUE-" + (i + 1) + "\" ; \n");
					preprocess.append("  dr:function <" + pi.getFunction() + "> ; \n");
					preprocess.append("dr:parameterBinding [ \n");
					preprocess.append("   dr:parameter \"input\" ; \n");
					if (i == 0) {
						preprocess.append("   rr:column \"lexicalValue\" ; \n");
					} else {
						preprocess.append("   rr:column \"PREPROCESS-LEXICALVALUE-" + i + "\" ; \n");
					}
					preprocess.append("] ; \n");
					for (DataServiceParameterValue pv : pi.getParameters()) {
						preprocess.append("dr:parameterBinding [ \n");
						preprocess.append("   dr:parameter \"" + pv.getName() + "\" ; \n");
						preprocess.append("   rr:constant \"" + pv.getValue() + "\" ; \n");
						preprocess.append("] ; \n");
					}
					preprocess.append("] ;\n");
				} else {
					preprocess.append("[ dr:name \"PREPROCESS-LEXICALVALUE-" + (i + 1) + "\" ; \n");
					preprocess.append("  dr:function <" + D2RMLOPVocabulary.f_identity + "> ; \n");
					preprocess.append("dr:parameterBinding [ \n");
					preprocess.append("   dr:parameter \"input\" ; \n");
					if (i == 0) {
						preprocess.append("   rr:column \"lexicalValue\" ; \n");
					} else {
						preprocess.append("   rr:column \"PREPROCESS-LEXICALVALUE-" + i + "\" ; \n");
					}
					
					preprocess.append("   dr:condition [ \n");
//						if (i == 0) {
//							preprocess.append("   rr:column \"lexicalValue\" ; \n");
//						} else {
//							preprocess.append("   rr:column \"PREPROCESS-LEXICALVALUE-" + i + "\" ; \n");
//						}
//						preprocess.append("  <" + pi.getFunction() + "> \"" + pi.getParameters().get(0).getValue() + "\" ; \n");
					if (pi.getModifier() == null) {
						preprocess.append("  dr:function <" + pi.getFunction() + "> ; \n");
						preprocess.append("dr:parameterBinding [ \n");
						preprocess.append("   dr:parameter \"input\" ; \n");
						if (i == 0) {
							preprocess.append("   rr:column \"lexicalValue\" ; \n");
						} else {
							preprocess.append("   rr:column \"PREPROCESS-LEXICALVALUE-" + i + "\" ; \n");
						}
						preprocess.append("] ; \n");
						for (DataServiceParameterValue pv : pi.getParameters()) {
							preprocess.append("dr:parameterBinding [ \n");
							preprocess.append("   dr:parameter \"" + pv.getName() + "\" ; \n");
							preprocess.append("   rr:constant \"" + pv.getValue() + "\" ; \n");
							preprocess.append("] ; \n");
						}
					} else if (pi.getModifier().equals(D2RMLOPVocabulary.logicalNot.toString())){
						preprocess.append("   dr:booleanOperator <" + D2RMLOPVocabulary.logicalNot + "> ; \n");
						preprocess.append("   dr:condition [ \n");
						preprocess.append("  dr:function <" + pi.getFunction() + "> ; \n");
						preprocess.append("dr:parameterBinding [ \n");
						preprocess.append("   dr:parameter \"input\" ; \n");
						if (i == 0) {
							preprocess.append("   rr:column \"lexicalValue\" ; \n");
						} else {
							preprocess.append("   rr:column \"PREPROCESS-LEXICALVALUE-" + i + "\" ; \n");
						}
						preprocess.append("] ; \n");
						for (DataServiceParameterValue pv : pi.getParameters()) {
							preprocess.append("dr:parameterBinding [ \n");
							preprocess.append("   dr:parameter \"" + pv.getName() + "\" ; \n");
							preprocess.append("   rr:constant \"" + pv.getValue() + "\" ; \n");
							preprocess.append("] ; \n");
						}
						preprocess.append("] ; \n");
					}
				
					preprocess.append("] ; \n");
					
					preprocess.append("] ; \n");
					preprocess.append("] ; \n");
				}
			}
				
			preprocess.append("dr:definedColumn \n");
			preprocess.append("[ \n");
			preprocess.append("  dr:name \"PREPROCESS-LITERAL\" ; \n");
			preprocess.append("  dr:function <http://islab.ntua.gr/ns/d2rml-op#strlang> ; \n");
			preprocess.append("  dr:parameterBinding [ \n");
			preprocess.append("     dr:parameter \"lexicalValue\" ; \n");
			preprocess.append("     rr:column \"PREPROCESS-LEXICALVALUE-" + pis.size() + "\" ; \n");
			preprocess.append("  ] ; \n");
			preprocess.append("  dr:parameterBinding [ \n");
			preprocess.append("     dr:parameter \"language\" ; \n");
			preprocess.append("     rr:column \"language\" ; \n");
			preprocess.append("  ] ; \n");
			preprocess.append("] ; \n");

			
			str = str.replace("{##ppPREPROCESS##}", preprocess.toString());
			str = str.replace("{##ppLITERAL##}", "PREPROCESS-LITERAL");
			str = str.replace("{##ppLEXICAL-VALUE##}", "PREPROCESS-LEXICALVALUE-" + pis.size());

		} else {
			str = str.replace("{##ppPREPROCESS##}", "");
			str = str.replace("{##ppLITERAL##}", "literal");
			str = str.replace("{##ppLEXICAL-VALUE##}", "lexicalValue");
		}

//		System.out.println(str);
		return str;

	}
	
	@Override
	public AnnotatorContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		AnnotatorContainer ac = new AnnotatorContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ac.annotatorDocument == null || ac.getDataset() == null) {
			return null;
		} else {
			return ac;
		}
	}
	
	public AnnotatorPrepareStatus prepareAnnotator(AnnotatorContainer ac, boolean checkOnly) throws Exception {

		AnnotatorDocument adoc = ac.annotatorDocument;
		String id = ac.annotatorId.toString();
		
		Map<String, Object> params = new HashMap<>();

		for (DataServiceParameterValue dsp : adoc.getParameters()) {
			params.put(dsp.getName(), dsp.getValue());
		}

		if (adoc.getThesaurus() != null) {
			GraphLocation gl = idService.getGraph(adoc.getThesaurus());
			if (gl != null && gl.isPublik()) {
				params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(adoc.getThesaurus()).toString());
			}
		}
				
		AnnotatorPrepareStatus res = AnnotatorPrepareStatus.UNKNOWN;
		
		try (RDFOutputHandler outhandler = new StringRDFOutputHandler()) {
			
			String str = applyPreprocessToMappingDocument(adoc, params);

			D2RMLModel rmlMapping = D2RMLModel.readFromString(str, resourceVocabulary.getMappingAsResource(id).toString(), 
					                                               ResourceFactory.createResource(resourceVocabulary.getMappingAsResource(id).toString() + "#" + (checkOnly ? "IsPreparedSpecification" : "PrepareSpecification") ));
			
//			System.out.println(">>> " + ResourceFactory.createResource(resourceVocabulary.getMappingAsResource(id).toString() + "#" + (checkOnly ? "IsPreparedSpecification" : "PrepareSpecification") ));
//			System.out.println(">>> " + rmlMapping.getD2RMLSpecification());
			
			if (rmlMapping.getD2RMLSpecification() == null) {
				res = AnnotatorPrepareStatus.PREPARED; 
			
			} else {
				Executor exec = new Executor(outhandler, safeExecute);
				
				exec.setMonitor(new SimpleMonitor());
				exec.execute(rmlMapping, params);
				
				Model model = ModelFactory.createDefaultModel();
				
//				System.out.println(((StringOutputHandler)outhandler).getResult());
				
				try (StringReader sr = new StringReader(((StringOutputHandler)outhandler).getResult())) {
					RDFDataMgr.read(model, sr, null, Lang.TURTLE);
					
					StmtIterator stmtIter = null;
					
					stmtIter = model.listStatements(null, SEMAVocabulary.annotatorPrepared, ResourceFactory.createTypedLiteral(true));
					if (stmtIter.hasNext()) {
						res = AnnotatorPrepareStatus.PREPARED;
					} else {
						stmtIter = model.listStatements(null, SEMAVocabulary.annotatorPreparing, ResourceFactory.createTypedLiteral(true));
						if (stmtIter.hasNext()) {
							res = AnnotatorPrepareStatus.PREPARING;
						} else {
							res = AnnotatorPrepareStatus.NOT_PREPARED;
						}
					}
	
				}
			}
		}
		
		return res;
	}

	public boolean clearExecution(AnnotatorContainer ac) {
		MappingExecuteState es = ac.annotatorDocument.checkExecuteState(fileSystemConfiguration.getId());
		
		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
			return false;
		}
		
		return clearExecution(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), es);
	}
	
	private boolean clearExecution(AnnotatorContainer ac, MappingExecuteState es) {
		return clearExecution(ac.getCurrentUser(), ac.getDataset(), ac.getAnnotatorDocument(), es);
	}
	
	private boolean clearExecution(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, MappingExecuteState es) {
	
//		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
//			return false;
//		}
		
		ProcessStateContainer psv = doc.getCurrentPublishState(virtuosoConfigurations.values());
		
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
					File f = folderService.getAnnotatorExecutionTrigFile(currentUser, dataset, doc, es, i);
					boolean ok = false;
					if (f != null) {
						ok = f.delete();
						if (ok) {
							logger.info("Deleted file " + f.getAbsolutePath());
						}
					}
					if (!ok) {
						logger.warn("Failed to delete trig execution " + i + " for annotator " + doc.getUuid());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		// catalog file
		try {
			File f = folderService.getAnnotatorExecutionCatalogTrigFile(currentUser, dataset, doc, es);
			boolean ok = false;
			if (f != null) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
			}
			if (!ok) {
				logger.warn("Failed to delete catalog for annotator " + doc.getUuid());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			File f = folderService.getAnnotatorExecutionZipFile(currentUser, dataset, doc, es);
			boolean ok = false;
			if (f != null) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
			}
			if (!ok) {
				logger.warn("Failed to delete zipped execution for annotator " + doc.getUuid());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		folderService.deleteAnnotationsExecutionDatasetFolderIfEmpty(currentUser, dataset);
		
		if (doc.checkExecuteState(fileSystemConfiguration.getId()) == es) {
			doc.deleteExecuteState(fileSystemConfiguration.getId());
		
			if (psv != null) {
				MappingPublishState ps = (MappingPublishState)psv.getProcessState();
				MappingExecuteState pes = ps.getExecute();
		
				// do not clear published execution
				if (pes != null && pes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) != 0 && pes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {
					MappingExecuteState nes = doc.getExecuteState(fileSystemConfiguration.getId());
					nes.setCount(pes.getCount());
					nes.setExecuteCompletedAt(pes.getExecuteCompletedAt());
					nes.setExecuteStartedAt(pes.getExecuteStartedAt());
					nes.setExecuteShards(pes.getExecuteShards());
					nes.setExecuteState(pes.getExecuteState());
				}
			}
		} 
		
		annotatorRepository.save(doc);

		return true;
	}	
	
	@Override
	@Async("mappingExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();

		AnnotatorContainer ac = (AnnotatorContainer)tdescr.getContainer(); 
		AnnotatorDocument adoc = ac.getAnnotatorDocument();
		Dataset dataset = ac.getDataset();
		UserPrincipal currentUser = ac.getCurrentUser();
		String id = ac.annotatorId.toString();
		
		clearExecution(ac);
		
		Date executeStart = new Date(System.currentTimeMillis());
		
		MappingExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());

		es.setExecuteState(MappingState.EXECUTING);
		es.setExecuteStartedAt(executeStart);
		es.setExecuteShards(0);
		es.setCount(0);
		es.setMessages(null);

		annotatorRepository.save(adoc);

		logger.info("Annotator " + id + " starting");
		
		em.sendMessage(new ExecuteNotificationObject(es.getExecuteState(), ac));
		
		try (FileSystemRDFOutputHandler outhandler = folderService.createAnnotationsExecutionRDFOutputHandler(ac, shardSize)) {

	    	DatasetCatalog dcg = schemaService.asCatalog(adoc.getDatasetUuid());

	    	String fromClause = schemaService.buildFromClause(dcg);
			
			String prop = PathElement.onPathStringListAsSPARQLString(adoc.getOnProperty());
//			System.out.println(prop);

			Map<String, Object> params = new HashMap<>();

//			TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
			TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
				
//			params.put("iidatabase", vc.getSparqlEndpoint());
			params.put("iiproperty", prop);
//			params.put("iigraph", resourceVocabulary.getDatasetAsResource(ds.getUuid()).toString());
			params.put("iigraph", fromClause);
//			params.put("iitime", currentTime());
			params.put("iirdfsource", vc.getSparqlEndpoint());
			params.put("iiannotator", resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()));

			// for compatibility: add empty values to later added parameters
			Map<String, DataServiceParameter> map = new HashMap<>();
			for (DataServiceParameter dsp : ac.getDataService().getParameters()) {
				map.put(dsp.getName(), dsp);
			}
			
			for (DataServiceParameterValue dsp : adoc.getParameters()) {
				params.put(dsp.getName(), dsp.getValue());
				
				map.remove(dsp.getName());
			}
			
			for (DataServiceParameter dsp : map.values()) {
				params.put(dsp.getName(), dsp.getDefaultValue() != null ? dsp.getDefaultValue() : "");
			}

			if (adoc.getThesaurus() != null) {
				
//				logger.info("Annotator " + id + " reading thesaurus");
//				
//				String sparql = "SELECT ?url FROM <" + resourceVocabulary.getContentGraphResource() + "> " + "WHERE { "
//						+ " ?url <http://purl.org/dc/elements/1.1/identifier> <" + adoc.getThesaurus() + "> } ";
//
//				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
//						QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//					ResultSet rs = qe.execSelect();
//					while (rs.hasNext()) {
//						QuerySolution sol = rs.next();
//
//						params.put("iithesaurus", sol.get("url").toString());
//						break;
//					}
//				}
				
				GraphLocation gl = idService.getGraph(adoc.getThesaurus());
				if (gl != null && gl.isPublik()) {
					params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(adoc.getThesaurus()).toString());
				}

			}

			logger.info("Annotator " + id + " preprocessing");
			
			String str = applyPreprocessToMappingDocument(adoc, params);

//			System.out.println(">> " + str);

			Executor exec = new Executor(outhandler, safeExecute);
			
			folderService.checkCreateExtractTempFolder(currentUser);

			try  {
//				tdescr.setMonitor(em);
				exec.setMonitor(em);

				D2RMLModel d2rml = D2RMLModel.readFromString(str);
				
				exec.configureFileExtraction(extractMinSize, folderService.getExtractTempFolder(currentUser), d2rml.usesCaches() ? restCacheSize : 0);
				
				em.createStructure(d2rml);

				logger.info("Annotator started -- id: " + id);

				em.sendMessage(new ExecuteNotificationObject(MappingState.EXECUTING, ac));
				
				exec.keepSubjects(true);
				exec.execute(d2rml, params);

				Set<Resource> subjects = exec.getSubjects();

				if (subjects.size() > 0) {
					try (Writer sw = new OutputStreamWriter(new FileOutputStream(folderService.createAnnotationsExecutionCatalogFile(currentUser, dataset, adoc, es), false), StandardCharsets.UTF_8)) {
						
						sw.write("<" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()).toString() + ">\n");
						sw.write("        <http://purl.org/dc/terms/hasPart>\n");
						sw.write("                ");
						int c = 0;
						for (Resource r : subjects) {
							if (c++ > 0) {
								sw.write(" , ");
							}
							sw.write("<" + r.getURI() + ">");
						}
						sw.write(" .");
					}
				}
				
				em.complete();
				
//				Date executeFinish = new Date(System.currentTimeMillis());

				es.setExecuteState(MappingState.EXECUTED);
				es.setExecuteCompletedAt(em.getCompletedAt());
				es.setExecuteShards(outhandler.getShards());
//				es.setCount(outhandler.getTotalItems());
				es.setCount(subjects.size()); // is this the same with outhandler.getTotalItems() ? No e.g. in grouped execution
				
				es.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getD2rmlExecution());
				
				annotatorRepository.save(adoc);

				em.sendMessage(new ExecuteNotificationObject(es.getExecuteState(), ac), subjects.size());

				logger.info("Annotator executed -- id: " + id + ", shards: " + outhandler.getShards());

				if (subjects.size() > 0) {
					try {
						zipExecution(currentUser, dataset, adoc, es, outhandler.getShards());
					} catch (Exception ex) {
						ex.printStackTrace();
						
						logger.info("Zipping annotator execution failed -- id: " + id);
					}
				}
				
//				publishToDatabase(currentUser, new String[] {id});
//				
//				logger.info("Annotations copied to db -- id: " + id );
				
				return new AsyncResult<>(em.getCompletedAt());

			} catch (Exception ex) {
//				ex.printStackTrace();
				
				logger.info("Annotator failed -- id: " + id);
				
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

			es.setExecuteState(MappingState.EXECUTION_FAILED);
			es.setExecuteCompletedAt(em.getCompletedAt());
			es.setExecuteShards(0);
			es.setSparqlExecuteShards(0);
			es.setCount(0);
			es.setSparqlCount(0);
			es.setMessage(em.getFailureMessage());

			es.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getD2rmlExecution());
			
			annotatorRepository.save(adoc);

			
			em.sendMessage(new ExecuteNotificationObject(MappingState.EXECUTION_FAILED, ac));
			
//			return new AsyncResult<>(false);
			throw new TaskFailureException(ex, em.getCompletedAt());
		}

	}
	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();

		AnnotatorContainer ac = (AnnotatorContainer)tdescr.getContainer();
		AnnotatorDocument adoc = ac.getAnnotatorDocument();
		Dataset dataset = ac.getDataset();
		UserPrincipal currentUser = ac.getCurrentUser();

		TripleStoreConfiguration vc = ac.getDatasetTripleStoreVirtuosoConfiguration();
		
		try {
//			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), ac));
			
			ac.save(iac -> {
				PublishState ips = ((PublishableContainer)iac).getPublishState();
				ips.startDo(pm);
			});
			
			pm.sendMessage(new PublishNotificationObject(ac));
	
			tripleStore.publish(vc, ac);

			pm.complete();
			
			ac.save(iac -> {
				AnnotatorPublishState ips = (AnnotatorPublishState)((PublishableContainer)iac).getPublishState();
				ips.completeDo(pm);
				ips.setExecute(((ExecutableContainer)iac).getExecuteState());
				ips.setAsProperty(adoc.getAsProperty());
			});

			// update annotation edit group change date
			Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
			AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
			
			aeg.setLastPublicationStateChange(new Date());

			annotationEditGroupRepository.save(aeg);

			logger.info("Publication of " + resourceVocabulary.getAnnotatorAsResource(dataset.getUuid()) + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(ac));
		
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);
			
			try {
				ac.save(iac -> {
					PublishState ips = ((PublishableContainer)iac).checkPublishState();
					if (ips != null) {
						ips.failDo(pm);
					}
				});
				
				if (ac.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(ac));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}

	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		AnnotatorContainer ac = (AnnotatorContainer)tdescr.getContainer();
		AnnotatorDocument adoc = ac.getAnnotatorDocument();
		Dataset dataset = ac.getDataset();
		UserPrincipal currentUser = ac.getCurrentUser();

		TripleStoreConfiguration vc = ac.getDatasetTripleStoreVirtuosoConfiguration();

//		AnnotatorPublishState ps = adoc.getPublishState(vc.getId());
		
		try {
			ac.save(iac -> {
				PublishState ips = ((PublishableContainer)iac).getPublishState();
				ips.startUndo(pm);
			});
			
			pm.sendMessage(new PublishNotificationObject(ac));

			tripleStore.unpublish(vc, ac);

			pm.complete();
			
			ac.save(iac -> {
				PublishableContainer ipc = (PublishableContainer)iac;
				ExecutableContainer iec = (ExecutableContainer)iac;
				
				MappingPublishState ips = (MappingPublishState)ipc.getPublishState();
				
				ipc.removePublishState(ips);
				
				MappingExecuteState ies = iec.getExecuteState();
				MappingExecuteState ipes = ips.getExecute();
				if (ies != null && ipes != null && ies.getExecuteStartedAt().compareTo(ipes.getExecuteStartedAt()) != 0) {
					iec.clearExecution(ipes);
				}
			});
			
			// update annotation edit group change date
			Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
			AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
			
			aeg.setLastPublicationStateChange(new Date());
			
			annotationEditGroupRepository.save(aeg);
			
			logger.info("Unpublication of " + resourceVocabulary.getAnnotatorAsResource(dataset.getUuid()) + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(ac));
		
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);

			try {
				ac.save(iac -> {
					PublishState ips = ((PublishableContainer)iac).checkPublishState();
					if (ips != null) {
						ips.failUndo(pm);
					}
				});
				
				if (ac.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(ac));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}

	}
	
	public void unload(Dataset dataset) throws Exception {

		String id = dataset.getId().toString();
		
		try (StringRDFOutputHandler outhandler = new StringRDFOutputHandler()) {

			Map<String, Object> params = new HashMap<>();

			if (dataset.getIdentifier() != null) {
				GraphLocation gl = idService.getGraph(dataset.getIdentifier());
				if (gl != null && gl.isPublik()) {
					params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(dataset.getIdentifier()).toString());
				}
			}

			logger.info("Unloading thesaurus " + dataset.getName());
			
			String d2rmlPath = dataserviceFolder + "enrich-inthesaurus-w3c.ttl"; // temp hardcoded!!!!
			
			String str = null;
			try (InputStream inputStream = resourceLoader.getResource("classpath:" + d2rmlPath).getInputStream()) {
				str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			}
			
			str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
			str = str.replace("{##ppPREPROCESS##}", "");
			
			D2RMLModel rmlMapping = D2RMLModel.readFromString(str, resourceVocabulary.getMappingAsResource(id).toString(), 
                    ResourceFactory.createResource(resourceVocabulary.getMappingAsResource(id).toString() + "#UnloadThesaurusSpecification" ));

			Executor exec = new Executor(outhandler, safeExecute);

			exec.setMonitor(new SimpleMonitor());
			exec.execute(rmlMapping, params);

		
			Model model = ModelFactory.createDefaultModel();
			
			try (StringReader sr = new StringReader(((StringOutputHandler)outhandler).getResult())) {
				RDFDataMgr.read(model, sr, null, Lang.TURTLE);

//		        model.write(System.out, "TURTLE");

//				StmtIterator stmtIter = null;
//				
//				stmtIter = model.listStatements(null, SEMAVocabulary.thesaurusUnloaded, ResourceFactory.createTypedLiteral(true));
//				if (stmtIter.hasNext()) {
//					res = AnnotatorPrepareStatus.PREPARED;
//				} else {
//					stmtIter = model.listStatements(null, SEMAVocabulary.annotatorPreparing, ResourceFactory.createTypedLiteral(true));
//					if (stmtIter.hasNext()) {
//						res = AnnotatorPrepareStatus.PREPARING;
//					} else {
//						res = AnnotatorPrepareStatus.NOT_PREPARED;
//					}
//				}
//
			}
		}

	}

	public ThesaurusLoadStatus isLoaded(Dataset dataset) throws Exception {

		ThesaurusLoadStatus res = ThesaurusLoadStatus.UNKNOWN;
		
		String id = dataset.getId().toString();
		
		try (StringRDFOutputHandler outhandler = new StringRDFOutputHandler()) {

			Map<String, Object> params = new HashMap<>();
			
			if (dataset.getIdentifier() != null) {
				GraphLocation gl = idService.getGraph(dataset.getIdentifier());
				if (gl != null && gl.isPublik()) {
					params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(dataset.getIdentifier()).toString());
				}
			}

			logger.info("Check thesaurus loading status " + dataset.getName());
			
			String d2rmlPath = dataserviceFolder + "enrich-inthesaurus-w3c.ttl"; // temp hardcoded!!!!
			
			String str = null;
			try (InputStream inputStream = resourceLoader.getResource("classpath:" + d2rmlPath).getInputStream()) {
				str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			}
			
			str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
			str = str.replace("{##ppPREPROCESS##}", "");
			
			D2RMLModel rmlMapping = D2RMLModel.readFromString(str, resourceVocabulary.getMappingAsResource(id).toString(), 
                    ResourceFactory.createResource(resourceVocabulary.getMappingAsResource(id).toString() + "#IsThesaurusLoadedSpecification" ));

			Executor exec = new Executor(outhandler, safeExecute);

			exec.setMonitor(new SimpleMonitor());
			exec.execute(rmlMapping, params);
		
			Model model = ModelFactory.createDefaultModel();
			
			try (StringReader sr = new StringReader(((StringOutputHandler)outhandler).getResult())) {
				RDFDataMgr.read(model, sr, null, Lang.TURTLE);

//		        model.write(System.out, "TURTLE");
//
				StmtIterator stmtIter = null;
				
				stmtIter = model.listStatements(null, SEMAVocabulary.thesaurusLoaded, ResourceFactory.createTypedLiteral(true));
				if (stmtIter.hasNext()) {
					res = ThesaurusLoadStatus.LOADED;
				} else {
					stmtIter = model.listStatements(null, SEMAVocabulary.thesaurusLoading, ResourceFactory.createTypedLiteral(true));
					if (stmtIter.hasNext()) {
						res = ThesaurusLoadStatus.LOADING;
					} else {
						res = ThesaurusLoadStatus.NOT_LOADED;
					}
				}
			}
		}
		
		return res;

	}

//	private void zipExecution(UserPrincipal currentUser, AnnotatorDocument adoc, int shards) throws IOException {
//
//		try (FileOutputStream fos = new FileOutputStream(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid() + ".zip");
//				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
//			for (int i = 0; i < shards; i++) {
//				File fileToZip = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
//						+ (i == 0 ? "" : "_#" + i) + ".trig");
//
//				try (FileInputStream fis = new FileInputStream(fileToZip)) {
//					ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
//					zipOut.putNextEntry(zipEntry);
//
//					byte[] bytes = new byte[1024];
//					int length;
//					while ((length = fis.read(bytes)) >= 0) {
//						zipOut.write(bytes, 0, length);
//					}
//				}
//			}
//		}
//	}
	
	private File zipExecution(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, ExecuteState es, int shards) throws IOException {
		
		File file = folderService.createAnnotationsExecutionZipFile(currentUser, dataset, doc, es);
		
		try (FileOutputStream fos = new FileOutputStream(file);
				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
			for (int i = 0; i < shards; i++) {
	        	File fileToZip = folderService.getAnnotatorExecutionTrigFile(currentUser, dataset, doc, es, i);

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
	
	
//  problem with @prefix!!!	
//	private void zipExecution(UserPrincipal currentUser, AnnotatorDocument adoc, int shards) throws IOException {
//
//		try (FileOutputStream fos = new FileOutputStream(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid() + ".zip");
//				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
//
//			File file = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
//			+ ".trig");
//
//			ZipEntry zipEntry = new ZipEntry(file.getName());
//			zipOut.putNextEntry(zipEntry);
//
//			for (int i = 0; i < shards; i++) {
//				File fileToZip = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
//					+ (i == 0 ? "" : "_#" + i) + ".trig");
//
//				try (FileInputStream fis = new FileInputStream(fileToZip)) {
//
//					byte[] bytes = new byte[1024];
//					int length;
//					while ((length = fis.read(bytes)) >= 0) {
//						zipOut.write(bytes, 0, length);
//					}
//				}
//			}
//		}
//	}	

	public List<AnnotatorDocumentResponse> getAnnotators(UserPrincipal currentUser, String datasetUri) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);
		
		List<AnnotatorDocument> docs = annotatorRepository.findByDatasetUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		Optional<Dataset> datasetOpt = datasetRepository.findByUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return new ArrayList<>();
		}
		
		Dataset dataset = datasetOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
		
		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;
		
		List<AnnotatorDocumentResponse> response = docs.stream()
				.map(doc -> modelMapper.annotator2AnnotatorResponse(vc, doc,
						annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(
								doc.getDatasetUuid(), doc.getOnProperty(), doc.getAsProperty(),
								new ObjectId(currentUser.getId())).get()))
				.collect(Collectors.toList());

		return response;
	}
	
	public List<AnnotatorDocumentResponse> getAnnotators(String datasetUri) {

		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);

		List<AnnotatorDocument> docs = annotatorRepository.findByDatasetUuid(datasetUuid);

		Optional<Dataset> datasetOpt = datasetRepository.findByUuid(datasetUuid);

		if (!datasetOpt.isPresent()) {
			return new ArrayList<>();
		}
		
		Dataset dataset = datasetOpt.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
		
		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;

		
		List<AnnotatorDocumentResponse> response = docs.stream()
				.map(doc -> modelMapper.annotator2AnnotatorResponse(vc, doc,
						annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsProperty(
								doc.getDatasetUuid(), doc.getOnProperty(), doc.getAsProperty()).get()))
				.collect(Collectors.toList());
		return response;
	}
	
	
//	public boolean publish(UserPrincipal currentUser, String[] ids) throws Exception {
//		List<AnnotatorDocument> docs = new ArrayList<>();
//
//		logger.info("PUBLICATION STARTED");
//		
//		TripleStoreConfiguration vc = null; // assumes all adocs belong to the same dataset and are published to the save virtuoso;
//				
//		for (String id : ids) {
//			Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//
//			if (doc.isPresent()) {
//				AnnotatorDocument adoc = doc.get();
//				
//				if (vc == null) {
//					Optional<Dataset> dres = datasetRepository.findByUuid(adoc.getDatasetUuid());
//					Dataset ds = dres.get();
//					vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
//				}
//
//				PublishState state = adoc.getPublishState(vc.getId());
//				state.setPublishState(DatasetState.PUBLISHING);
//				state.setPublishStartedAt(new Date(System.currentTimeMillis()));
//
//				annotatorRepository.save(adoc);
//
//				docs.add(adoc);
//			}
//		}
//
////		logger.info("PUBLICATING TO VIRTUOSO");
//		
//		tripleStore.publish(currentUser, vc, docs);
//
//		for (AnnotatorDocument adoc : docs) {
//			AnnotatorPublishState state = adoc.getPublishState(vc.getId());
//			state.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//			state.setPublishState(DatasetState.PUBLISHED);
//			state.setExecute(adoc.getExecuteState(fileSystemConfiguration.getId()));
//			state.setAsProperty(adoc.getAsProperty());
//
//			annotatorRepository.save(adoc);
//
//			// update annotation edit group change date
//			Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
//			AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
//			
//			aeg.setLastPublicationStateChange(new Date());
//
//			annotationEditGroupRepository.save(aeg);
//		}
//
//		System.out.println("PUBLICATION COMPLETED");
//
//		return true;
//	}

//	public boolean unpublish(UserPrincipal currentUser, String[] ids) throws Exception {
//		List<AnnotatorDocument> docs = new ArrayList<>();
//		List<AnnotatorContainer> acs = new ArrayList<>();
//
//		logger.info("UNPUBLICATION STARTED");
//
//		TripleStoreConfiguration vc = null; // assumes all adocs belong to the same dataset and are published to the save virtuoso;
//		
//		for (String id : ids) {
//			AnnotatorContainer ac = getContainer(currentUser, new ObjectId(id));
//			
//			if (ac != null) {
//				AnnotatorDocument adoc = ac.getAnnotatorDocument();
//
//				if (vc == null) {
//					vc = ac.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
//				}
//				
//				PublishState state = adoc.getPublishState(vc.getId());
//				state.setPublishState(DatasetState.UNPUBLISHING);
//				state.setPublishStartedAt(new Date(System.currentTimeMillis()));
//				annotatorRepository.save(adoc);
//
//				docs.add(adoc);
//				acs.add(ac);
//			}
//		}
//		
////		logger.info("UNPUBLICATING FROM VIRTUOSO");
//
//		tripleStore.unpublish(vc, docs);
//
//		for (AnnotatorContainer ac : acs) {
//			AnnotatorDocument adoc = ac.getAnnotatorDocument();
//			
//			AnnotatorPublishState state = adoc.getPublishState(vc.getId());
//			
//			adoc.removePublishState(state);
////			state.setPublishState(DatasetState.UNPUBLISHED);
////			state.setPublishStartedAt(null);
////			state.setPublishCompletedAt(null);
//			
//			MappingExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());
//			MappingExecuteState pes = state.getExecute();
//			if (es != null && pes != null && es.getExecuteStartedAt().compareTo(pes.getExecuteStartedAt()) != 0) {
//				clearExecution(currentUser, ac.getDataset(), adoc, pes);
//			}
//			
//			annotatorRepository.save(adoc);
//			
//			// update annotation edit group change date
//			Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
//			AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
//			
//			aeg.setLastPublicationStateChange(new Date());
//			
//			annotationEditGroupRepository.save(aeg);
//
//		}
//
//		logger.info("UNPUBLICATION COMPLETED");
//
//		return true;
//	}
	
//    private String getAnnotationsFolder(UserPrincipal currentUser) {
//    	if (annotationsFolder.endsWith("/")) {
//    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder.substring(0, annotationsFolder.length() - 1);
//    	} else {
//    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder;
//    	}    	
//    }
    
//	public boolean publishToDatabase(UserPrincipal currentUser, String[] ids) throws Exception {
//		List<AnnotatorDocument> docs = new ArrayList<>();
//
//		for (String id : ids) {
//			Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//
//			if (doc.isPresent()) {
//				AnnotatorDocument adoc = doc.get();
//
////				PublishState state = adoc.getPublishState(virtuosoConfiguration.getId());
////				state.setPublishState(DatasetState.PUBLISHING);
////				state.setPublishStartedAt(new Date(System.currentTimeMillis()));
////
////				annotatorRepository.save(adoc);
//
//				docs.add(adoc);
//			}
//		}
//
////		virtuosoJDBC.publish(currentUser, docs);
//	    	
//	    String af = getAnnotationsFolder(currentUser);
//	    	
//		for (AnnotatorDocument adoc : docs) {
//			annotationRepository.deleteByGenerator(adoc.getUuid());
//
//			ExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());
//		    		
//			for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
//				String fileName = adoc.getUuid() + (i == 0 ? "" : "_#" + i) + ".trig";
//
//				try (BufferedReader reader = new BufferedReader(new FileReader(af + File.separator + fileName))) {
//					StringBuffer annotation = new StringBuffer();
//
//					String line = reader.readLine();
//					while (line != null) {
////						System.out.println(line);
//						if (line.length() > 0) {
//							annotation.append("\n");
//							annotation.append(line);
//						}
//						
//						if (line.endsWith(".")) {
//							Model tmp = ModelFactory.createDefaultModel();
//							try (StringReader sr = new StringReader(annotation.toString())) {
//								tmp.read(sr, null, "ttl");
//							}
//							storeToMongo(currentUser, tmp);
//							annotation = new StringBuffer();
//						}
//						line = reader.readLine();
//					}
//				}
//	    	}
//
//			return true;
//	    }    
//
//		System.out.println("PUBLICATION COMPLETED");
//
//		return true;
//	}
	
	@Autowired
	@Qualifier("date-format")
	private SimpleDateFormat dateFormat;

	
//	private void storeToMongo(UserPrincipal currentUser, Model model) {
//		AnnotationDocument ann = new AnnotationDocument();
//		
//		ann.setUserId(new ObjectId(currentUser.getId()));
//		
//		StmtIterator iter = model.listStatements(null, null, (RDFNode)null);
//		while (iter.hasNext()) {
//			Statement st = iter.next();
//			
//			if (st.getPredicate().equals(RDFVocabulary.type)) {
//				ann.setUuid(SEMAVocabulary.getId(st.getSubject().toString()));
//				ann.addType(st.getObject().toString());
//			} else if (st.getPredicate().equals(DCVocabulary.created)) {
//				try {
//					ann.setCreated(dateFormat.parse(st.getObject().toString().substring(0,19)));
//				} catch (ParseException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			} else if (st.getPredicate().equals(OAVocabulary.hasBody)) {
//				if (!st.getObject().isAnon()) {
//					ann.setHasBody(st.getObject().toString());
//				}
//			} else if (st.getPredicate().equals(OWLTime.intervalStartedBy) || st.getPredicate().equals(OWLTime.hasBeginning)) {
//				ann.setHasBodyStart(st.getObject().toString());
//			} else if (st.getPredicate().equals(OWLTime.intervalFinishedBy) || st.getPredicate().equals(OWLTime.hasEnd)) {
//				ann.setHasBodyEnd(st.getObject().toString());
//			} else if (st.getPredicate().equals(OAVocabulary.hasSource)) {
//				ann.setHasSource(st.getObject().toString());
//			} else if (st.getPredicate().equals(SOAVocabulary.start)) {
//				ann.setStart(st.getObject().asLiteral().getInt());
//			} else if (st.getPredicate().equals(SOAVocabulary.end)) {
//				ann.setEnd(st.getObject().asLiteral().getInt());
//			} else if (st.getPredicate().equals(SOAVocabulary.score)) {
//				ann.setScore(st.getObject().asLiteral().getDouble());
//			} else if (st.getPredicate().equals(SOAVocabulary.onProperty)) {
//				ann.setOnProperty(st.getObject().toString());
//			} else if (st.getPredicate().equals(SOAVocabulary.onValue)) {
//				ann.setOnValue(st.getObject().asLiteral().getLexicalForm());
//				String lang = st.getObject().asLiteral().getLanguage();
//				if (lang.length() > 0) {
//					ann.setOnValueLanguage(st.getObject().asLiteral().getLanguage());				
//				}
//			} else if (st.getPredicate().equals(ASVocabulary.generator)) {
//				ann.setGenerator(SEMAVocabulary.getId(st.getObject().toString()));
//			}
//		}
//		
//		annotationRepository.save(ann);
//
//
//	}

	public org.apache.jena.query.Dataset load(UserPrincipal currentUser, ObjectId id) throws IOException {

		org.apache.jena.query.Dataset ds = DatasetFactory.create();

		AnnotatorContainer ac = getContainer(currentUser, new SimpleObjectIdentifier(id));

		if (ac == null) {
			return ds;
		}
		
		MappingExecuteState es = ac.annotatorDocument.checkExecuteState(fileSystemConfiguration.getId());

		
		if (es.getExecuteShards() != null) {
			for (int i = 0; i < es.getExecuteShards(); i++) {
				File f = folderService.getAnnotatorExecutionTrigFile(currentUser, ac.getDataset(), ac.getAnnotatorDocument(), es, i);
				if (f != null) {
					RDFDataMgr.read(ds, "file:" + f.getCanonicalPath(), null, Lang.TRIG);
				}
			}
		}
//			for (int i = 0; i < Math.max(0, es.getExecuteShards()); i++) { // load only 1st file ????
//				String file = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
//							+ (i == 0 ? "" : "_#" + i) + ".trig";
//	
//				RDFDataMgr.read(ds, "file:" + file, null, Lang.TRIG);
		
		return ds;

	}
	
	public List<ValueCount> getValuesForPage(String onPropertyString, org.apache.jena.query.Dataset rdfDataset, int page) {
		
		// should also filter out URI values here but this would spoil pagination due to previous bug.
		String sparql = 
            "SELECT ?value ?valueCount WHERE { " +
			"  SELECT ?value (count(*) AS ?valueCount)" +
	        "  WHERE { " + 
            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		    "     <" + OAVocabulary.hasTarget + "> ?r . " + 
            "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
		    "     <" + SOAVocabulary.onValue + "> ?value . "  + 
		    "    FILTER (isLiteral(?value)) " +		         
		    "  } " +
			"  GROUP BY ?value " + 
			"  ORDER BY desc(?valueCount) ?value } " + 
 		    "LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1);


    	List<ValueCount> values = new ArrayList<>();
//	    System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
    	try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
    		
    		ResultSet rs = qe.execSelect();
    		
    		while (rs.hasNext()) {
    			QuerySolution qs = rs.next();
    			RDFNode value = qs.get("value");
    			int count = qs.get("valueCount").asLiteral().getInt(); //valueCount is the number a value appears (not of annotations on value)
    			
    			values.add(new ValueCount(value, count));
    		}
    	}
    	
    	return values;
		
	}
	
	public ValueResponseContainer<ValueAnnotation> view(UserPrincipal currentUser, AnnotatorDocument adoc, org.apache.jena.query.Dataset rdfDataset, int page) {
		
//    	String spath = AnnotationEditGroup.onPropertyListAsString(adoc.getOnProperty());
		String spath = PathElement.onPathStringListAsSPARQLString(adoc.getOnProperty());
    	
		String sparql = 
				"SELECT (count(?v) AS ?annCount) (count(DISTINCT ?source) AS ?sourceCount) (count(DISTINCT ?value) AS ?valueCount)" + 
		        "WHERE { " +  
		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
			    " ?v <" + OAVocabulary.hasTarget + "> [ <" + OAVocabulary.hasSource + "> ?source ; <" + SOAVocabulary.onValue + "> ?value ] } ";
	
		int annCount = 0;
		int sourceCount = 0;
		int valueCount = 0;
		
		try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				
				annCount = sol.get("annCount").asLiteral().getInt();
				sourceCount = sol.get("sourceCount").asLiteral().getInt();
				valueCount = sol.get("valueCount").asLiteral().getInt();
			}
		}
		
		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
		vrc.setTotalCount(annCount);
		vrc.setDistinctSourceTotalCount(sourceCount);
		vrc.setDistinctValueTotalCount(valueCount);
		
		
    	List<ValueCount> values = getValuesForPage(spath, rdfDataset, page);
    	
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

    	StringBuffer sb = new StringBuffer();
    	for (ValueCount vc : values) {
			AnnotationEditValue aev = null;
    		
    		if (vc.getValue().isLiteral()) {
				Literal l = vc.getValue().asLiteral();
				String lf = l.getLexicalForm();
				
				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
				sb.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString());
	    		sb.append(" ");
				
				aev = new AnnotationEditValue(vc.getValue().asLiteral());
			} else {
				//ignore URI values. They should not be returned by getValuesForPage 
				
//				sb.append("<" + vc.getValue().toString() + ">");
//	    		sb.append(" ");

//				aev = new AnnotationEditValue(vc.getValue().asResource());
			}
    		
    		if (aev != null) {
				ValueAnnotation va = new ValueAnnotation();
				va.setOnValue(aev);
				va.setCount(vc.getCount()); // the number of appearances of the value
				
				res.put(aev, va);
    		}
    	}

    	String valueString = sb.toString();
    	
//    	System.out.println(valueString);


		sparql =   "SELECT ?value ?t ?ie ?start ?end ?score ?count {" + 	
				   "SELECT distinct ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count) " + 
			        "WHERE { " +  
			        " ?v <" + OAVocabulary.hasTarget + "> ?r . " + 
				    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
				    " { ?v <" + OAVocabulary.hasBody + "> [ " + 
				    " a <" + OWLTime.DateTimeInterval + "> ; " + 
				    " <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
				    " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " +
				    "  ?r <" + OAVocabulary.hasSource + "> ?s ; " + 
				    "     <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
				    "     <" + SOAVocabulary.onValue + "> ?value  . " + 
				    "  VALUES ?value { " + valueString  + " } " +
				    " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +
				    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
				    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " + " } " +
		            "GROUP BY ?t ?ie ?value ?start ?end " +
					"} ORDER BY DESC(?count) ?value ?start ?end ";
    	
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
		
		if (valueString.length() > 0) {
			try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					RDFNode value = sol.get("value");
					
					String ann = sol.get("t") != null ? sol.get("t").toString() : null;
					String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
					Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
					Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
					int count = sol.get("count").asLiteral().getInt();
					Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
					
					AnnotationEditValue aev = null;
					if (value.isResource()) {
						aev = new AnnotationEditValue(value.asResource());
					} else if (value.isLiteral()) {
						aev = new AnnotationEditValue(value.asLiteral());
					}
					
					
					ValueAnnotation va = res.get(aev);
					
					if (va != null && ann != null) {
						va.setCount(count);
						
						ValueAnnotationDetail vad = new ValueAnnotationDetail();
						vad.setValue(ann);
						vad.setValue2(ie);
						vad.setStart(start);
						vad.setEnd(end);
//						vad.setCount(count); // the number of appearances of the annotation 
//						                     // it is different than the number of appearances of the value if multiple annotations exist on the same value
						
						vad.setScore(score);
						
						va.getDetails().add(vad);
					} 
				}

			}
		}
		
		vrc.setValues(new ArrayList<>(res.values()));
		
		return vrc;
    }
	
	public AnnotatorDocument failExecution(AnnotatorContainer ac) {			
		AnnotatorDocument adoc = ac.getAnnotatorDocument();
		
		MappingExecuteState es = adoc.checkExecuteState(ac.getContainerFileSystemConfiguration().getId());
		if (es != null) {
			es.setExecuteState(MappingState.EXECUTION_FAILED);
			es.setExecuteCompletedAt(new Date());
			es.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
			annotatorRepository.save(adoc);
		}
		
		return adoc;
	}
	
//	public void failExecution(String id, Throwable ex, Date date) {
//		AnnotatorDocument adoc = annotatorRepository.findById(id).get();
//		
//		MappingExecuteState es = adoc.checkExecuteState(fileSystemConfiguration.getId());
//		
//		if (es != null) {
//			es.setExecuteState(MappingState.EXECUTION_FAILED);
//			es.setExecuteCompletedAt(date);
//			if (ex != null) {
//				es.addMessage(new NotificationMessage(MessageType.ERROR, ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage()));
//			}
//	
//			annotatorRepository.save(adoc);
//		}
//	}
}
