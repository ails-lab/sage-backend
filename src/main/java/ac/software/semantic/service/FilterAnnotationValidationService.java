package ac.software.semantic.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.WebSocketService;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.model.AnnotationEditFilter;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ExecuteDocument;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.MappingExecutePublishDocument;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.payload.FilterAnnotationValidationResponse;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FilterAnnotationValidationRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class FilterAnnotationValidationService implements ExecutingPublishingService {

	private Logger logger = LoggerFactory.getLogger(FilterAnnotationValidationService.class);

    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;
			
	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    @Value("${annotation.manual.folder}")
    private String annotationsFolder;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;
	
	@Value("${dataservice.definition.folder}")
	private String dataserviceFolder;

	@Autowired
	private Environment env;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private LegacyVocabulary legacyVocabulary;
	
	@Autowired
	private DatasetRepository datasetRepository;
	
	@Autowired
	private AnnotatorDocumentRepository annotatorDocumentRepository;

    @Autowired
    private AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	private FilterAnnotationValidationRepository favRepository;

	@Autowired
	private TripleStore tripleStore;
	
	@Autowired
	private ResourceLoader resourceLoader;

    @Autowired
    private AnnotationEditGroupService aegService;

    @Autowired
	private ModelMapper modelMapper;

	@Autowired
	private FolderService folderService;
	
	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends ObjectContainer> getContainerClass() {
		return FilterAnnotationValidationContainer.class;
	}

	public class FilterAnnotationValidationContainer extends ObjectContainer implements PublishableContainer, ExecutableContainer, AnnotationValidationContainer {
		private ObjectId favId;
		private ObjectId aegId;
		
		private FilterAnnotationValidation fav;
		private AnnotationEditGroup aeg;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		public FilterAnnotationValidationContainer(UserPrincipal currentUser, ObjectId favId) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.favId = favId;

			load();
			loadAnnotationEditGroup();
		}
		
		public FilterAnnotationValidationContainer(UserPrincipal currentUser, FilterAnnotationValidation fav) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.favId = fav.getId();
			
			this.fav = fav;
			loadAnnotationEditGroup();
		}
		
		@Override
		protected void load() {
			Optional<FilterAnnotationValidation> favOpt = favRepository.findById(favId);

			if (!favOpt.isPresent()) {
				return;
			}

			fav = favOpt.get();
		}
		
		private void loadAnnotationEditGroup() {
			this.aegId = fav.getAnnotationEditGroupId();

			Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(fav.getAnnotationEditGroupId());

			if (!aegOpt.isPresent()) {
				return;
			}
		
			aeg = aegOpt.get();
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(fav.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			dataset = datasetOpt.get();
		}

		@Override
		public ExecuteDocument getExecuteDocument() {
			return getFilterAnnotationValidation();
		}
		
		@Override
		public MappingExecutePublishDocument<MappingPublishState> getPublishDocument() {
			return getFilterAnnotationValidation();
		}

		@Override
		public AnnotationValidation getAnnotationValidation() {
			return getFilterAnnotationValidation();
		}

		public FilterAnnotationValidation getFilterAnnotationValidation() {
			return fav;
		}
		
		public void setFilterAnnotationValidation(FilterAnnotationValidation fav) {
			this.fav = fav;
		}

		public ObjectId getFilterAnnotationValidationId() {
			return this.fav.getId();
		}

		public void setFilterAnnotationValidationId(ObjectId favId) {
			this.favId = favId;
		}
		
		@Override
		public void save(MongoUpdateInterface mui) throws Exception {
			synchronized (saveSyncString()) { 
				load();
			
				mui.update(this);
				
				favRepository.save(fav);
			}
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				clearExecution();
					
				favRepository.delete(fav);
	
				return true;
			}
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return favId;
		}
		
		@Override
		public ObjectId getSecondaryId() {
			return aegId;
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
		public FilterAnnotationValidationResponse asResponse() {
			return modelMapper.filterAnnotationValidation2FilterAnnotationValidationResponse(getDatasetTripleStoreVirtuosoConfiguration(), fav);
		}

		public AnnotationEditGroup getAnnotationEditGroup() {
			return aeg;
		}

		public void setAnnotationEditGroup(AnnotationEditGroup aeg) {
			this.aeg = aeg;
		}

		public ObjectId getAnnotationEditGroupId() {
			return aegId;
		}

		public void setAnnotationEditGroupId(ObjectId aegId) {
			this.aegId = aegId;
		}

		@Override
		public String localSynchronizationString() {
			return getContainerFileSystemConfiguration().getId().toString() + ":" + getFilterAnnotationValidationId().toString();
		}

		@Override
		public FileSystemConfiguration getContainerFileSystemConfiguration() {
			return containerFileSystemConfiguration;
		}

		@Override
		public TaskType getExecuteTask() {
			return TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE;
		}

		@Override
		public TaskType getClearLastExecutionTask() {
			return TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION;
		}

		@Override
		public TaskType getPublishTask() {
			return TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH;
		}

		@Override
		public TaskType getRepublishTask() {
			return TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH;
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}		
		
		@Override
		public String syncString() {
			return ("SYNC:" + containerString() + ":" + getPrimaryId()).intern();
		}
		
		@Override
		public String saveSyncString() {
			return ("SYNC:SAVE:" + containerString() + ":" + getPrimaryId()).intern();
		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return syncString(id);
	}
	
	public static String syncString(String id) {
		return ("SYNC:" + FilterAnnotationValidationContainer.class.getName() + ":" + id).intern();
	}
	
	@Override
	public FilterAnnotationValidationContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		FilterAnnotationValidationContainer favc = new FilterAnnotationValidationContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());
		if (favc.fav == null || favc.getDataset() == null) {
			return null;
		} else {
			return favc;
		}
	}

	public FilterAnnotationValidationContainer getContainer(UserPrincipal currentUser, FilterAnnotationValidation fav) {
		FilterAnnotationValidationContainer favc = new FilterAnnotationValidationContainer(currentUser, fav);
		if (favc.fav == null || favc.getDataset() == null) {
			return null;
		} else {
			return favc;
		}
	}
	
	public FilterAnnotationValidation create(UserPrincipal currentUser, String aegId, String name, List<AnnotationEditFilter> filters) {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));
		if (!aegOpt.isPresent()) {
			return null;
		}
		
		AnnotationEditGroup aeg = aegOpt.get();

		List<String> annotatorUuids = annotatorDocumentRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList());
		
		FilterAnnotationValidation fav = new FilterAnnotationValidation();
		fav.setUuid(UUID.randomUUID().toString());
		fav.setDatabaseId(database.getId());
		fav.setUserId(new ObjectId(currentUser.getId()));
		fav.setAnnotationEditGroupId(aeg.getId());
		fav.setDatasetUuid(aeg.getDatasetUuid());
		fav.setOnProperty(aeg.getOnProperty());
		fav.setAsProperty(aeg.getAsProperty());
		fav.setAnnotatorDocumentUuid(annotatorUuids);
		
		fav.setName(name);
		fav.setFilters(filters);
			
		fav = favRepository.save(fav);
	
		return fav;
	}
	
	public FilterAnnotationValidation update(FilterAnnotationValidationContainer favc, String name, List<AnnotationEditFilter> filters) throws Exception {

		favc.save(ifavc -> {
			FilterAnnotationValidation fav = ((FilterAnnotationValidationContainer)ifavc).getFilterAnnotationValidation();

			fav.setName(name);
			fav.setFilters(filters);
		});
			
		return favc.getFilterAnnotationValidation();
	}

	@Override
	@Async("filterAnnotationValidationExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();
		
		FilterAnnotationValidationContainer favc = (FilterAnnotationValidationContainer)tdescr.getContainer();
		TripleStoreConfiguration vc = favc.getDatasetTripleStoreVirtuosoConfiguration();

	    try {
		    Date executeStart = new Date(System.currentTimeMillis());

			serviceUtils.clearExecution(favc);
		    
			favc.save(ifavc -> {			    
				MappingExecuteState ies = ((AnnotationValidationContainer)ifavc).getAnnotationValidation().getExecuteState(fileSystemConfiguration.getId());
			    ies.setExecuteState(MappingState.EXECUTING);
				ies.setExecuteStartedAt(executeStart);
				ies.setExecuteShards(0);
				ies.setCount(0);
				ies.clearMessages();
			});
		} catch (Exception iex) {
			throw new TaskFailureException(iex, new Date());
		}
	    
		logger.info("Filter annotation validation " + favc.getPrimaryId() + " starting");
		
		em.sendMessage(new ExecuteNotificationObject(favc));

		try (FileSystemRDFOutputHandler outhandler = folderService.createAnnotationValidationExecutionRDFOutputHandler(favc, shardSize)) {
			Executor exec = new Executor(outhandler, safeExecute);
			
			try {
				exec.setMonitor(em);
				
				String deleteD2rml = dataserviceFolder + env.getProperty("validator.mark.d2rml");
				D2RMLModel deleteMapping = null;
//				System.out.println("Loading " + deleteD2rml);
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ deleteD2rml).getInputStream()) {
					String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
					str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
					
					deleteMapping = D2RMLModel.readFromString(str);
				}

				String replaceD2rml = dataserviceFolder + env.getProperty("validator.filter-mark-replace.d2rml");
				D2RMLModel replaceMapping = null;
//				System.out.println("Loading " + replaceD2rml);
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ replaceD2rml).getInputStream()) {
					String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
					str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
					
					replaceMapping = D2RMLModel.readFromString(str);
				}
				
				FilterAnnotationValidation fav = favc.getFilterAnnotationValidation();

				String onPropertyString = PathElement.onPathStringListAsSPARQLString(fav.getOnProperty());
				String annfilter = aegService.annotatorFilter("annotation", fav.getAnnotatorDocumentUuid());

				em.createStructure(deleteMapping, outhandler);
				
				em.sendMessage(new ExecuteNotificationObject(favc));
				
				for (AnnotationEditFilter aef : fav.getFilters()) { // both delete and replace
					String expr = aef.getSelectExpression();
	    	
			    	String sparql = 
			    			"SELECT ?annotation " +
	     			        "WHERE { " + 
	    			        "  GRAPH <" + fav.getAsProperty() + "> { " + 
	    			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
	    			        expr + 
						    annfilter +
	    			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
	    			        "    ?target <" + SOAVocabulary.onValue + "> ?value . " +
	    			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
	    		            "  } . " +	    			        
	    			        "  GRAPH <" + resourceVocabulary.getDatasetAsResource(fav.getDatasetUuid()).toString() + "> { " +
	    			        "    ?source " + onPropertyString + " ?value } " +
	                        "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " +		    			        
	    			        "}";		
			    	
					Map<String, Object> params = new HashMap<>();
					params.put("iirdfsource", vc.getSparqlEndpoint());
					params.put("iisparql", sparql);
					params.put("validator", resourceVocabulary.getAnnotationValidatorAsResource(fav.getUuid()));
					
					params.put("action", SOAVocabulary.Delete);
					params.put("scope", "");
					
//					System.out.println(params);
					exec.partialExecute(deleteMapping, params);
				}	
				
				for (AnnotationEditFilter aef : fav.getReplaceFilters()) { 
					String expr = aef.getSelectExpression();
					
			    	String sparql = 
	    			"SELECT * " +
 			        "WHERE { " + 
			        "  GRAPH <" + fav.getAsProperty() + "> { " + 
				    "    ?annotation a ?type . VALUES ?type { <" + SEMAVocabulary.SpatialAnnotation + "> <" + SEMAVocabulary.TemporalAnnotation + "> <" + SEMAVocabulary.TermAnnotation + "> } . " + 
			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
			        expr + 
				    annfilter +
//		            "    OPTIONAL { ?annotation <" + DCTVocabulary.created + "> ?created } . " +
		            "    OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?score } . " +		    		            
			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
			        "    ?target <" + SOAVocabulary.onValue + "> ?value . " +
			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
			        "    OPTIONAL { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " +		    			        
			        "    OPTIONAL { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " +
		            (annfilter.length() == 0 ? "    OPTIONAL { <" + ASVocabulary.generator + "> ?generator } . " : "") +
		            "  } . " +	    			        
			        "  GRAPH <" + resourceVocabulary.getDatasetAsResource(fav.getDatasetUuid()).toString() + "> { " +
			        "    ?source " + onPropertyString + " ?value } " +
                    "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
                    "    ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " +		    			        
			        "}";		
	    	
			    	Map<String, Object> params = new HashMap<>();
			    	params.put("iirdfsource", vc.getSparqlEndpoint());
			    	params.put("iisparql", sparql);
			    	params.put("iiproperty", onPropertyString);
					params.put("iiannotator", resourceVocabulary.getAnnotationValidatorAsResource(fav.getUuid()));
			    	params.put("validator", resourceVocabulary.getAnnotationValidatorAsResource(fav.getUuid()));
			    	params.put("newValue", aef.getNewValue());
			    	params.put("iiconfidence", "1");
			    	
			    	params.put("action", SOAVocabulary.Approve);
			    	
			    	exec.partialExecute(replaceMapping, params);
				}
				
				exec.completeExecution();
		
				em.complete();
					
				favc.save(ifavc -> {			    
					MappingExecuteState ies = ((AnnotationValidationContainer)ifavc).getAnnotationValidation().getExecuteState(fileSystemConfiguration.getId());

					ies.setExecuteState(MappingState.EXECUTED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(outhandler.getShards());
					ies.setCount(outhandler.getTotalItems());
				});
		
				em.sendMessage(new ExecuteNotificationObject(favc), outhandler.getTotalItems());
				
				logger.info("Filter validation executed -- id: " + favc.getPrimaryId() + ", shards: " + 0);

				try {
					serviceUtils.zipExecution(favc, outhandler.getShards());
				} catch (Exception ex) {
					ex.printStackTrace();
					
					logger.info("Zipping filter validation execution failed -- id: " + favc.getPrimaryId());
				}
				
				return new AsyncResult<>(em.getCompletedAt());

			} catch (Exception ex) {
				ex.printStackTrace();
				
				logger.info("Filter validation failed -- id: " + favc.getPrimaryId());

				throw ex;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			
			em.complete(ex);

			try {
				favc.save(ifavc -> {			    
					MappingExecuteState ies = ((FilterAnnotationValidationContainer)ifavc).getFilterAnnotationValidation().getExecuteState(fileSystemConfiguration.getId());
	
					ies.failDo(em);
					ies.setExecuteShards(0);
					ies.setSparqlExecuteShards(0);
					ies.setCount(0);
					ies.setSparqlCount(0);
				});
			} catch (Exception iex) {
				throw new TaskFailureException(iex, em.getCompletedAt());
			}
			
			em.sendMessage(new ExecuteNotificationObject(favc));

			throw new TaskFailureException(ex, em.getCompletedAt());
		}
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
