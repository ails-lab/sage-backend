package ac.software.semantic.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import ac.software.semantic.config.ConfigurationContainer;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.model.AnnotationEditFilter;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.payload.notification.ExecuteNotificationObject;
import ac.software.semantic.payload.request.FilterAnnotationValidationUpdateRequest;
import ac.software.semantic.payload.response.FilterAnnotationValidationResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService.AnnotationEditGroupContainer;
import ac.software.semantic.service.container.AnnotationValidationContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.monitor.ExecuteMonitor;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class FilterAnnotationValidationService implements ExecutingPublishingService<FilterAnnotationValidation,FilterAnnotationValidationResponse>, 
                                                          EnclosedCreatableService<FilterAnnotationValidation, FilterAnnotationValidationResponse,FilterAnnotationValidationUpdateRequest, AnnotationEditGroup> {

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

	@Value("${virtuoso.graphs.separate:#{true}}")
	private boolean separateGraphs;

	@Autowired
	private SparqlQueryService sparqlService;
	
	@Autowired
	private Environment env;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	private TaskRepository taskRepository;

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
	private FolderService folderService;
	
	@Autowired
	private ServiceUtils serviceUtils;
	
	@Autowired
	private SchemaService schemaService;
	
	@Override
	public Class<? extends EnclosedObjectContainer<FilterAnnotationValidation,FilterAnnotationValidationResponse,Dataset>> getContainerClass() {
		return FilterAnnotationValidationContainer.class;
	}
	
	@Override 
	public DocumentRepository<FilterAnnotationValidation> getRepository() {
		return favRepository;
	}

	public class FilterAnnotationValidationContainer extends EnclosedObjectContainer<FilterAnnotationValidation,FilterAnnotationValidationResponse,Dataset> 
	                                                 implements PublishableContainer<FilterAnnotationValidation, FilterAnnotationValidationResponse, MappingExecuteState, MappingPublishState,Dataset>, 
	                                                            AnnotationValidationContainer<FilterAnnotationValidation,FilterAnnotationValidationResponse,Dataset>, 
	                                                            UpdatableContainer<FilterAnnotationValidation, FilterAnnotationValidationResponse,FilterAnnotationValidationUpdateRequest> {
		private ObjectId favId;
		
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
			this(currentUser, fav, null);
		}
		
//		public FilterAnnotationValidationContainer(UserPrincipal currentUser, FilterAnnotationValidation fav, Dataset dataset) {
//			this.containerFileSystemConfiguration = fileSystemConfiguration;
//			this.currentUser = currentUser;
//			
//			this.favId = fav.getId();
//			
//			this.object = fav;
//			this.dataset = dataset;
//			
//			loadAnnotationEditGroup();
//		}
		
		public FilterAnnotationValidationContainer(UserPrincipal currentUser, FilterAnnotationValidation fav, AnnotationEditGroup aeg) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.favId = fav.getId();
			
			this.object = fav;
			this.aeg = aeg;
			
			
//			this.aegId = aeg.getId();
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return favId;
		}
		
		@Override 
		public DocumentRepository<FilterAnnotationValidation> getRepository() {
			return favRepository;
		}
		
		@Override
		public FilterAnnotationValidationService getService() {
			return FilterAnnotationValidationService.this;
		}

		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}
		
		private void loadAnnotationEditGroup() {
//			this.aegId = object.getAnnotationEditGroupId();

			Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(object.getAnnotationEditGroupId());

			if (!aegOpt.isPresent()) {
				return;
			}
		
			aeg = aegOpt.get();
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(object.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			dataset = datasetOpt.get();
		}
		

//		@Override
//		public ExecutableDocument<MappingExecuteState> getExecuteDocument() {
//			return getObject();
//		}
//		
//		@Override
//		public PublishableDocument<MappingExecuteState,MappingPublishState> getPublishDocument() {
//			return getObject();
//		}
//
//		@Override
//		public AnnotationValidation getAnnotationValidation() {
//			return getObject();
//		}

		@Override
		public FilterAnnotationValidation update(FilterAnnotationValidationUpdateRequest ur) throws Exception {

			return update(ifavc -> {
				FilterAnnotationValidation fav = ifavc.getObject();

				fav.setName(ur.getName());
				fav.setFilters(ur.getFilters());
			});
				
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				clearExecution();
					
				favRepository.delete(object);
	
				return true;
			}
		}
		
		@Override
		public ObjectId getSecondaryId() {
			return getAnnotationEditGroup().getId();
//			return aegId;
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
		public FilterAnnotationValidationResponse asResponse() {
			TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();
			
			FilterAnnotationValidationResponse response = new FilterAnnotationValidationResponse();
			response.setId(object.getId().toString());
			response.setUuid(object.getUuid());
			response.setName(object.getName());
			response.setFilters(object.getFilters());
			
			response.copyStates(object, vc, fileSystemConfiguration);
	    	
			response.setCreatedAt(object.getCreatedAt());
			response.setUpdatedAt(object.getUpdatedAt());

	        return response;
		}

		@Override
		public String getDescription() {
			return object.getName();
		}
		
		public AnnotationEditGroup getAnnotationEditGroup() {
			if (aeg == null) {
				loadAnnotationEditGroup();
			}
			
			return aeg;
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
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByFilterAnnotationValidationIdAndFileSystemConfigurationId(getObject().getId(), getContainerFileSystemConfiguration().getId(), type).orElse(null);
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}		
		
		@Override
		public boolean hasValidations() {
			return false;
		}

	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	@Override
	public FilterAnnotationValidationContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		FilterAnnotationValidationContainer favc = new FilterAnnotationValidationContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());
		if (favc.getObject() == null || favc.getEnclosingObject() == null) {
			return null;
		} else {
			return favc;
		}
	}

	@Override
	public ObjectContainer<FilterAnnotationValidation,FilterAnnotationValidationResponse> getContainer(UserPrincipal currentUser, FilterAnnotationValidation fav, AnnotationEditGroup aeg) {
		FilterAnnotationValidationContainer favc = new FilterAnnotationValidationContainer(currentUser, fav, aeg);
		if (favc.getObject() == null || favc.getEnclosingObject() == null) {
			return null;
		} else {
			return favc;
		}
	}

	@Override
	public ObjectContainer<FilterAnnotationValidation,FilterAnnotationValidationResponse> getContainer(UserPrincipal currentUser, FilterAnnotationValidation object) {
		FilterAnnotationValidationContainer favc = new FilterAnnotationValidationContainer(currentUser, object);
		if (favc.getObject() == null || favc.getEnclosingObject() == null) {
			return null;
		} else {
			return favc;
		}
	}

	
	@Override
	public FilterAnnotationValidation create(UserPrincipal currentUser, AnnotationEditGroup aeg, FilterAnnotationValidationUpdateRequest ur) throws Exception {

		Optional<Dataset> datasetOpt = datasetRepository.findByUuid(aeg.getDatasetUuid());
		
		if (!datasetOpt.isPresent()) {
			return null;
		}
		
		Dataset dataset = datasetOpt.get();

		List<String> annotatorUuids = annotatorDocumentRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList());
		
		FilterAnnotationValidation fav = new FilterAnnotationValidation(dataset);
		fav.setUserId(new ObjectId(currentUser.getId()));
		fav.setAnnotationEditGroupId(aeg.getId());
		
		fav.setOnProperty(aeg.getOnProperty());
		fav.setAsProperty(aeg.getAsProperty());
		fav.setAnnotatorDocumentUuid(annotatorUuids);
		
		fav.setTag(aeg.getTag());
		
		fav.setName(ur.getName());
		fav.setFilters(ur.getFilters());
			
		return create(fav);
	}
	
	@Override
	@Async("filterAnnotationValidationExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();
		
		FilterAnnotationValidationContainer favc = (FilterAnnotationValidationContainer)tdescr.getContainer();

	    try {
		    Date executeStart = new Date(System.currentTimeMillis());

			serviceUtils.clearExecution(favc);
		    
			favc.update(ifavc -> {			    
				MappingExecuteState ies = ((FilterAnnotationValidationContainer)ifavc).getExecuteState();
			    
				ies.setExecuteState(MappingState.EXECUTING);
				ies.setExecuteStartedAt(executeStart);
				ies.setExecuteMessage(null);
				ies.setExecuteShards(0);
				ies.setCount(0);
				ies.clearMessages();
				ies.setExecuteMessage(null);
			});
		} catch (Exception iex) {
			throw new TaskFailureException(iex, new Date());
		}
	    
		logger.info("Filter annotation validation " + favc.getPrimaryId() + " starting");
		
		em.sendMessage(new ExecuteNotificationObject(favc));

		try (FileSystemRDFOutputHandler outhandler = folderService.createExecutionRDFOutputHandler(favc, shardSize)) {
			Executor exec = new Executor(outhandler, safeExecute);
			
			TripleStoreConfiguration vc = favc.getDatasetTripleStoreVirtuosoConfiguration();
			
			try {
				exec.setMonitor(em);
				
				String deleteD2rml = dataserviceFolder + env.getProperty("validator.mark.d2rml");
				D2RMLModel deleteMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ deleteD2rml).getInputStream()) {
					String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
					str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
					deleteMapping = D2RMLModel.readFromString(str);
				}

				String replaceD2rml = dataserviceFolder + env.getProperty("validator.filter-mark-replace.d2rml");
				D2RMLModel replaceMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ replaceD2rml).getInputStream()) {
					String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
					str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
					replaceMapping = D2RMLModel.readFromString(str);
				}
				
				FilterAnnotationValidation fav = favc.getObject();
				AnnotationEditGroupContainer aegc = aegService.getContainer(null, new SimpleObjectIdentifier(fav.getAnnotationEditGroupId()));
				
				String onPropertyString = PathElement.onPathStringListAsSPARQLString(fav.getOnProperty());

				DatasetCatalog dcg = schemaService.asCatalog(fav.getDatasetUuid());
				String fromClause = schemaService.buildFromClause(dcg);
		    	String annotatorFromClause = "";

		    	List<AnnotatorDocument> adocs = aegc.getAnnotators();

				for (AnnotatorDocument adoc : adocs) {
					if (separateGraphs) {
						annotatorFromClause += "FROM <" + resourceVocabulary.getAnnotatorAsResource(adoc) + "> ";
					}
				}
				
				String annfilter = sparqlService.generatorFilter("annotation", aegc.getAnnotators().stream().map(adoc -> adoc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));
				if (!separateGraphs) {
					annotatorFromClause = "FROM <" + aegc.getObject().getTripleStoreGraph(resourceVocabulary) + "> ";
				} else {
					annotatorFromClause += "FROM <" + fav.getTripleStoreGraph(resourceVocabulary, separateGraphs) + "> ";
				}
				
				
				em.createStructure(deleteMapping, outhandler);
				em.sendMessage(new ExecuteNotificationObject(favc));
				
				exec.keepSubjects(false);
				for (AnnotationEditFilter aef : fav.getFilters()) { // both delete and replace
					String expr = aef.getSelectExpression();
	    	
			    	String sparql = 
			    			"SELECT ?annotation " +
			    		    fromClause +
//				    	    "FROM NAMED <" + fav.getTripleStoreGraph(resourceVocabulary) + "> " +
			    		    annotatorFromClause +
				    	    (!separateGraphs && fav.getAsProperty() != null ? "FROM NAMED <" + resourceVocabulary.getAnnotationGraphResource() + "> " : "") +			    		    
	     			        "WHERE { " + 
//	    			        "  GRAPH <" + fav.getTripleStoreGraph(resourceVocabulary) + "> { " + 
	    			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
	    			        expr + 
						    annfilter +
	    			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
	    			        "    ?target <" + SOAVocabulary.onValue + "> ?value . " +
	    			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//	    		            "  } . " +	    			        
//	    			        "  GRAPH <" + resourceVocabulary.getDatasetAsResource(fav.getDatasetUuid()).toString() + "> { " +
	    			        "    ?source " + onPropertyString + " ?value " +
//                            "  } " +
							(!separateGraphs && fav.getAsProperty() != null ? "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " : "") +		    			        
	    			        "}";		
			    	
					Map<String, Object> params = new HashMap<>();
					params.put("iirdfsource", vc.getSparqlEndpoint());
					params.put("iisparql", sparql);
					params.put("validator", fav.asResource(resourceVocabulary));
					
					params.put("action", SOAVocabulary.Delete);
					params.put("scope", "");
					
//					System.out.println(params);
					exec.partialExecute(deleteMapping, params);
				}	
				
				exec.keepSubjects(true);
				for (AnnotationEditFilter aef : fav.getReplaceFilters()) { 
					String expr = aef.getSelectExpression();
					
			    	String sparql = 
	    			"SELECT * " +
			    	fromClause +
//				    "FROM NAMED <" + fav.getTripleStoreGraph(resourceVocabulary) + "> " +
			    	annotatorFromClause +
				    (!separateGraphs && fav.getAsProperty() != null ? "FROM NAMED <" + resourceVocabulary.getAnnotationGraphResource() + "> " : "") +				    	
 			        "WHERE { " + 
//			        "  GRAPH <" + fav.getTripleStoreGraph(resourceVocabulary) + "> { " + 
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
//		            "  } . " +	    			        
//			        "  GRAPH <" + resourceVocabulary.getDatasetAsResource(fav.getDatasetUuid()).toString() + "> { " +
			        "    ?source " + onPropertyString + " ?value " + 
//                    " } " +
					(!separateGraphs ? "  GRAPH <" + fav.getTOCGraph(resourceVocabulary, separateGraphs) + "> { ?adocid <" + DCTVocabulary.hasPart + "> ?annotation . } " : "") +
			        "}";		
	    	
			    	Map<String, Object> params = new HashMap<>();
			    	params.put("iirdfsource", vc.getSparqlEndpoint());
			    	params.put("iisparql", sparql);
			    	params.put("iiproperty", onPropertyString);
					params.put("iiannotator", fav.asResource(resourceVocabulary));
			    	params.put("validator", fav.asResource(resourceVocabulary));
			    	params.put("newValue", aef.getNewValue());
			    	params.put("iiconfidence", "1");
			    	
			    	params.put("action", SOAVocabulary.Approve);
			    	
			    	exec.partialExecute(replaceMapping, params);
				}
				
				exec.completeExecution();
		
				Set<Resource> subjects = exec.getSubjects();
				serviceUtils.writeExecutionCatalog(subjects, favc);

				em.complete();
					
				favc.update(ifavc -> {			    
					MappingExecuteState ies = ((AnnotationValidationContainer)ifavc).getAnnotationValidation().getExecuteState(fileSystemConfiguration.getId());

					ies.setExecuteState(MappingState.EXECUTED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(outhandler.getShards());
					ies.setCount(outhandler.getTotalItems());
					ies.setExecuteMessage(null);
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
				favc.update(ifavc -> {			    
					MappingExecuteState ies = ((FilterAnnotationValidationContainer)ifavc).getObject().getExecuteState(fileSystemConfiguration.getId());
	
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

	@Override
	public ListPage<FilterAnnotationValidation> getAllByUser(ObjectId userId, Pageable page) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListPage<FilterAnnotationValidation> getAllByUser(List<AnnotationEditGroup> aeg, ObjectId userId, Pageable page) {
		// TODO Auto-generated method stub
		return null;
	}

}
