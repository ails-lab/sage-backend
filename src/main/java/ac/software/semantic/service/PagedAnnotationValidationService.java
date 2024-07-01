package ac.software.semantic.service;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.APIPagedAnnotationValidationController;
import ac.software.semantic.controller.APIPagedAnnotationValidationController.PageRequestMode;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
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

import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotatorContext;
import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.PagedValidationOption;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.base.StartableDocument;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.PagedAnnotationValidationState;
import ac.software.semantic.model.constants.type.AnnotationEditType;
import ac.software.semantic.model.constants.type.AnnotationValidationMode;
import ac.software.semantic.model.constants.type.SortingType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.DatasetPublishState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.payload.PropertyValue;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueAnnotationReference;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.payload.notification.ExecuteNotificationObject;
import ac.software.semantic.payload.notification.LifecycleNotificationObject;
import ac.software.semantic.payload.request.PagedAnnotationValidationUpdateRequest;
import ac.software.semantic.payload.response.PagedAnnotationValidatationDataResponse;
import ac.software.semantic.payload.response.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.response.ProgressResponse;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.UpdateLockedPageResponse;
import ac.software.semantic.payload.response.ValueResponse;
import ac.software.semantic.payload.response.VocabularyContextResponse;
import ac.software.semantic.payload.response.modifier.PagedAnnotationValidationResponseModifier;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.AnnotationEditRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationPageRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService.AnnotationEditGroupContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.container.AnnotationValidationContainer;
import ac.software.semantic.service.container.StartableContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.MultipleResponseContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.monitor.ExecuteMonitor;
import ac.software.semantic.service.monitor.GenericMonitor;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class PagedAnnotationValidationService implements ExecutingPublishingService<PagedAnnotationValidation,PagedAnnotationValidationResponse>, 
                                                         EnclosedCreatableService<PagedAnnotationValidation, PagedAnnotationValidationResponse,PagedAnnotationValidationUpdateRequest, AnnotationEditGroup>,
                                                         EnclosingService<PagedAnnotationValidation,PagedAnnotationValidationResponse, AnnotationEditGroup> {

	private Logger logger = LoggerFactory.getLogger(PagedAnnotationValidationService.class);

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

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;
	
	@Value("${dataservice.definition.folder}")
	private String dataserviceFolder;
	
    @Autowired
    @Qualifier("annotators")
    private Map<String, DataService> annotators;

	@Value("${virtuoso.graphs.separate:#{true}}")
	private boolean separateGraphs;
	
	@Autowired
	private SparqlQueryService sparqlService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	private VocabularyService vocabularyService;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private Environment env;

    @Autowired
    private AnnotationEditGroupRepository aegRepository;

    @Autowired
    private AnnotationEditGroupService aegService;
    
	@Autowired
	private PagedAnnotationValidationRepository pavRepository;

	@Autowired
	private PagedAnnotationValidationPageRepository pavpRepository;

	@Autowired
	private AnnotationEditRepository annotationEditRepository;

	@Autowired
	private DatasetRepository datasetRepository;
	
	@Autowired
	private DatasetService datasetService;

	@Autowired
	private PagedAnnotationValidationPageLocksService locksService;

	@Autowired
	private TripleStore tripleStore;
	
	@Autowired
	private ResourceLoader resourceLoader;
	
	@Autowired
	private FolderService folderService;

	@Autowired
	private ServiceUtils serviceUtils;
	
    @Autowired
    @Qualifier("paged-validations")
	Map<String, PagedValidationOption> pavOptions;
    
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;
	
	public Class<? extends EnclosedObjectContainer<PagedAnnotationValidation,PagedAnnotationValidationResponse,Dataset>> getContainerClass() {
		return PagedAnnotationValidationContainer.class;
	}
	
	@Override
	public DocumentRepository<PagedAnnotationValidation> getRepository() {
		return pavRepository;
	}
	
	public class PagedAnnotationValidationContainer extends EnclosedObjectContainer<PagedAnnotationValidation,PagedAnnotationValidationResponse,Dataset> 
	                                                implements PublishableContainer<PagedAnnotationValidation, PagedAnnotationValidationResponse,MappingExecuteState,MappingPublishState,Dataset>, 
	                                                           AnnotationValidationContainer<PagedAnnotationValidation,PagedAnnotationValidationResponse,Dataset>,
	                                                           MultipleResponseContainer<PagedAnnotationValidation, PagedAnnotationValidationResponse, PagedAnnotationValidationResponseModifier>,
	                                                           UpdatableContainer<PagedAnnotationValidation, PagedAnnotationValidationResponse,PagedAnnotationValidationUpdateRequest>,
	                                                           StartableContainer<PagedAnnotationValidation,PagedAnnotationValidationResponse> {
		private ObjectId pavId;
		
		private AnnotationEditGroup aeg;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		public PagedAnnotationValidationContainer(UserPrincipal currentUser, ObjectId pavId) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.pavId = pavId;
			this.currentUser = currentUser;
			
			load();
			loadAnnotationEditGroup();
		}
		
		public PagedAnnotationValidationContainer(UserPrincipal currentUser, PagedAnnotationValidation pav) {
			this(currentUser, pav, null);
		}
		
//		public PagedAnnotationValidationContainer(UserPrincipal currentUser, PagedAnnotationValidation pav, Dataset dataset) {
//			this.containerFileSystemConfiguration = fileSystemConfiguration;
//			
//			this.pavId = pav.getId();
//			this.currentUser = currentUser;
//			
//			this.object = pav;
//			this.dataset = dataset;
//			
//			loadAnnotationEditGroup();
//		}
		
		public PagedAnnotationValidationContainer(UserPrincipal currentUser, PagedAnnotationValidation pav, AnnotationEditGroup aeg) {
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.pavId = pav.getId();
			this.currentUser = currentUser;
			
			this.object = pav;
			this.aeg = aeg;
//			this.aegId = aeg.getId();
			
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}

		@Override
		public ObjectId getPrimaryId() {
			return pavId;
		}
		
		@Override
		public DocumentRepository<PagedAnnotationValidation> getRepository() {
			return pavRepository;
		}
		
		@Override
		public PagedAnnotationValidationService getService() {
			return PagedAnnotationValidationService.this;
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

		@Override
		public StartableDocument getLifecycleDocument() {
			return getObject();
		}
		
		@Override
		public PagedAnnotationValidation update(PagedAnnotationValidationUpdateRequest ur) throws Exception {

			return update(ipavc -> {
				PagedAnnotationValidation pav = ipavc.getObject();

				pav.setName(ur.getName());
			
				String mode = ur.getMode();
				
//				if (!mode.equals(pav.getMode())) {
//					pav.setUpdatedAt(new Date());
//				}
				
				pav.setMode(mode);

				pav.setUserVocabulariesFromString(ur.getUserVocabularies());
				pav.setSystemVocabulariesFromString(ur.getSystemVocabularies());
				
			});
		}
		
		@Override
		public ObjectId getSecondaryId() {
			return getAnnotationEditGroup().getId();
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
		public PagedAnnotationValidationResponse asResponse(PagedAnnotationValidationResponseModifier rm) {
	    	PagedAnnotationValidationResponse response = new PagedAnnotationValidationResponse();
	    	
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());
	    	response.setMode(object.getMode());
			response.setComplete(object.isComplete());

	    	response.setAnnotationEditGroupId(object.getAnnotationEditGroupId().toString());
	    	response.setAnnotatedPagesCount(object.getAnnotatedPagesCount());
			response.setNonAnnotatedPagesCount(object.getNonAnnotatedPagesCount());
			response.setAsProperty(object.getAsProperty());
			response.setTag(object.getTag());
			
			if (rm == null || rm.getStates() == ResponseFieldType.EXPAND) { 
				response.copyStates(object, getDatasetTripleStoreVirtuosoConfiguration(), fileSystemConfiguration);
			}
			
			if (rm == null || rm.getDates() == ResponseFieldType.EXPAND) {
				response.setCreatedAt(object.getCreatedAt());
				response.setUpdatedAt(object.getUpdatedAt());
			}
			
			if (rm == null || rm.getVocabularies() == ResponseFieldType.EXPAND) {
				if (object.getSystemVocabularies() != null) {
					List<VocabularyContextResponse> cnt = new ArrayList<>();
				
					for (ObjectId id : object.getSystemVocabularies()) {
						cnt.add(VocabularyContextResponse.create(vocc.getVocsById().get(id)));
					}
					response.setSystemVocabularies(cnt);
				}
				
				if (object.getUserVocabularies() != null) {
					List<VocabularyContextResponse> cnt = new ArrayList<>();
				
					for (ObjectId id : object.getUserVocabularies()) {
						DatasetContainer dc = datasetService.getContainer(null, new SimpleObjectIdentifier(id));
						if (dc != null) {
							Dataset dataset = dc.getObject();
							DatasetPublishState dps = dataset.getCurrentPublishState(virtuosoConfigurations.values()).getProcessState();
							
							VocabularyContextResponse ai = new VocabularyContextResponse();
							ai.setName(dataset.getName());
							ai.setId(dataset.getId().toString());
							ai.setUriDescriptors(dps.namespaces2UriDesciptors());
							
							cnt.add(ai);
						}
					}
					response.setUserVocabularies(cnt);
				}
			}
			
			if (rm != null && rm.getProgress() == ResponseFieldType.EXPAND) {
				response.setProgress(computeProgress());
			}
	    	
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
			return TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE;
		}

		@Override
		public TaskType getClearLastExecutionTask() {
			return TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION;
		}

		@Override
		public TaskType getPublishTask() {
			return TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH;
		}

		@Override
		public TaskType getRepublishTask() {
			return TaskType.PAGED_ANNOTATION_VALIDATION_REPUBLISH;
		}		
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByPagedAnnotationValidationIdAndFileSystemConfigurationId(getObject().getId(), getContainerFileSystemConfiguration().getId(), type).orElse(null);
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}		

		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				clearExecution();
	
				pavRepository.delete(object);
				pavpRepository.deleteByPagedAnnotationValidationId(pavId);
	
				return true;
			}
		}
		
		@Override
		public boolean hasValidations() {
			return annotationEditRepository.existsByPagedAnnotationValidationId(this.pavId);
		}
		
		public List<ResourceContext> getResourceContext(String resource) {
			
			PagedAnnotationValidation pav = getObject();
			
			List<ResourceContext> res = new ArrayList<>();

			boolean noVocabularies = pav.getSystemVocabularies() == null && pav.getUserVocabularies() == null;

			if (noVocabularies || pav.getSystemVocabularies() != null) {
				List<Vocabulary> vocs = vocc.resolve(resource);
				
				if (vocs != null) {
					if (noVocabularies) {
						res.addAll(vocs);
					} else {
						for (Vocabulary voc : vocs) {
							if (pav.getSystemVocabularies().contains(voc.getId())) {
								res.add(voc);
							}
						}
					}
				}
			}
			
			if (pav.getUserVocabularies() != null) {
				for (ObjectId vocId : pav.getUserVocabularies()) {
	
					VocabularyContainer<ResourceContext> voc = datasetService.createVocabularyContainer(vocId);
					List<ResourceContext> vocs = voc.resolve(resource);
					if (vocs != null) {
						res.addAll(vocs);
					}
//					
//					Dataset dataset = datasetRepository.findById(vocId).get();
//					DatasetPublishState dps = dataset.getCurrentPublishState(virtuosoConfigurations.values()).getProcessState();
//					
//					for (String namespace : dps.getUriSpaces()) {
//						if (resource.startsWith(namespace)) {
//							DatasetContext ai = new DatasetContext(schemaService);
//	
//							ai.setUriDescriptors(dps.namespaces2UriDesciptors());
//							ai.setDatasetContainer(datasetService.getContainer(null, dataset));
//	
//							res.add(ai);
//						}
//					}
				}
			}
			
			return res;
		}
		
		private ProgressResponse computeProgress() {

			int totalAnnotations = object.getAnnotationsCount();
			int totalValidations = 0;
			int totalAdded = 0;
			int totalAccepted = 0;
			int totalRejected = 0;
			int totalNeutral = 0;

			for(PagedAnnotationValidationPage page: pavpRepository.findByPagedAnnotationValidationIdAndMode(pavId, AnnotationValidationMode.ANNOTATED_ONLY)) {
				totalValidations += page.getValidatedCount();
				totalAdded += page.getAddedCount();
				totalAccepted += page.getAcceptedCount();
				totalRejected += page.getRejectedCount();
				totalNeutral += page.getNeutralCount();
			}

			for(PagedAnnotationValidationPage page: pavpRepository.findByPagedAnnotationValidationIdAndMode(pavId, AnnotationValidationMode.UNANNOTATED_ONLY)) {
				totalAdded += page.getAddedCount();
			}

			ProgressResponse res = new ProgressResponse();

			res.setTotalAnnotations(totalAnnotations);
			res.setTotalValidations(totalValidations);
			res.setTotalAdded(totalAdded);
			res.setTotalAccepted(totalAccepted);
			res.setTotalRejected(totalRejected);
			res.setTotalNeutral(totalNeutral);
			
			try {
				BigDecimal bd = BigDecimal.valueOf((1.0 * totalValidations / totalAnnotations) * 100);
				bd = bd.setScale(2, RoundingMode.HALF_UP);
				res.setProgress(bd.doubleValue());
			} catch(Exception e) {
				res.setProgress(0);
			}

			return res;
		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	@Override
	public PagedAnnotationValidationContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		PagedAnnotationValidationContainer pavc = new PagedAnnotationValidationContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());
		if (pavc.getObject() == null || pavc.getEnclosingObject() == null) {
			return null;
		} else {
			return pavc;
		}
	}
	
	@Override
	public PagedAnnotationValidationContainer getContainer(UserPrincipal currentUser, PagedAnnotationValidation pav, AnnotationEditGroup aeg) {
		PagedAnnotationValidationContainer pavc = new PagedAnnotationValidationContainer(currentUser, pav, aeg);
		if (pavc.getObject() == null || pavc.getEnclosingObject() == null) {
			return null;
		} else {
			return pavc;
		}
	}
	
	@Override
	public PagedAnnotationValidationContainer getContainer(UserPrincipal currentUser, PagedAnnotationValidation object) {
		PagedAnnotationValidationContainer pavc = new PagedAnnotationValidationContainer(currentUser, object);
		if (pavc.getObject() == null || pavc.getEnclosingObject() == null) {
			return null;
		} else {
			return pavc;
		}
	}
	
	@Override
	public PagedAnnotationValidation create(UserPrincipal currentUser, AnnotationEditGroup aeg, PagedAnnotationValidationUpdateRequest ur) throws Exception {

		// temporary: do not create more that one pavs for an aeg;
		List<PagedAnnotationValidation> pavList = pavRepository.findByAnnotationEditGroupId(aeg.getId());
		if (pavList.size() > 0) {
			throw new Exception("A paged annotation validation already exists");
		}		
		
		DatasetContainer dc = datasetService.getContainer(currentUser, aeg.getDatasetId());
		PagedAnnotationValidation pav = createPagedAnnotationValidation(currentUser, dc.getObject(), aeg, dc.getTripleStoreConfiguration());
		
		pav.setComplete(false);
		pav.setLifecycle(PagedAnnotationValidationState.STARTED);
		pav.setLifecycleStartedAt(new Date());
		pav.setName(ur.getName());
		pav.setMode(ur.getMode());
		pav.setUserVocabulariesFromString(ur.getUserVocabularies());
		pav.setSystemVocabulariesFromString(ur.getSystemVocabularies());
		
		return create(pav);
	}

	private int getAnnotatedValueCount(String onPropertyPath, String asProperty, String annfilter, TripleStoreConfiguration vc, String fromClause) {
		String annotatedCountSparql = 
				"SELECT (COUNT(DISTINCT ?value) AS ?count) " +
		        fromClause + 
//		        "FROM NAMED <" + asProperty + "> " + 
		        asProperty + 
		        "WHERE { " + 
		        "    ?s " + onPropertyPath + " ?value . " + 
//				"  GRAPH <" + asProperty + "> { " + 
		        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
				"     <" + OAVocabulary.hasTarget + "> ?r . " + 
			    annfilter + 
		        "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyPath + "\" ; " + 
		        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		        "     <" + OAVocabulary.hasSource + "> ?s . " + 
//		        "  } " +
                "  FILTER (isLiteral(?value)) " + 
                " } ";
                
		int annotatedValueCount = 0;
		
//		System.out.println("getAnnotatedValueCount");
//		System.out.println(QueryFactory.create(annotatedCountSparql, Syntax.syntaxSPARQL_11));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(annotatedCountSparql, Syntax.syntaxSPARQL_11))) {

			ResultSet rs = qe.execSelect();

			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				annotatedValueCount = sol.get("count").asLiteral().getInt();
			}
		}
		
		return annotatedValueCount;
	}
	
	private int getNonAnnotatedValueCount(String onPropertyPath, String asProperty, String annfilter, TripleStoreConfiguration vc, String fromClause) {
		String nonAnnotatedCountSparql = 
				"SELECT (COUNT(DISTINCT ?value) AS ?count) " +
		        fromClause + 
//		        "FROM NAMED <" + asProperty + "> " +
		        asProperty +
		        "WHERE { " + 
		        "    ?s " + onPropertyPath + " ?value . " + 
				"  FILTER NOT EXISTS { " +
//				" GRAPH <" + asProperty + "> { " + 
		        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
				"     <" + OAVocabulary.hasTarget + "> ?r . " + 
			    annfilter + 			        
		        "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyPath + "\" ; " + 
		        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		        "     <" + OAVocabulary.hasSource + "> ?s . " + 
//		        "  } " + 
		        "} " +
		        "  FILTER (isLiteral(?value)) " +
		        "}";

		int nonAnnotatedValueCount = 0;
		
//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(nonAnnotatedCountSparql, Syntax.syntaxSPARQL_11))) {

			ResultSet rs = qe.execSelect();

			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				nonAnnotatedValueCount = sol.get("count").asLiteral().getInt();
			}
		}
		
		return nonAnnotatedValueCount;
	}
	
	private int getAnnotationsCount(String onPropertyPath, String asProperty, String annfilter, TripleStoreConfiguration vc, String fromClause) {
		String annotationsCountSparql = 
				"SELECT (COUNT(?v) AS ?count) " + 
		        fromClause + 
//		        "FROM NAMED <" + asProperty + "> " +
		        asProperty + 
		        "WHERE { " + 
		        "    ?s " + onPropertyPath + " ?value . " + 
//				"  GRAPH <" + asProperty + "> { " + 
		        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
				"     <" + OAVocabulary.hasTarget + "> ?r . " + 
			    annfilter + 			        
		        "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyPath + "\" ; " + 
		        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		        "     <" + OAVocabulary.hasSource + "> ?s . " + 
//		        "  } " + 
		        "  FILTER (isLiteral(?value)) " +
		        "} ";
		
		int annotationsCount = 0;
		
//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(annotationsCountSparql, Syntax.syntaxSPARQL_11))) {

			ResultSet rs = qe.execSelect();

			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				annotationsCount = sol.get("count").asLiteral().getInt();
			}
		}
		
		return annotationsCount;
	}
	
	private String buildQuery(PagedValidationOption pvo, String onPropertyString, String asProperty, String annfilter, AnnotationValidationMode mode, Map<String, SortingType> pavOrdering,int page, String fromClause, String annotatorFromClause) {
		String query = null;
		if (mode == AnnotationValidationMode.ALL) {
			query = pvo.getAllQuery();
		} else if (mode == AnnotationValidationMode.ANNOTATED_ONLY) {
			query = pvo.getAnnotatedQuery();
		} else if (mode == AnnotationValidationMode.UNANNOTATED_ONLY) {
			query = pvo.getUnannotatedQuery();
		}
		
		query = query.replaceAll("\\{@@PAV_DATASET_URI@@\\}", fromClause + " " + annotatorFromClause).
				replaceAll("\\{@@PAV_AS_PROPERTY_FROM@@\\}", asProperty != null && asProperty.length() > 0 ? " FROM <" + asProperty + "> " : " FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> ").
				replaceAll("\\{@@PAV_ON_PROPERTY_STRING@@\\}", onPropertyString).
				replaceAll("\\{@@PAV_GENERATORS@@\\}", annfilter).
				concat("LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1));

		for (Map.Entry<String, SortingType> entry : pavOrdering.entrySet()) {
			query = query.replaceAll("\\{@@PAV_SORTING_" + entry.getKey() + "@@\\}", entry.getValue().toString());
		}

//		System.out.println("PAV:buildQuery");
//		System.out.println(query);
//		System.out.println(QueryFactory.create(query, Syntax.syntaxSPARQL_11));
		
		return query;
				
	}
	
	public List<ValueCount> getValuesForPage(TripleStoreConfiguration vc, PagedAnnotationValidation pav, String onPropertyString, String annfilter, AnnotationValidationMode mode, int page, String fromClause, String annotatorFromClause) {
		String pavMode = pav.getMode();
		
		String asProperty = "";
		if (!separateGraphs) {
			asProperty = pav.getTripleStoreGraph(resourceVocabulary, separateGraphs);
		}

		if (pavMode == null) {
			pavMode = "PAV-VAL:CNT:DESC"; // for old data, may be update mongo
		}
		
		String[] pavElements = pavMode.split("-");
		String pavBase = "PAV";
		Map<String, SortingType> pavOrderings = new HashMap<>();
		for (int i = 1; i < pavElements.length; i++) {
			String el = pavElements[i].substring(0, pavElements[i].lastIndexOf(":"));
			pavBase += "-" + el;
			pavOrderings.put(el, SortingType.get(pavElements[i].substring(pavElements[i].lastIndexOf(":") + 1)));
		}
		
		String sparql = buildQuery(pavOptions.get(pavBase), onPropertyString, asProperty, annfilter, mode, pavOrderings, page, fromClause, annotatorFromClause);

    	List<List<PathElement>> cPaths = new ArrayList<>();
		if (pav.getControlProperties() != null) {
			for (List<String> extra : pav.getControlProperties()) {
				cPaths.add(PathElement.onPathElementListAsStringListInverse(extra, null));
			}
		}
		
    	List<ValueCount> values = new ArrayList<>();
    	
//    	System.out.println(sparql);
//    	System.out.println("getValuesForPage");
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
    		
    		ResultSet rs = qe.execSelect();
    		
    		while (rs.hasNext()) {
    			QuerySolution qs = rs.next();
    			RDFNode value = qs.get("value");
    			int count = qs.get("valueCount").asLiteral().getInt(); //valueCount is the number a value appears (not of annotations on value) wrong!
    			
    			ValueCount vct = new ValueCount(value, count);

    			if (pav.getControlProperties() != null) {
	        		PropertyValue pv = new PropertyValue(PathElement.onPathElementListAsStringListInverse(pav.getOnProperty(), null), RDFTerm.createRDFTerm(value));
	        		ValueResponseContainer<ValueResponse> vres = schemaService.getItemsForPropertyValue(vc.getSparqlEndpoint(), fromClause, pv, cPaths, 1, 10);
	        		
	        		vct.setControlGraph(vres.getExtra());
    			}
    			
    			values.add(vct);
    		}
    	}
    	
    	return values;
		
	}
	
	private class AnnotationValuesContainer {
		private Map<RDFTermHandler, ValueAnnotation> valueMap;
		private int totalAnnotationsCount;
		
		public Map<RDFTermHandler, ValueAnnotation> getValueMap() {
			return valueMap;
		}
		
		public void setValueMap(Map<RDFTermHandler, ValueAnnotation> valueMap) {
			this.valueMap = valueMap;
		}
		
		public int getTotalAnnotationsCount() {
			return totalAnnotationsCount;
		}
		
		public void setTotalAnnotationsCount(int totalAnnotationsCount) {
			this.totalAnnotationsCount = totalAnnotationsCount;
		}
	}
	
	private AnnotationValuesContainer readPage(PagedAnnotationValidation pav, String onPropertyString, Map<String, AnnotatorDocument> annotatorMap, String annfilter, TripleStoreConfiguration vc, AnnotationValidationMode mode, int page, String fromClause, String annotatorFromClause) {
		List<ValueCount> values = getValuesForPage(vc, pav, onPropertyString, annfilter, mode, page, fromClause, annotatorFromClause);
		
		Map<RDFTermHandler, ValueAnnotation> res = new LinkedHashMap<>();

    	StringBuffer sb = new StringBuffer();
    	for (ValueCount vct : values) {
			RDFTermHandler aev = null;
    		
    		if (vct.getValue().isLiteral()) {
				Literal l = vct.getValue().asLiteral();
				String lf = l.getLexicalForm();
				
//				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
				
				sb.append(RDFTerm.createLiteral(lf, l.getLanguage(), l.getDatatype().getURI().toString()).toRDFString());
	    		sb.append(" ");
				
				aev = new SingleRDFTerm(vct.getValue().asLiteral());
			} else {
				//ignore URI values. They should not be returned by getValuesForPage 
				
//				sb.append("<" + vc.getValue().toString() + ">");
//	    		sb.append(" ");

//				aev = new AnnotationEditValue(vc.getValue().asResource());
			}
    		
    		if (aev != null) {
				ValueAnnotation va = new ValueAnnotation();
				va.setOnValue(aev);
				va.setCount(vct.getCount()); // the number of appearances of the value
				va.setControlGraph(vct.getControlGraph());
				
				res.put(aev, va);
    		}
    	}
    	
    	String valueString = sb.toString();
    	
		String sparql = null;
		
		String graph = 
//			"GRAPH <" + asProperty + "> { " + 
		    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
	        "     <" + OAVocabulary.hasTarget + "> ?r . " + 
		    annfilter +
		    "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
		    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		    "     <" + OAVocabulary.hasSource + "> ?s . " + 
		    "  { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) }  " + 
//		    "  UNION { ?v <" + OAVocabulary.hasBody + "> [ " + 
//		    "  a <" + OWLTime.DateTimeInterval + "> ; " + 
//		    "  <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
//		    "  <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " +
		    "  OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } ." + 
		    "  OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } "; 
//		    "} ";
		    		
		String sparqlRef = null;
		String sparqlGen = null;
		if (mode == AnnotationValidationMode.ANNOTATED_ONLY) {
			sparql = 
//					"SELECT distinct ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(*) AS ?count) (GROUP_CONCAT(?ref ; separator=\"@@\") AS ?refConcat) " + 
//					"SELECT distinct ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(*) AS ?count) " +
					"SELECT ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count) " +
			        fromClause + 
//			        "FROM NAMED <" + pav.getTripleStoreGraph(resourceVocabulary) + ">" +
			        annotatorFromClause +
		            "WHERE { " + 
		            "  ?s " + onPropertyString + " ?value . " + 
//		            "  GRAPH <" + pav.getTripleStoreGraph(resourceVocabulary) + "> { " + 
		                 graph + " . " +
		            "    OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } " + 
//		            "  } " +  
//                  "  OPTIONAL { ?r <" + DCTVocabulary.isReferencedBy + ">  ?ref } . " +                       
                    "  VALUES ?value { " + valueString  + " } " +     
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end ";// +
//					"ORDER BY DESC(?count) ?value ?start ?end";
			
//			sparqlRef = "SELECT distinct ?value ?t ?ie ?start ?end (count(*) AS ?count) ?ref " + 
			sparqlRef = "SELECT ?value ?t ?ie ?start ?end (count(distinct ?s) AS ?count) ?ref " +
			        fromClause + 
//			        "FROM NAMED <" + pav.getTripleStoreGraph(resourceVocabulary) + ">" +
			        annotatorFromClause +
					"WHERE { " + 
		            "  ?s " + onPropertyString + " ?value . " + 
//		            "  GRAPH <" + pav.getTripleStoreGraph(resourceVocabulary) + "> { " + 
		                  graph + " . " + 
		               " ?r <" + DCTVocabulary.isReferencedBy + ">  ?ref " + 
//		            " } " +  
                    "  VALUES ?value { " + valueString  + " } " +     
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end ?ref ";
			
			sparqlGen = "SELECT distinct ?value ?t ?ie ?start ?end ?generator " + 
			        fromClause + 
//			        "FROM NAMED <" + pav.getTripleStoreGraph(resourceVocabulary) + ">" +
			        annotatorFromClause +
		            "WHERE { " + 
		            "  ?s " + onPropertyString + " ?value . " + 
//		            "  GRAPH <" + pav.getTripleStoreGraph(resourceVocabulary) + "> { " + 
		               graph + 
//		               " } " +  
                    "  VALUES ?value { " + valueString  + " } " +     
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end ?generator ";
			
		} else if (mode == AnnotationValidationMode.UNANNOTATED_ONLY) {
			sparql = 
//					"SELECT distinct ?value (count(*) AS ?count) " + 
					"SELECT ?value (count(distinct ?s) AS ?count) " +
			        fromClause + 
//			        "FROM NAMED <" + pav.getTripleStoreGraph(resourceVocabulary) + ">" + 
			        "WHERE { " + 
					"    ?s " + onPropertyString + " ?value . " + 
		            "  FILTER NOT EXISTS { " + 
//					"    GRAPH <" + pav.getTripleStoreGraph(resourceVocabulary) + "> { " + 
					"      ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "         <" + OAVocabulary.hasTarget + "> ?r . " + 
		                   annfilter +
					"      ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
					"         <" + SOAVocabulary.onValue + "> ?value ; " + 
					"         <" + OAVocabulary.hasSource + "> ?s  " + 
//					" } " + 
					"} " +
					"  VALUES ?value { " + valueString  + " } " +
					"} " +
					"GROUP BY ?value ";// +
//			        "ORDER BY DESC(?count) ?value ";
		}    	
		
//		System.out.println("readPage");
//		System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
//    	System.out.println(QueryFactory.create(sparqlRef, Syntax.syntaxSPARQL_11));
    	
//		System.out.println(sb);
//		System.out.println(mode);
		
		int totalAnnotationsCount = 0;
		if (valueString.length() > 0) {
			
//			System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
			
				ResultSet rs = qe.execSelect();
//				System.out.println(rs.hasNext());
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
//					RDFNode value = sol.get("value");
					RDFTermHandler aev = null;
					if (pav.getOnProperty() != null) {
						RDFNode value = sol.get("value");
						
						if (value.isResource()) {
							aev = new SingleRDFTerm(value.asResource());
						} else if (value.isLiteral()) {
							aev = new SingleRDFTerm(value.asLiteral());
						}
					} else {
						//not supported yet
//						List<SingleRDFTerm> list = new ArrayList<>();
//						for (Map.Entry<String,IndexKeyMetadata> entry : eo.getTargets().entrySet()) {
//							RDFNode value = sol.get("value_" + entry.getKey());
//							
//							if (value != null) {
//								SingleRDFTerm st = null;
//								if (value.isResource()) {
//									st = new SingleRDFTerm(value.asResource());
//								} else if (value.isLiteral()) {
//									st = new SingleRDFTerm(value.asLiteral());
//								}
//								st.setName(entry.getValue().getName());
//							
//								list.add(st);
//							}
//						}
//						
//						aev = new MultiRDFTerm(list);
					}
					
					String ann = sol.get("t") != null ? sol.get("t").toString() : null;
					String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;

					Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
					Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
					Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
					
//					String refConcat = sol.get("refConcat") != null ? sol.get("refConcat").toString() : null;
					
					int count = sol.get("count").asLiteral().getInt();
//					int refCount = sol.get("refCount").asLiteral().getInt();
					
//					System.out.println(sol.get("value") + " " + sol.get("generator"));
					
//					RDFTerm aev = RDFTerm.createRDFTerm(value);
					
					totalAnnotationsCount += count;
					
					ValueAnnotation va = res.get(aev);

					if (va != null && ann != null) {
						ValueAnnotationDetail vad = new ValueAnnotationDetail();
							
						vad.setValue(ann);
						vad.setValue2(ie);
						vad.setStart(start);
						vad.setEnd(end);
						vad.setCount(count); // the number of appearances of the annotation 
							                 // it is different than the number of appearances of the value if multiple annotations exist on the same value
							
						vad.setScore(score);
						
//						if (refConcat.length() != 0) {
//							Multiset<String> refSet = HashMultiset.create();
//							for (String s : refConcat.split("@@")) {
//								refSet.add(s);
//							}
//							
//							for (Multiset.Entry<String> entry : refSet.entrySet()) {
//								vad.addReference(new ValueAnnotationReference(entry.getElement(), entry.getCount()));
//							}
//						}
						
						va.getDetails().add(vad);

					}
				}

			}
			
			// very inefficient but virtuoso has bugs and no better way? .....
			
			if (sparqlRef != null) {
				
				//get referenced by per annotation
				
//				System.out.println(QueryFactory.create(sparqlRef, Syntax.syntaxSPARQL_11));
				
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparqlRef, Syntax.syntaxSPARQL_11))) {
					
					ResultSet rs = qe.execSelect();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						
//						RDFNode value = sol.get("value");
						RDFTermHandler aev = null;
						if (pav.getOnProperty() != null) {
							RDFNode value = sol.get("value");
							
							if (value.isResource()) {
								aev = new SingleRDFTerm(value.asResource());
							} else if (value.isLiteral()) {
								aev = new SingleRDFTerm(value.asLiteral());
							}
						} else {
							//not supported yet
//							List<SingleRDFTerm> list = new ArrayList<>();
//							for (Map.Entry<String,IndexKeyMetadata> entry : eo.getTargets().entrySet()) {
//								RDFNode value = sol.get("value_" + entry.getKey());
//								
//								if (value != null) {
//									SingleRDFTerm st = null;
//									if (value.isResource()) {
//										st = new SingleRDFTerm(value.asResource());
//									} else if (value.isLiteral()) {
//										st = new SingleRDFTerm(value.asLiteral());
//									}
//									st.setName(entry.getValue().getName());
//								
//									list.add(st);
//								}
//							}
//							
//							aev = new MultiRDFTerm(list);
						}
						
						String ann = sol.get("t") != null ? sol.get("t").toString() : null;
						String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
	
						Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
						Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
						
						String ref = sol.get("ref").toString();
						
						int count = sol.get("count").asLiteral().getInt();
	//					int refCount = sol.get("refCount").asLiteral().getInt();
						
//						RDFTerm aev = RDFTerm.createRDFTerm(value);
						
						ValueAnnotation va = res.get(aev);
						if (va != null) {
							for (ValueAnnotationDetail vad : va.getDetails()) {
								if (eq(vad.getValue(), ann) && eq(vad.getValue2(),ie) && eq(vad.getStart(), start) && eq(vad.getEnd(),end)) {
	
									vad.addReference(new ValueAnnotationReference(ref, count));
								}
							}
						}
					}
	
				}
				
				
//				System.out.println(QueryFactory.create(sparqlGen, Syntax.syntaxSPARQL_11));
				Map<String, AnnotatorContext> annotatorInfoMap = new HashMap<>();
				
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparqlGen, Syntax.syntaxSPARQL_11))) {
					
					ResultSet rs = qe.execSelect();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						
//						RDFNode value = sol.get("value");
						RDFTermHandler aev = null;
						if (pav.getOnProperty() != null) {
							RDFNode value = sol.get("value");
							
							if (value.isResource()) {
								aev = new SingleRDFTerm(value.asResource());
							} else if (value.isLiteral()) {
								aev = new SingleRDFTerm(value.asLiteral());
							}
						} else {
							//not supported yet
//							List<SingleRDFTerm> list = new ArrayList<>();
//							for (Map.Entry<String,IndexKeyMetadata> entry : eo.getTargets().entrySet()) {
//								RDFNode value = sol.get("value_" + entry.getKey());
//								
//								if (value != null) {
//									SingleRDFTerm st = null;
//									if (value.isResource()) {
//										st = new SingleRDFTerm(value.asResource());
//									} else if (value.isLiteral()) {
//										st = new SingleRDFTerm(value.asLiteral());
//									}
//									st.setName(entry.getValue().getName());
//								
//									list.add(st);
//								}
//							}
//							
//							aev = new MultiRDFTerm(list);
						}						
						
						String ann = sol.get("t") != null ? sol.get("t").toString() : null;
						String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
	
						Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
						Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
						
						String generator = sol.get("generator").toString();
						
//						RDFTerm aev = RDFTerm.createRDFTerm(value);
						
						ValueAnnotation va = res.get(aev);
						if (va != null) {
							for (ValueAnnotationDetail vad : va.getDetails()) {
								if (eq(vad.getValue(), ann) && eq(vad.getValue2(), ie) && eq(vad.getStart(), start) && eq(vad.getEnd(), end)) {

									if (annotatorMap != null) {
										AnnotatorContext ai = annotatorInfoMap.get(generator);
										AnnotatorDocument adoc = annotatorMap.get(resourceVocabulary.getUuidFromResourceUri(generator));

										if (ai == null) {
											DataService annotatorService = annotators.get(adoc.getAnnotator());

											ai = new AnnotatorContext();
											ai.setName(annotatorService.getTitle());

//											if (adoc.getThesaurus() != null) {
											if (adoc.getThesaurusId() != null) {
//												Dataset dataset = datasetRepository.findByIdentifierAndDatabaseId(adoc.getThesaurus(), database.getId()).get();
												Dataset dataset = datasetRepository.findById(adoc.getThesaurusId()).get();

												ai.setId(dataset.getId());												
												ai.setVocabularyContainer(datasetService.createVocabularyContainer(dataset.getId()));

											}
											
											annotatorInfoMap.put(generator, ai);
										}
										
										vad.setAnnotatorInfo(ai);
										if (ai.getVocabularyContainer() != null) {
											vad.setLabel(vocabularyService.getLabel(ann, ai.getVocabularyContainer().resolve(ann), false));
										} else {
											vad.setLabel(vocabularyService.getLabel(ann, null, true));
										}
										
										String dp = adoc.getDefaultTarget();
										if (dp != null) {
											vad.addDefaultTarget(dp);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		
		AnnotationValuesContainer avc = new AnnotationValuesContainer();

		avc.setValueMap(res);
		avc.setTotalAnnotationsCount(totalAnnotationsCount);

		return avc;
	}
	

	private boolean eq(Object o1, Object o2) {
		if ((o1 == null && o2 == null) || (o1 != null && o2 != null && o1.equals(o2))) {
			return true;
		} else {
			return false;
		}
	}
	
	

	
	// should be updated for other values sorting
	public void recreatePagedAnnotationValidation(UserPrincipal currentUser, PagedAnnotationValidation oldPav) throws Exception {

		AnnotationEditGroup aeg = aegRepository.findById(oldPav.getAnnotationEditGroupId()).get();

		DatasetContainer dc = datasetService.getContainer(currentUser, aeg.getDatasetId());
		TripleStoreConfiguration vc = dc.getTripleStoreConfiguration();
		PagedAnnotationValidation newPav = createPagedAnnotationValidation(currentUser, dc.getObject(), aeg, vc);
		
//		oldPav.setAnnotatorDocumentUuid(newPav.getAnnotatorDocumentUuid());
		oldPav.setAnnotatedPagesCount(newPav.getAnnotatedPagesCount());
		oldPav.setAnnotationsCount(newPav.getAnnotationsCount());
		oldPav.setNonAnnotatedPagesCount(newPav.getNonAnnotatedPagesCount());
		
		pavRepository.save(oldPav);
		
		logger.info("Saved updated paged annotation validation for " + oldPav.getId());
		
		pavpRepository.deleteByPagedAnnotationValidationId(oldPav.getId());

		logger.info("Deleted exisiting paged annotation validation pages for " + oldPav.getId());
		
		List<AnnotationEdit> edits = annotationEditRepository.findByPagedAnnotationValidationId(oldPav.getId());
		
		logger.info("Creating updated annotation validation pages for " + oldPav.getId() + ": " + edits.size() + " annotation edits.");
		
		if (edits.size() == 0) {
			return;
		}

		Set<String> editIds = edits.stream().map(edit -> edit.getId().toString()).collect(Collectors.toSet());
		
		if (editIds.size() > 0) {
			for (int j = 1; j <= Math.max(newPav.getAnnotatedPagesCount(),newPav.getNonAnnotatedPagesCount()); j++ ) {
			
//					System.out.println(">> " + j);
				if (editIds.isEmpty()) {
					break;
				}

				if (j <= newPav.getAnnotatedPagesCount()) {
					int before = editIds.size();
					PagedAnnotationValidationPage pavp = repaginateSimulationView(currentUser, oldPav, vc, AnnotationValidationMode.ANNOTATED_ONLY, j, editIds);
					if (before > editIds.size()) {
						logger.info("Created: " + pavp.getMode() + " page " + pavp.getPage() + " : " + pavp.getAnnotationsCount() + " " + pavp.getValidatedCount() + " " + pavp.getUnvalidatedCount() + " " + pavp.getAddedCount() + " / " + editIds.size());
						pavpRepository.save(pavp);
					}
				}

				if (editIds.isEmpty()) {
					break;
				}

				if (j <= newPav.getNonAnnotatedPagesCount()) {
					int before = editIds.size();
					PagedAnnotationValidationPage pavp = repaginateSimulationView(currentUser, oldPav, vc, AnnotationValidationMode.UNANNOTATED_ONLY, j, editIds);
					if (before > editIds.size()) {
						logger.info("Created: " + pavp.getMode() + " PAGE " + pavp.getPage() + " : " + pavp.getAnnotationsCount() + " " + pavp.getValidatedCount() + " " + pavp.getUnvalidatedCount() + " " + pavp.getAddedCount() + " / " + editIds.size());
						pavpRepository.save(pavp);
					}
				}					
			}

			logger.info("Created updated annotation validation pages for " + oldPav.getId() + ". " +  (editIds.size() > 0 ? edits.size() + "have been missed." : ""));
		}
			
	}
	
	private PagedAnnotationValidation createPagedAnnotationValidation(UserPrincipal currentUser, Dataset dataset, AnnotationEditGroup aeg, TripleStoreConfiguration vc) throws Exception {

		AnnotationEditGroupContainer aegc = aegService.getContainer(currentUser, aeg, dataset);
		List<AnnotatorDocument> adocs = aegc.getAnnotators();

		DatasetCatalog dcg = schemaService.asCatalog(dataset);
		String fromClause = schemaService.buildFromClause(dcg);

		String annotatorFromClause = "";
//		String annotationGraph = "";
		
		if (separateGraphs) {
			for (AnnotatorDocument adoc : adocs) {
				if (separateGraphs) {
					annotatorFromClause += "FROM <" + resourceVocabulary.getAnnotatorAsResource(adoc) + "> ";
				}
			}
//			annotationGraph = annotatorFromClause;
		} else {		
			if (!separateGraphs) {
				annotatorFromClause += "FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> "; 
			} else {
				annotatorFromClause += "FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> "; // for compatibility to remove
			}			
//			annotationGraph = "FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> " ;
		}
		 
		String annfilter = sparqlService.generatorFilter("v", adocs.stream().map(adoc -> adoc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));

		
		String onPropertyPath = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
		
//		logger.info("Creating paged annotation validation on " + aeg.getDatasetUuid() + "/" + annotationGraph + "/" + aeg.getOnProperty() + ".");
		
		int annotatedValueCount = getAnnotatedValueCount(onPropertyPath, annotatorFromClause, annfilter, vc, fromClause);
		int annotatedPages = annotatedValueCount / pageSize + (annotatedValueCount % pageSize > 0 ? 1 : 0);

		int nonAnnotatedValueCount = getNonAnnotatedValueCount(onPropertyPath, annotatorFromClause, annfilter, vc, fromClause);
		int nonAnnotatedPages = nonAnnotatedValueCount / pageSize + (nonAnnotatedValueCount % pageSize > 0 ? 1 : 0);

		int annotationsCount = getAnnotationsCount(onPropertyPath, annotatorFromClause, annfilter, vc, fromClause);

		PagedAnnotationValidation pav = new PagedAnnotationValidation(dataset);

		pav.setUserId(new ObjectId(currentUser.getId()));
//		pav.setAnnotatorDocumentUuid(adocs.stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList()));
		pav.setAnnotationEditGroupId(aeg.getId());
		pav.setOnProperty(aeg.getOnProperty());
		pav.setAsProperty(aeg.getAsProperty());
		
		pav.setTag(aeg.getTag());

		pav.setPageSize(pageSize);
		pav.setAnnotationsCount(annotationsCount);
		pav.setAnnotatedPagesCount(annotatedPages);
		pav.setNonAnnotatedPagesCount(nonAnnotatedPages);

//		logger.info("Created paged annotation validation on " + aeg.getDatasetUuid() + "/" + annotationGraph + "/" + aeg.getOnProperty() + ": values=" + annotatedValueCount + "/" + nonAnnotatedValueCount + ", pages=" + annotatedPages + "/" + nonAnnotatedPages);

		return pav;
	}
	
	public PagedAnnotationValidatationDataResponse view(UserPrincipal currentUser, TripleStoreConfiguration vc, ObjectId pavId, AnnotationValidationMode mode, int page, boolean ignoreAdded) {
		
		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(pavId);
		if (!pavOpt.isPresent()) {
			return new PagedAnnotationValidatationDataResponse();
		}		
		
		PagedAnnotationValidation pav = pavOpt.get();

		DatasetCatalog dcg = schemaService.asCatalog(pav.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		String annotatorFromClause = "";
		
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());

    	AnnotationEditGroupContainer aegc = aegService.getContainer(currentUser, new SimpleObjectIdentifier(pav.getAnnotationEditGroupId()));
    	List<AnnotatorDocument> adocs = aegc.getAnnotators();

		Map<String, AnnotatorDocument> annotatorMap = new HashMap<>();
		for (AnnotatorDocument adoc : adocs) {
			annotatorMap.put(adoc.getUuid(), adoc);
			if (separateGraphs) {
				annotatorFromClause += "FROM <" + resourceVocabulary.getAnnotatorAsResource(adoc) + "> ";
			}
		}
		
		if (!separateGraphs) {
			annotatorFromClause = "FROM <" + aegc.getObject().getTripleStoreGraph(resourceVocabulary) + "> ";
		} else {
			annotatorFromClause += "FROM <" + pav.getTripleStoreGraph(resourceVocabulary, separateGraphs) + "> ";
			annotatorFromClause += "FROM <" + aegc.getObject().getTripleStoreGraph(resourceVocabulary) + "> "; // for compatibility, to remove
		}

		String annfilter = sparqlService.generatorFilter("v", adocs.stream().map(adoc -> adoc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));

		AnnotationValuesContainer avc = readPage(pav, onPropertyString, annotatorMap, annfilter, vc, mode, page, fromClause, annotatorFromClause);
		
		ObjectId uid = new ObjectId(currentUser.getId());
		
		int validatedCount = 0;
		int addedCount = 0;
		int acceptedCount = 0;
		int rejectedCount = 0;
		int neutralCount = 0;
		
		Map<RDFTermHandler, ValueAnnotation> res = avc.getValueMap();
		
		VocabularyContainer<ResourceContext> vcont = new VocabularyContainer<>();
		
		if (pav.getSystemVocabularies() != null) {
			for (ObjectId id : pav.getSystemVocabularies()) {
				vcont.add(vocc.getVocsById().get(id));
			}
		}
		
		if (pav.getUserVocabularies() != null) {
			datasetService.createVocabularyContainer(pav.getUserVocabularies(), vcont);
		}
		
		for (Map.Entry<RDFTermHandler, ValueAnnotation> entry : res.entrySet()) {
			RDFTerm aev = (RDFTerm)entry.getKey();
			ValueAnnotation nva = entry.getValue();
			
			Set<ObjectId> set = new HashSet<>();
			
			for (ValueAnnotationDetail vad : nva.getDetails()) {
				Optional<AnnotationEdit> editOpt = null;
				if (aev.getIri() != null) {
					editOpt = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAnnotationValueAndStartAndEnd(pav.getAnnotationEditGroupId(), aev.getIri(), vad.getValue(), vad.getStart() != null ? vad.getStart() : -1, vad.getEnd() != null ? vad.getEnd() : -1);
				} else {
					editOpt = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAnnotationValueAndStartAndEnd(pav.getAnnotationEditGroupId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype(), vad.getValue(), vad.getStart() != null ? vad.getStart() : -1, vad.getEnd() != null ? vad.getEnd() : -1 );
				}

				// no added annotation should appear here
				if (editOpt.isPresent()) {
					AnnotationEdit edit = editOpt.get();

					vad.setId(edit.getId().toString()); // missing from repaginate view
					
					if (edit.getAcceptedByUserId().size() > 0 || edit.getRejectedByUserId().size() > 0) {
						validatedCount += vad.getCount();
						
						if (edit.getAcceptedByUserId().size() > edit.getRejectedByUserId().size()) {
							acceptedCount += vad.getCount();
						}  else if (edit.getAcceptedByUserId().size() < edit.getRejectedByUserId().size()) {
							rejectedCount += vad.getCount();
						} else {
							neutralCount += vad.getCount();
						}
						
					}
					
					set.add(edit.getId());
					
					if (edit.getAcceptedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.ACCEPT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
						vad.setSelectedTarget(edit.getTargetAcceptPropertyForUserId(uid));
						
					} else if (edit.getRejectedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.REJECT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
//						vad.setSelectedTarget(edit.getMostAcceptedTargetAcceptProperty());
						
					} else if (edit.getAddedByUserId().contains(uid)) { // should not allow addition of existing annotation
//						vad.setState(AnnotationEditType.ADD);
//						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
//						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
						vad.setSelectedTarget(edit.getTargetAcceptPropertyForUserId(uid));
						
					} else {
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
//						vad.setSelectedTarget(edit.getMostAcceptedTargetAcceptProperty());
						
					}
					
					vad.setOthersTarget(edit.getMostAcceptedTargetAcceptProperty());
					
					if (edit.getAddedByUserId().size() > 0) {
						vad.setManual(true);
						
						vad.setReferences(edit.getReferences());
					}

				}
			}

			if (!ignoreAdded) {
				List<AnnotationEdit> edits = null;
				if (aev.getIri() != null) {
					edits = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAdded(pav.getAnnotationEditGroupId(), aev.getIri());
				} else {
					edits = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAdded(pav.getAnnotationEditGroupId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype());
				}

				for (AnnotationEdit edit : edits) {
					if (set.contains(edit.getId())) {
						continue;
					}
					
					addedCount += nva.getCount();

					ValueAnnotationDetail vad = new ValueAnnotationDetail();
					vad.setValue(edit.getAnnotationValue());
					vad.setStart(edit.getStart() != -1 ? edit.getStart() : null);
					vad.setEnd(edit.getEnd() != -1 ? edit.getEnd() : null);
					vad.setCount(nva.getCount());
					
//					pav.getU
					vad.setLabel(vocabularyService.getLabel(edit.getAnnotationValue(), vcont));
					
					vad.setId(edit.getId().toString());

					if (edit.getAddedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.ADD);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
						vad.setSelectedTarget(edit.getTargetAcceptPropertyForUserId(uid));
					} else if (edit.getAcceptedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.ACCEPT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
						vad.setSelectedTarget(edit.getTargetAcceptPropertyForUserId(uid));
					} else if (edit.getRejectedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.REJECT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
//						vad.setSelectedTarget(edit.getMostAcceptedTargetAcceptProperty());
					} else {
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
						
//						vad.setSelectedTarget(edit.getMostAcceptedTargetAcceptProperty());
					}
					
					vad.setOthersTarget(edit.getMostAcceptedTargetAcceptProperty());
					
					if (edit.getAddedByUserId().size() > 0) {
						vad.setManual(true);
						
						vad.setReferences(edit.getReferences());
					}


					nva.getDetails().add(vad);
				}
			}
		}		
		
		// get page
		Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findByPagedAnnotationValidationIdAndModeAndPage(pav.getId(), mode, page);
		PagedAnnotationValidationPage pavp = null;
		if (!pavpOpt.isPresent()) {
			pavp = new PagedAnnotationValidationPage();
			pavp.setDatabaseId(database.getId());
			pavp.setPagedAnnotationValidationId(pav.getId());
			pavp.setAnnotationEditGroupId(pav.getAnnotationEditGroupId());
			pavp.setMode(mode);
			pavp.setPage(page);
			pavp.setAnnotationsCount(avc.getTotalAnnotationsCount());
			pavp.setValidatedCount(validatedCount);
			pavp.setUnvalidatedCount(avc.getTotalAnnotationsCount() - validatedCount);
			pavp.setAddedCount(addedCount);
			pavp.setAcceptedCount(acceptedCount);
			pavp.setRejectedCount(rejectedCount);
			pavp.setNeutralCount(neutralCount);			
			pavp.setAssigned(true);
			
			pavpRepository.save(pavp);
		} else {
			pavp = pavpOpt.get();
		}
		
		PagedAnnotationValidatationDataResponse pr = new PagedAnnotationValidatationDataResponse();
		pr.setId(pav.getId().toString());
		pr.setData(new ArrayList<>(res.values()));
		pr.setCurrentPage(page);
		pr.setMode(mode);
		pr.setPagedAnnotationValidationId(pavp.getPagedAnnotationValidationId().toString());
		pr.setPagedAnnotationValidationPageId(pavp.getId().toString());
		pr.setOnProperty(pav.getOnProperty());
		
		if (mode == AnnotationValidationMode.ANNOTATED_ONLY) {
			pr.setTotalPages(pav.getAnnotatedPagesCount());
		} else {
			pr.setTotalPages(pav.getNonAnnotatedPagesCount());
		}
		
		return pr;
    } 

	private PagedAnnotationValidationPage repaginateSimulationView(UserPrincipal currentUser, PagedAnnotationValidation pav, TripleStoreConfiguration vc, AnnotationValidationMode mode, int page, Set<String> editIds) {
		
		DatasetCatalog dcg = schemaService.asCatalog(pav.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		String annotatorFromClause = "";
		
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());
    	
    	AnnotationEditGroupContainer aegc = aegService.getContainer(currentUser, new SimpleObjectIdentifier(pav.getAnnotationEditGroupId()));
    	List<AnnotatorDocument> adocs = aegc.getAnnotators();

		Map<String, AnnotatorDocument> annotatorMap = new HashMap<>();
		for (AnnotatorDocument adoc : adocs) {
			annotatorMap.put(adoc.getUuid(), adoc);
			if (separateGraphs) {
				annotatorFromClause += "FROM <" + resourceVocabulary.getAnnotatorAsResource(adoc) + "> ";
			}
		}
		
		String annfilter = sparqlService.generatorFilter("v", aegc.getAnnotators().stream().map(adoc -> adoc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));
		if (!separateGraphs) {
			annotatorFromClause = "FROM <" + aegc.getObject().getTripleStoreGraph(resourceVocabulary) + "> ";
		} else {
			annotatorFromClause += "FROM <" + pav.getTripleStoreGraph(resourceVocabulary, separateGraphs) + "> ";
		}
    	
		AnnotationValuesContainer avc  = readPage(pav, onPropertyString, null, annfilter, vc, mode, page, fromClause, annotatorFromClause);
    	
		int validatedCount = 0;
		int addedCount = 0;
		int acceptedCount = 0;
		int rejectedCount = 0;
		int neutralCount = 0;
		
		for (Map.Entry<RDFTermHandler, ValueAnnotation> entry : avc.getValueMap().entrySet()) {
			RDFTerm aev = (RDFTerm)entry.getKey();
			ValueAnnotation nva = entry.getValue();
			
			Set<ObjectId> set = new HashSet<>();
			
			for (ValueAnnotationDetail vad : nva.getDetails()) {
				Optional<AnnotationEdit> editOpt = null;
				if (aev.getIri() != null) {
					editOpt = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAnnotationValueAndStartAndEnd(pav.getAnnotationEditGroupId(), aev.getIri(), vad.getValue(), vad.getStart() != null ? vad.getStart() : -1, vad.getEnd() != null ? vad.getEnd() : -1);
				} else {
					editOpt = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAnnotationValueAndStartAndEnd(pav.getAnnotationEditGroupId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype(), vad.getValue(), vad.getStart() != null ? vad.getStart() : -1, vad.getEnd() != null ? vad.getEnd() : -1 );
				}
				
				if (editOpt.isPresent()) {
					AnnotationEdit edit = editOpt.get();
					
					if (edit.getAcceptedByUserId().size() > 0 || edit.getRejectedByUserId().size() > 0) {
						validatedCount += vad.getCount();
					
						if (edit.getAcceptedByUserId().size() > edit.getRejectedByUserId().size()) {
							acceptedCount += vad.getCount();
						}  else if (edit.getAcceptedByUserId().size() < edit.getRejectedByUserId().size()) {
							rejectedCount += vad.getCount();
						} else {
							neutralCount += vad.getCount();
						}
					}
					
					set.add(edit.getId());
					
					editIds.remove(edit.getId().toString());
				}

			}

			List<AnnotationEdit> edits = null;
			if (aev.getIri() != null) {
				edits = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAdded(pav.getAnnotationEditGroupId(), aev.getIri());
			} else {
				edits = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAdded(pav.getAnnotationEditGroupId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype());
			}

			for (AnnotationEdit edit : edits) {
				if (set.contains(edit.getId())) {
					continue;
				}
				
				addedCount += nva.getCount();

				editIds.remove(edit.getId().toString());
			}
		}		

		PagedAnnotationValidationPage pavp = new PagedAnnotationValidationPage();
		pavp.setDatabaseId(database.getId());
		pavp.setPagedAnnotationValidationId(pav.getId());
		pavp.setAnnotationEditGroupId(pav.getAnnotationEditGroupId());
		pavp.setMode(mode);
		pavp.setPage(page);
		pavp.setAnnotationsCount(avc.getTotalAnnotationsCount());
		pavp.setValidatedCount(validatedCount);
		pavp.setUnvalidatedCount(avc.getTotalAnnotationsCount() - validatedCount);
		pavp.setAddedCount(addedCount);
		pavp.setAcceptedCount(acceptedCount);
		pavp.setRejectedCount(rejectedCount);
		pavp.setNeutralCount(neutralCount);
		pavp.setAssigned(false);

		return pavp;
	}

	




	/*
		A function to update the Page object and set isAssigned to a boolean value, after we have locked it.
		Overloaded.
	 */
	public UpdateLockedPageResponse updateLockedPageIsAssigned(int page, ObjectId pavId, AnnotationValidationMode mode, boolean isAssigned) {
		Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findByPagedAnnotationValidationIdAndModeAndPage(pavId, mode, page);
		if (!pavpOpt.isPresent()) {
//			System.out.println("not present");
			return new UpdateLockedPageResponse(false, null);
		}
		PagedAnnotationValidationPage pavp = pavpOpt.get();
		pavp.setAssigned(isAssigned);
		try {
			pavpRepository.save(pavp);
		}
		catch(Exception e) {
			e.printStackTrace();
			return new UpdateLockedPageResponse(true, null);
		}
		return new UpdateLockedPageResponse(false, pavp);
	}

	public boolean updateLockedPageIsAssigned(PagedAnnotationValidationPage pavp, boolean isAssigned) {
		pavp.setAssigned(isAssigned);
		try {
			pavpRepository.save(pavp);
		}
		catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public ObjectId lockPage(UserPrincipal currentUser, ObjectId pavId, int page, AnnotationValidationMode mode) {
		ObjectId locked = locksService.obtainLock(currentUser.getId(), pavId, page, mode);
		if(locked != null) {
			return locked;
		}
		else {
			return null;
		}
	}

	public boolean unlockPage(UserPrincipal currentUser, ObjectId pavId, int page, AnnotationValidationMode mode) {
		boolean unlocked = locksService.removeLock(currentUser.getId(), pavId, page, mode);
		if(unlocked) {
			return true;
		}
		else {
			return false;
		}
	}
	/*
		This function serves the UNANNOTADED_ONLY_SERIAL and ANNOTATED_ONLY_SERIAL PageRequestMode.
	 */

	public PagedAnnotationValidatationDataResponse getCurrent(UserPrincipal currentUser, TripleStoreConfiguration vc, ObjectId pavId, int currentPage, AnnotationValidationMode mode, PageRequestMode pgMode) {
		// try to lock current page to give it back to user
		PagedAnnotationValidatationDataResponse res;
		UpdateLockedPageResponse updateRes;
		ObjectId lockId;
		lockId = locksService.obtainLock(currentUser.getId(), pavId, currentPage, mode);
		if (lockId != null) {
			updateRes = updateLockedPageIsAssigned(currentPage, pavId, mode, true);
			if (updateRes.isError()) {
				return new PagedAnnotationValidatationDataResponse("INTERNAL_ERROR");
			}
			try {
				res = view(currentUser, vc, pavId, mode, currentPage, false);
			}
			catch(Exception e) {
				return new PagedAnnotationValidatationDataResponse("NO_PAGE_FOUND");
			}
			res.setLockId(lockId.toString());
			res.setErrorMessage("redirect");
			res.setFilter(pgMode.toString());
			return res;
		}
		else {
			// if locking current page fails, just throw an error...
			return new PagedAnnotationValidatationDataResponse("NO_PAGE_FOUND");
		}
	}

	public PagedAnnotationValidatationDataResponse determinePageSerial(UserPrincipal currentUser, ObjectId pavId, int currentPage, PageRequestMode mode, APIPagedAnnotationValidationController.NavigatePageMode navigation) {
			Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(pavId);
			if (!pavOpt.isPresent()) {
				return new PagedAnnotationValidatationDataResponse();
			}

			if(!locksService.checkForLockAndRemove(currentUser)) {
				return new PagedAnnotationValidatationDataResponse("Error on lock deletion. Try again.");
			}

			PagedAnnotationValidation pav = pavOpt.get();
			ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
			TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());


			//number of total pages that exist - must not bypass that!
			int unannotated_pages = pav.getNonAnnotatedPagesCount();
			int annotated_pages = pav.getAnnotatedPagesCount();
//			System.out.println(unannotated_pages+" "+annotated_pages);

			PagedAnnotationValidatationDataResponse res = null;
//			Optional<PagedAnnotationValidationPage> pavpOpt;
//			PagedAnnotationValidationPage pavp;
			UpdateLockedPageResponse updateRes;
			ObjectId lockId;

			//UNANNOTATED_ONLY modes
			if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SERIAL)) {
				/* check for the range of current_page, page_count.
				   If we get a lock, then check if the page exists to update isAssigned, else create it
				*/
				if (navigation.equals(APIPagedAnnotationValidationController.NavigatePageMode.RIGHT)) {
					for (int i = currentPage + 1; i <= unannotated_pages; i++) {
						lockId = lockPage(currentUser, pavId, i, AnnotationValidationMode.UNANNOTATED_ONLY);
						if (lockId != null) {
							updateLockedPageIsAssigned(i, pavId, AnnotationValidationMode.UNANNOTATED_ONLY, true);
							res = view(currentUser, vc, pavId, AnnotationValidationMode.UNANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							break;
						}
					}
				}
				else {
					for (int i = currentPage - 1; i > 0; i--) {
						lockId = lockPage(currentUser, pavId, i, AnnotationValidationMode.UNANNOTATED_ONLY);
						if (lockId != null) {
							updateLockedPageIsAssigned(i, pavId, AnnotationValidationMode.UNANNOTATED_ONLY, true);
							res = view(currentUser, vc, pavId, AnnotationValidationMode.UNANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							break;
						}
					}
				}
			}
			// ANNOTATED_ONLY modes
			else {
				if (navigation.equals(APIPagedAnnotationValidationController.NavigatePageMode.RIGHT)) {
					for (int i = currentPage + 1; i <= annotated_pages; i++) {
						lockId = lockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
						if (lockId != null) {
							updateRes = updateLockedPageIsAssigned(i, pavId, AnnotationValidationMode.ANNOTATED_ONLY, true);
							if (updateRes.isError()) {
								return new PagedAnnotationValidatationDataResponse("Error on server..");
							}

							//This checks for NOT_VALIDATED MODE
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED)) {
								if (updateRes.getPage() == null || updateRes.getPage().getValidatedCount() == 0) {
									res = view(currentUser, vc, pavId, AnnotationValidationMode.ANNOTATED_ONLY, i,false);
									res.setLockId(lockId.toString());
									res.setFilter("ANNOTATED_ONLY_NOT_VALIDATED");
									break;
								}
								else {
									unlockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
									continue;
								}
							}

							// NOT_COMPLETE mode
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)) {
								// If null then we have a new page, we go to next iteration.
								if (updateRes.getPage() == null) {
									unlockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
									continue;
								}
								else {
									if (updateRes.getPage().getValidatedCount() > 0 && updateRes.getPage().getUnvalidatedCount() > 0 )  {
										res = view(currentUser, vc, pavId, AnnotationValidationMode.ANNOTATED_ONLY, i,false);
										res.setLockId(lockId.toString());
										res.setFilter("ANNOTATED_ONLY_NOT_COMPLETE");
										break;
									}
									else {
										unlockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
										continue;
									}
								}
							}
							res = view(currentUser, vc, pavId, AnnotationValidationMode.ANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							res.setFilter("ANNOTATED_ONLY_SERIAL");
							break;
						}
					}
				}
				else {
					for (int i = currentPage - 1; i > 0; i--) {
						lockId = lockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
						if (lockId != null) {
							updateRes = updateLockedPageIsAssigned(i, pavId, AnnotationValidationMode.ANNOTATED_ONLY, true);
							if (updateRes.isError()) {
								return new PagedAnnotationValidatationDataResponse("Error on server..");
							}

							//This checks for NOT_VALIDATED MODE
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED)) {
								if (updateRes.getPage() == null || updateRes.getPage().getValidatedCount() == 0) {
									res = view(currentUser, vc, pavId, AnnotationValidationMode.ANNOTATED_ONLY, i, false);
									res.setLockId(lockId.toString());
									res.setFilter("ANNOTATED_ONLY_NOT_VALIDATED");
									break;
								}
								else {
									unlockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
									continue;
								}
							}

							// NOT_COMPLETE mode
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)) {
								// If null then we have a new page, we go to next iteration.
								if (updateRes.getPage() == null) {
									unlockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
									continue;
								}
								else {
									if (updateRes.getPage().getValidatedCount() > 0 && updateRes.getPage().getUnvalidatedCount() > 0 )  {
										res = view(currentUser, vc, pavId, AnnotationValidationMode.ANNOTATED_ONLY, i, false);
										res.setLockId(lockId.toString());
										res.setFilter("ANNOTATED_ONLY_NOT_COMPLETE");
										break;
									}
									else {
										unlockPage(currentUser, pavId, i, AnnotationValidationMode.ANNOTATED_ONLY);
										continue;
									}
								}
							}
							res = view(currentUser, vc, pavId, AnnotationValidationMode.ANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							res.setFilter("ANNOTATED_ONLY_SERIAL");
							break;
						}
					}
				}
			}
			// If we found a page, return it
			if (res != null) {
				return res;
			}
			// Else, try to lock again our "current page"
			else {
				// try to lock current page to give it back to user
				AnnotationValidationMode req;
				if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SERIAL)) {
					req = AnnotationValidationMode.UNANNOTATED_ONLY;
				}
				else {
					req = AnnotationValidationMode.ANNOTATED_ONLY;
				}
				res = getCurrent(currentUser, vc, pavId, currentPage, req, mode);
				return res;
			}
		}

	/*
		This function serves the UNANNOTATED_ONLY_SPECIFIC_PAGE and ANNOTATED_ONLY_SPECIFIC_PAGE PageRequestModes.
	 */
	public PagedAnnotationValidatationDataResponse getSpecificPage(UserPrincipal currentUser, ObjectId pavId, int requestedPage, PageRequestMode mode, int currentPage) {
		PagedAnnotationValidatationDataResponse res;
		ObjectId lockId;
		UpdateLockedPageResponse updateRes;

		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(pavId);
		if (!pavOpt.isPresent()) {
			return new PagedAnnotationValidatationDataResponse("PagedAnnotationValidation id is not present in database.");
		}
		PagedAnnotationValidation pav = pavOpt.get();

		ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
		TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		if(!locksService.checkForLockAndRemove(currentUser)) {
			return new PagedAnnotationValidatationDataResponse("Error on lock deletion. Try again.");
		}

		// check if requested page exists or not
		if ((mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE) && (requestedPage > pav.getNonAnnotatedPagesCount()))
			|| (mode.equals(PageRequestMode.ANNOTATED_ONLY_SPECIFIC_PAGE) && (requestedPage > pav.getAnnotatedPagesCount()))) {
			if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE)) {
				res = getCurrent(currentUser, vc, pavId, currentPage, AnnotationValidationMode.UNANNOTATED_ONLY, mode);
			}
			else {
				res = getCurrent(currentUser, vc, pavId, currentPage, AnnotationValidationMode.ANNOTATED_ONLY, mode);
			}
			return res;
		}

		if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE)) {
			lockId = locksService.obtainLock(currentUser.getId(), pavId, requestedPage, AnnotationValidationMode.UNANNOTATED_ONLY);

			if (lockId != null) {
				// If the page exists, mark it as assigned.
				updateRes = updateLockedPageIsAssigned(requestedPage, pavId, AnnotationValidationMode.UNANNOTATED_ONLY, true);
				if (updateRes.isError()) {
					return new PagedAnnotationValidatationDataResponse("Error on server..");
				}
				res = view(currentUser, vc, pavId, AnnotationValidationMode.UNANNOTATED_ONLY, requestedPage, false);
				res.setLockId(lockId.toString());
				return res;
			}
			else {
				// try to lock current page to give it back to user
				res = getCurrent(currentUser, vc, pavId, currentPage, AnnotationValidationMode.UNANNOTATED_ONLY, mode);
				return res;
			}
		}
		// ANNOTATED_ONLY_SPECIFIC_PAGE request
		else {
			lockId = locksService.obtainLock(currentUser.getId(), pavId, requestedPage, AnnotationValidationMode.ANNOTATED_ONLY);
			if (lockId != null) {
				updateRes = updateLockedPageIsAssigned(requestedPage, pavId, AnnotationValidationMode.ANNOTATED_ONLY, true);
				if (updateRes.isError()) {
					return new PagedAnnotationValidatationDataResponse("Error on server..");
				}
				res = view(currentUser, vc, pavId, AnnotationValidationMode.ANNOTATED_ONLY, requestedPage, false);
				res.setLockId(lockId.toString());
				return res;
			}
			else {
				res = getCurrent(currentUser, vc, pavId, currentPage, AnnotationValidationMode.ANNOTATED_ONLY, mode);
				return res;
			}
		}
	}


	
	public void stopValidation(PagedAnnotationValidationContainer pavc) throws Exception {

		pavc.update(ipavc -> {
			PagedAnnotationValidation ipav = ((PagedAnnotationValidationContainer)ipavc).getObject();
			
			ipav.setLifecycle(PagedAnnotationValidationState.STOPPED);
			ipav.setLifecycleCompletedAt(new Date());
			ipav.setComplete(true);
		});
		
	}
	
	@Async("pagedAnnotationValidationExecutor")
	public ListenableFuture<Date> resumeValidation(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		PagedAnnotationValidationContainer pavc = (PagedAnnotationValidationContainer)tdescr.getContainer();
		
		PagedAnnotationValidation pav = pavc.getObject();

		AnnotationEditGroup aeg = aegRepository.findById(pav.getAnnotationEditGroupId()).get();

		try {
			
			if (pav.getLifecycle() != PagedAnnotationValidationState.STARTED) {
				
				Date pavChange = pav.getLifecycleCompletedAt();
				Date aegChange = aeg.getLastPublicationStateChange();
				
//				System.out.println("PAV DATE " + pavChange);
//				System.out.println("AEG DATE " + aegChange);
				
				if (aegChange == null || pavChange == null || // legacy
						aegChange.after(pavChange)) {

					pav.setLifecycle(PagedAnnotationValidationState.RESUMING);
					pav.setResumingStartedAt(new Date());

					pm.sendMessage(new LifecycleNotificationObject(pavc), pav.getResumingStartedAt());
					
					pavRepository.save(pav);
					
					recreatePagedAnnotationValidation(pavc.getCurrentUser(), pav);
				}
	
				pav.setLifecycle(PagedAnnotationValidationState.STARTED);
				if (pav.getLifecycleStartedAt() == null) {
					pav.setLifecycleStartedAt(pav.getResumingStartedAt()); // for old entries with not date
				}
				pav.setResumingStartedAt(null);
				pav.setLifecycleCompletedAt(null);
				pav.setComplete(false);
				
				pavRepository.save(pav);

				pm.complete();
				
				pm.sendMessage(new LifecycleNotificationObject(pavc), pav.getLifecycleStartedAt());
			}
			
			return new AsyncResult<>(new Date());
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete();
			
			pav.setLifecycle(PagedAnnotationValidationState.RESUMING_FAILED);
			pav.setLifecycleCompletedAt(null);
			pav.setComplete(false);

			pavRepository.save(pav);

			pm.sendMessage(new LifecycleNotificationObject(pavc));			

//			return new AsyncResult<>(false);
			throw new TaskFailureException(ex, new Date());
		}
	}

	@Override
	@Async("pagedAnnotationValidationExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();
		
		PagedAnnotationValidationContainer pavc = (PagedAnnotationValidationContainer)tdescr.getContainer();

		try {
		    Date executeStart = new Date(System.currentTimeMillis());
			
			serviceUtils.clearExecution(pavc);

			pavc.update(ipavc -> {	
				MappingExecuteState ies = ((PagedAnnotationValidationContainer)ipavc).getExecuteState();

				ies.setExecuteState(MappingState.EXECUTING);
				ies.setExecuteStartedAt(executeStart);
				ies.setExecuteMessage(null);
				ies.setExecuteShards(0);
				ies.setCount(0);
				ies.clearMessages();
				ies.setExecuteMessage(null);
			});
		} catch (Exception ex) {
			throw new TaskFailureException(ex, new Date());
		}
		
		logger.info("Paged annotation validation " + pavc.getPrimaryId() + " execution starting");
		
		em.sendMessage(new ExecuteNotificationObject(pavc));
		
		try (FileSystemRDFOutputHandler outhandler = folderService.createExecutionRDFOutputHandler(pavc, shardSize)) {
			Executor exec = new Executor(outhandler, safeExecute);
//			exec.keepSubjects(true);
			
			TripleStoreConfiguration vc = pavc.getDatasetTripleStoreVirtuosoConfiguration();

			try {
				exec.setMonitor(em);
				
				String addD2rml = dataserviceFolder + env.getProperty("validator.paged-mark-add.d2rml");
				D2RMLModel addMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ addD2rml).getInputStream()) {
					String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
					str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
					addMapping = D2RMLModel.readFromString(str);
				}

				String markD2rml = dataserviceFolder + env.getProperty("validator.mark.d2rml");
				D2RMLModel markMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ markD2rml).getInputStream()) {
					String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
					str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
					markMapping = D2RMLModel.readFromString(str);
				}
				
				PagedAnnotationValidation pav = pavc.getObject();
				AnnotationEditGroupContainer aegc = aegService.getContainer(null, new SimpleObjectIdentifier(pav.getAnnotationEditGroupId()));
				
				String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());

				DatasetCatalog dcg = schemaService.asCatalog(pav.getDatasetUuid());
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
					annotatorFromClause += "FROM <" + pav.getTripleStoreGraph(resourceVocabulary, separateGraphs) + "> ";
				}
				
				em.createStructure(markMapping, outhandler);
				em.sendMessage(new ExecuteNotificationObject(pavc));

				for (AnnotationEdit edit :  annotationEditRepository.findByPagedAnnotationValidationId(pav.getId())) {
					if (edit.getAddedByUserId().size() > 0) {
						exec.keepSubjects(true);
						
						Map<String, Object> params = new HashMap<>();
						params.put("iirdfsource", vc.getSparqlEndpoint());
//						params.put("iigraph", resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString());
						params.put("iigraph", fromClause + " " + annotatorFromClause);
						params.put("iiproperty", onPropertyString);
						params.put("iivalue", edit.getOnValue().toString());
						params.put("iiannotation", edit.getAnnotationValue());
						params.put("iiconfidence", "1");
						params.put("iiannotator", pav.asResource(resourceVocabulary));
						params.put("validator", pav.asResource(resourceVocabulary));

						if (edit.getAcceptedByUserId().size() + 1 > edit.getRejectedByUserId().size()) { // + 1 is because user who added the annotation is considered to have approved it
							
							params.put("action", SOAVocabulary.Approve);
							
					    	String scope = edit.getMostAcceptedTargetAcceptProperty();
							params.put("scope", scope != null ? scope : "");
							
							exec.partialExecute(addMapping, params);
						
						} else if (edit.getAcceptedByUserId().size() + 1 < edit.getRejectedByUserId().size()) {
							
							params.put("action", SOAVocabulary.Delete);
							params.put("scope", "");
							
							exec.partialExecute(addMapping, params);
						}
					
					} else if (edit.getAddedByUserId().size() == 0) {
						exec.keepSubjects(false);
						
				    	String sparql = 
			    			"SELECT DISTINCT ?annotation " +
				    	    fromClause +
//				    	    "FROM NAMED <" + pav.getTripleStoreGraph(resourceVocabulary) + "> " +
				    	    annotatorFromClause +
				    	    (!separateGraphs && pav.getAsProperty() != null ? "FROM NAMED <" + resourceVocabulary.getAnnotationGraphResource() + "> " : "") +
	     			        "WHERE { " + 
//	    			        "  GRAPH <" + pav.getTripleStoreGraph(resourceVocabulary) + "> { " + 
	    			        "    ?annotation <" + OAVocabulary.hasBody + "> <" + edit.getAnnotationValue() + "> . " +
	    			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " +
						    annfilter +
	    			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
	    			        "    ?target <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " . " +
	    			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//		    			        (edit.getStart() != -1 ? " ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> " + edit.getStart() + " . " : " FILTER NOT EXISTS { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> " + edit.getStart() + " } . ") +
//		    			        (edit.getEnd() != -1 ? " ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> " + edit.getEnd() + " . " : " FILTER NOT EXISTS { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> " + edit.getEnd() + " } . ") +
                            (edit.getStart() != -1 ? " ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> " + edit.getStart() + " . " : " FILTER NOT EXISTS { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start  } . ") +
                            (edit.getEnd() != -1 ? " ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> " + edit.getEnd() + " . " : " FILTER NOT EXISTS { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . ") +
//	    		            "  } . " +	    			        
//	    			        "  GRAPH <" + resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString() + "> { " +
	    			        "    ?source " + onPropertyString + " " + edit.getOnValue().toString() + 
//	    			        "  } " +
	    			        (!separateGraphs ? "  GRAPH <" + pav.getTOCGraph(resourceVocabulary, separateGraphs) + "> { ?adocid <" + DCTVocabulary.hasPart + "> ?annotation . } " :"") +		    			        
	    			        "}";

//						System.out.println(edit.getId());
//				    	System.out.println(vc.getSparqlEndpoint());
//						System.out.println(QueryFactory.create(sparql));

						Map<String, Object> params = new HashMap<>();
						params.put("iirdfsource", vc.getSparqlEndpoint());
						params.put("iisparql", sparql);
						params.put("validator", pav.asResource(resourceVocabulary));

						if (edit.getAcceptedByUserId().size() < edit.getRejectedByUserId().size()) {
							params.put("action", SOAVocabulary.Delete);
							params.put("scope", "");
							
							exec.partialExecute(markMapping, params);
						
						} else if (edit.getAcceptedByUserId().size() > edit.getRejectedByUserId().size()) {
							params.put("action", SOAVocabulary.Approve);
							
					    	String scope = edit.getMostAcceptedTargetAcceptProperty();
							params.put("scope", scope != null ? scope : "");
							
							exec.partialExecute(markMapping, params);
						}
					}
				}	
				
				exec.completeExecution();
				
				Set<Resource> subjects = exec.getSubjects();
				serviceUtils.writeExecutionCatalog(subjects, pavc);

				em.complete();
		
				pavc.update(ipavc -> {			    
					MappingExecuteState ies = ((PagedAnnotationValidationContainer)ipavc).getExecuteState();

					ies.setExecuteState(MappingState.EXECUTED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(outhandler.getShards());
					ies.setCount(outhandler.getTotalItems());
					ies.setExecuteMessage(null);
	//				ies.setCount(subjects.size());
				});
		
				em.sendMessage(new ExecuteNotificationObject(pavc), outhandler.getTotalItems());

				logger.info("Paged annotation validation executed -- id: " + pavc.getPrimaryId() + ", shards: " + 0);

				try {
					serviceUtils.zipExecution(pavc, outhandler.getShards());
				} catch (Exception ex) {
					ex.printStackTrace();
					
					logger.info("Zipping paged annotation validation execution failed -- id: " + pavc.getPrimaryId());
				}
				
				return new AsyncResult<>(em.getCompletedAt());
				
			} catch (Exception ex) {
				ex.printStackTrace();
				
				logger.info("Paged validation failed -- id: " + pavc.getPrimaryId());

				throw ex;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			
			em.complete(ex);

			try {
				pavc.update(ipavc -> {			    
					MappingExecuteState ies = ((PagedAnnotationValidationContainer)ipavc).getObject().getExecuteState(fileSystemConfiguration.getId());
	
					ies.failDo(em);
					ies.setExecuteShards(0);
					ies.setSparqlExecuteShards(0);
					ies.setCount(0);
					ies.setSparqlCount(0);
				});
			} catch (Exception iex) {
				iex.printStackTrace();
				throw new TaskFailureException(iex, em.getCompletedAt());
			}
			
			em.sendMessage(new ExecuteNotificationObject(pavc));

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
	public ListPage<PagedAnnotationValidation> getAllByUser(ObjectId userId, Pageable page) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public ListPage<PagedAnnotationValidation> getAllByUser(List<AnnotationEditGroup> dataset, ObjectId userId, Pageable page) {
		// TODO Auto-generated method stub
		return null;
	}


}
