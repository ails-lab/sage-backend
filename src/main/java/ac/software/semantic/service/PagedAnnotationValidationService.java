package ac.software.semantic.service;

import java.io.File;
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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.APIPagedAnnotationValidationController;
import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.controller.APIPagedAnnotationValidationController.PageRequestMode;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
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

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.ExecuteDocument;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingExecutePublishDocument;
import ac.software.semantic.model.PagedValidationOption;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.constants.AnnotationEditType;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.model.constants.SortingType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PagedAnnotationValidationState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.DatasetProgressResponse;
import ac.software.semantic.payload.LifecycleNotificationObject;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.payload.PagedAnnotationValidatationDataResponse;
import ac.software.semantic.payload.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.ProgressResponse;
import ac.software.semantic.payload.UpdateLockedPageResponse;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueAnnotationReference;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.PagedAnnotationValidationPageLocksRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationPageRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.ANNOTATED_ONLY;
import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.UNANNOTATED_ONLY;

@Service
public class PagedAnnotationValidationService implements ExecutingPublishingService {

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
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	private VocabularyService vocabularyService;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private Environment env;

	@Autowired
	private AnnotatorDocumentRepository annotatorDocumentRepository;

    @Autowired
    private AnnotationEditGroupRepository aegRepository;

    @Autowired
    private AnnotationEditGroupService aegService;
    
	@Autowired
	private PagedAnnotationValidationRepository pavRepository;

	@Autowired
	private PagedAnnotationValidationPageRepository pavpRepository;

	@Autowired
	private PagedAnnotationValidationPageLocksRepository pavplRepository;

	@Autowired
	private AnnotationEditRepository annotationEditRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private PagedAnnotationValidationPageLocksService locksService;

	@Autowired
	private TripleStore tripleStore;
	
	@Autowired
	private ResourceLoader resourceLoader;
	
    @Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private FolderService folderService;

	@Autowired
	private ServiceUtils serviceUtils;
	
    @Autowired
    @Qualifier("paged-validations")
	Map<String, PagedValidationOption> pavOptions;
    
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer vocc;
	
	
	public Class<? extends ObjectContainer> getContainerClass() {
		return PagedAnnotationValidationContainer.class;
	}
	
	public class PagedAnnotationValidationContainer extends ObjectContainer implements PublishableContainer, ExecutableContainer, AnnotationValidationContainer {
		private ObjectId pavId;
		private ObjectId aegId;
		
		private PagedAnnotationValidation pav;
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
			this.containerFileSystemConfiguration = fileSystemConfiguration;
			
			this.pavId = pav.getId();
			this.currentUser = currentUser;
			
			this.pav = pav;
			loadAnnotationEditGroup();
		}

		@Override
		protected void load() {
			Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(pavId);

			if (!pavOpt.isPresent()) {
				return;
			}

			pav = pavOpt.get();
		}
		
		private void loadAnnotationEditGroup() {
			this.aegId = pav.getAnnotationEditGroupId();

			Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(pav.getAnnotationEditGroupId());

			if (!aegOpt.isPresent()) {
				return;
			}
		
			aeg = aegOpt.get();
		}

		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(pav.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			dataset = datasetOpt.get();
		}

		@Override
		public ExecuteDocument getExecuteDocument() {
			return getPagedAnnotationValidation();
		}
		
		@Override
		public MappingExecutePublishDocument<MappingPublishState> getPublishDocument() {
			return getPagedAnnotationValidation();
		}
		
		@Override
		public AnnotationValidation getAnnotationValidation() {
			return getPagedAnnotationValidation();
		}
		
		public PagedAnnotationValidation getPagedAnnotationValidation() {
			return pav;
		}

		public void setPagedAnnotationValidation(PagedAnnotationValidation pav) {
			this.pav = pav;
		}

		public ObjectId getPagedAnnotationValidationId() {
			return pavId;
		}

		public void setPagedAnnotationValidationId(ObjectId pavId) {
			this.pavId = pavId;
		}

		@Override
		public void save(MongoUpdateInterface mui) throws Exception {
			synchronized (saveSyncString()) { 
				load();
			
				mui.update(this);
				
				pavRepository.save(pav);
			}
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return pavId;
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
		public PagedAnnotationValidationResponse asResponse() {
			TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();
			
			return modelMapper.pagedAnnotationValidation2PagedAnnotationValidationResponse(vc, pav);
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
			return getContainerFileSystemConfiguration().getId().toString() + ":" + getPagedAnnotationValidationId().toString();
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
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}		

		@Override
		public String syncString() {
			return ("SYNC:" + containerString() + ":" + pavId).intern();
		}

		@Override
		public String saveSyncString() {
			return ("SYNC:SAVE:" + containerString() + ":" + pavId).intern();
		}

		@Override
		public boolean delete() throws Exception {
			throw new Exception("Not implemented");
		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return syncString(id);
	}
	
	public static String syncString(String id) {
		return ("SYNC:" + PagedAnnotationValidationContainer.class.getName() + ":" + id).intern();
	}
	
	@Override
	public PagedAnnotationValidationContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		PagedAnnotationValidationContainer pavc = new PagedAnnotationValidationContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());
//		System.out.println(">> " + mc.mappingDocument + " " + mc.getDataset());
		if (pavc.pav == null || pavc.getDataset() == null) {
			return null;
		} else {
			return pavc;
		}
	}
	
	public PagedAnnotationValidationContainer getContainer(UserPrincipal currentUser, PagedAnnotationValidation pav) {
		PagedAnnotationValidationContainer pavc = new PagedAnnotationValidationContainer(currentUser, pav);
		if (pavc.pav == null || pavc.getDataset() == null) {
			return null;
		} else {
			return pavc;
		}
	}
	
	private int getAnnotatedValueCount(String datasetUri, String onPropertyPath, String asProperty, String annfilter, TripleStoreConfiguration vc, String fromClause) {
		String annotatedCountSparql = 
				"SELECT (COUNT(DISTINCT ?value) AS ?count)" +
		        fromClause + 
		        "FROM NAMED <" + asProperty + "> " + 
		        "WHERE { " + 
		        "    ?s " + onPropertyPath + " ?value  " + 
				"  GRAPH <" + asProperty + "> { " + 
		        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
				"     <" + OAVocabulary.hasTarget + "> ?r . " + 
			    annfilter + 
		        "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyPath + "\" ; " + 
		        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } " +
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
	
	private int getNonAnnotatedValueCount(String datasetUri, String onPropertyPath, String asProperty, String annfilter, TripleStoreConfiguration vc, String fromClause) {
		String nonAnnotatedCountSparql = 
				"SELECT (COUNT(DISTINCT ?value) AS ?count)" +
		        fromClause + 
		        "FROM NAMED <" + asProperty + "> " + 
		        "WHERE { " + 
		        "    ?s " + onPropertyPath + " ?value  " + 
				"  FILTER NOT EXISTS { GRAPH <" + asProperty + "> { " + 
		        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
				"     <" + OAVocabulary.hasTarget + "> ?r . " + 
			    annfilter + 			        
		        "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyPath + "\" ; " + 
		        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } " +
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
	
	private int getAnnotationsCount(String datasetUri, String onPropertyPath, String asProperty, String annfilter, TripleStoreConfiguration vc, String fromClause) {
		String annotationsCountSparql = 
				"SELECT (COUNT(?v) AS ?count)" + 
		        fromClause + 
		        "FROM NAMED <" + asProperty + "> " + 
		        "WHERE { " + 
		        "    ?s " + onPropertyPath + " ?value  " + 
				"  GRAPH <" + asProperty + "> { " + 
		        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
				"     <" + OAVocabulary.hasTarget + "> ?r . " + 
			    annfilter + 			        
		        "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyPath + "\" ; " + 
		        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } " + 
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
	
	private String buildQuery(PagedValidationOption pvo, String datasetUri, String onPropertyString, String asProperty, List<String> annotatorUuids, AnnotationValidationRequest mode, Map<String, SortingType> pavOrdering,int page, String fromClause) {
		String query = null;
		if (mode == AnnotationValidationRequest.ALL) {
			query = pvo.getAllQuery();
		} else if (mode == ANNOTATED_ONLY) {
			query = pvo.getAnnotatedQuery();
		} else if (mode == UNANNOTATED_ONLY) {
			query = pvo.getUnannotatedQuery();
		}
		
		query = query.replaceAll("\\{@@PAV_DATASET_URI@@\\}",fromClause).
				replaceAll("\\{@@PAV_AS_PROPERTY@@\\}", asProperty).
				replaceAll("\\{@@PAV_ON_PROPERTY_STRING@@\\}", onPropertyString).
				replaceAll("\\{@@PAV_GENERATORS@@\\}", aegService.annotatorFilter("v", annotatorUuids)).
				concat("LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1));
		
		for (Map.Entry<String, SortingType> entry : pavOrdering.entrySet()) {
			query = query.replaceAll("\\{@@PAV_SORTING_" + entry.getKey() + "@@\\}", entry.getValue().toString());
		}
		
//		System.out.println("PAV:buildQuery");
//		System.out.println(QueryFactory.create(query, Syntax.syntaxSPARQL_11));
		
		return query;
				
	}
	
	public List<ValueCount> getValuesForPage(TripleStoreConfiguration vc, String datasetUri, String pavMode, String onPropertyString, String asProperty, List<String> annotatorUuids, AnnotationValidationRequest mode, int page, String fromClause) {
//		String annfilter = aegService.annotatorFilter("v", annotatorUuids);
//		
//		String graph = 
//			"GRAPH <" + asProperty + "> { " +
//            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
//		    "     <" + OAVocabulary.hasTarget + "> ?r . " + 
//            annfilter +
//            "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
//		    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
//            "     <" + OAVocabulary.hasSource + "> ?s . " + 
//		    " }";
//		
//		if (mode == AnnotationValidationRequest.ALL) {
////			graph +=  " OPTIONAL { " + graph + " }  "; 
//		} else if (mode == ANNOTATED_ONLY) {
//			graph = " FILTER EXISTS { " + graph + " }  ";
//		} else if (mode == UNANNOTATED_ONLY) {
//			graph = " FILTER NOT EXISTS { " + graph + " }  "; 
//		}
//		
//		// should also filter out URI values here but this would spoil pagination due to previous bug.
//		String sparql = 
//            "SELECT ?value ?valueCount WHERE { " +
//			"  SELECT ?value (count(*) AS ?valueCount)" +
//	        "  WHERE { " + 
//		    "    GRAPH <" + datasetUri + "> { " + 
//	        "      ?s " + onPropertyString + " ?value } " + 
//		         graph +
//		    "    FILTER (isLiteral(?value)) " +		         
//		    "  } " +
//			"  GROUP BY ?value " + 
//			"  ORDER BY desc(?valueCount) ?value } " + 
// 		    "LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1);
		
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
		
		String sparql = buildQuery(pavOptions.get(pavBase), datasetUri, onPropertyString, asProperty, annotatorUuids, mode, pavOrderings, page, fromClause);

    	List<ValueCount> values = new ArrayList<>();
    	
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
    		
    		ResultSet rs = qe.execSelect();
    		
    		while (rs.hasNext()) {
    			QuerySolution qs = rs.next();
    			RDFNode value = qs.get("value");
    			int count = qs.get("valueCount").asLiteral().getInt(); //valueCount is the number a value appears (not of annotations on value) wrong!
    			
    			values.add(new ValueCount(value, count));
    		}
    	}
    	
    	return values;
		
	}
	
	private class AnnotationValuesContainer {
		private Map<AnnotationEditValue, ValueAnnotation> valueMap;
		private int totalAnnotationsCount;
		
		public Map<AnnotationEditValue, ValueAnnotation> getValueMap() {
			return valueMap;
		}
		
		public void setValueMap(Map<AnnotationEditValue, ValueAnnotation> valueMap) {
			this.valueMap = valueMap;
		}
		
		public int getTotalAnnotationsCount() {
			return totalAnnotationsCount;
		}
		
		public void setTotalAnnotationsCount(int totalAnnotationsCount) {
			this.totalAnnotationsCount = totalAnnotationsCount;
		}
	}
	
	private AnnotationValuesContainer readPage(String datasetUri, String pavMode, String onPropertyString, String asProperty, List<String> annotatorDocumentUuids, String annfilter, TripleStoreConfiguration vc, AnnotationValidationRequest mode, int page, String fromClause) {

		List<ValueCount> values = getValuesForPage(vc, datasetUri, pavMode, onPropertyString, asProperty, annotatorDocumentUuids, mode, page, fromClause);
		
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

    	StringBuffer sb = new StringBuffer();
    	for (ValueCount vct : values) {
			AnnotationEditValue aev = null;
    		
    		if (vct.getValue().isLiteral()) {
				Literal l = vct.getValue().asLiteral();
				String lf = l.getLexicalForm();
				
				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
				sb.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString());
	    		sb.append(" ");
				
				aev = new AnnotationEditValue(vct.getValue().asLiteral());
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
		    "  { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
		    "  { ?v <" + OAVocabulary.hasBody + "> [ " + 
		    "  a <" + OWLTime.DateTimeInterval + "> ; " + 
		    "  <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
		    "  <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " +
		    "  OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } ." + 
		    "  OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } "; 
//		    "} ";
		    		
		String sparqlRef = null;
		String sparqlGen = null;
		if (mode == ANNOTATED_ONLY) {
			sparql = 
//					"SELECT distinct ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(*) AS ?count) (GROUP_CONCAT(?ref ; separator=\"@@\") AS ?refConcat) " + 
					"SELECT distinct ?value ?t ?ie ?start ?end (AVG(?sc) AS ?score) (count(*) AS ?count) " +
			        fromClause + 
			        "FROM NAMED <" + asProperty + ">" + 
		            "WHERE { " + 
		            "  ?s " + onPropertyString + " ?value  " + 
		            "  GRAPH <" + asProperty + "> { " + 
		                 graph + " . " +
		            "    OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } " + 
		            "  } " +  
//                  "  OPTIONAL { ?r <" + DCTVocabulary.isReferencedBy + ">  ?ref } . " +                       
                    "  VALUES ?value { " + valueString  + " } " +     
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end ";// +
//					"ORDER BY DESC(?count) ?value ?start ?end";
			
			sparqlRef = "SELECT distinct ?value ?t ?ie ?start ?end (count(*) AS ?count) ?ref " + 
			        fromClause + 
			        "FROM NAMED <" + asProperty + ">" + 
					"WHERE { " + 
		            "  ?s " + onPropertyString + " ?value  " + 
		            "  GRAPH <" + asProperty + "> { " + 
		                  graph + " . " + 
		               " ?r <" + DCTVocabulary.isReferencedBy + ">  ?ref } " +  
                    "  VALUES ?value { " + valueString  + " } " +     
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end ?ref ";
			
			sparqlGen = "SELECT distinct ?value ?t ?ie ?start ?end ?generator " + 
			        fromClause + 
			        "FROM NAMED <" + asProperty + ">" + 
		            "WHERE { " + 
		            "  ?s " + onPropertyString + " ?value  " + 
		            "  GRAPH <" + asProperty + "> { " + graph + " } " +  
                    "  VALUES ?value { " + valueString  + " } " +     
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end ?generator ";
		} else if (mode == UNANNOTATED_ONLY) {
			sparql = 
					"SELECT distinct ?value (count(*) AS ?count) " + 
			        fromClause + 
			        "FROM NAMED <" + asProperty + ">" + 
			        "WHERE { " + 
					"    ?s " + onPropertyString + " ?value  " + 
		            "  FILTER NOT EXISTS { " + 
					"    GRAPH <" + asProperty + "> { " + 
					"      ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "         <" + OAVocabulary.hasTarget + "> ?r . " + 
		                   annfilter +
					"      ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
					"         <" + SOAVocabulary.onValue + "> ?value ; " + 
					"         <" + OAVocabulary.hasSource + "> ?s  } } " +
					"  VALUES ?value { " + valueString  + " } " +
					"} " +
					"GROUP BY ?value ";// +
//			        "ORDER BY DESC(?count) ?value ";
		}    	
		
//		System.out.println("readPage");
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
//    	System.out.println(QueryFactory.create(sparqlRef, Syntax.syntaxSPARQL_11));
    	
//		System.out.println(sb);
//		System.out.println(mode);
		
		int totalAnnotationsCount = 0;
		if (valueString.length() > 0) {
			
//			System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
			
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					RDFNode value = sol.get("value");
					
					String ann = sol.get("t") != null ? sol.get("t").toString() : null;
					String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;

					Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
					Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
					Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
					
//					String refConcat = sol.get("refConcat") != null ? sol.get("refConcat").toString() : null;
					
					int count = sol.get("count").asLiteral().getInt();
//					int refCount = sol.get("refCount").asLiteral().getInt();
					
					AnnotationEditValue aev = null;
					if (value.isResource()) {
						aev = new AnnotationEditValue(value.asResource());
					} else if (value.isLiteral()) {
						aev = new AnnotationEditValue(value.asLiteral());
					}
					
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
						
						RDFNode value = sol.get("value");
						
						String ann = sol.get("t") != null ? sol.get("t").toString() : null;
						String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
	
						Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
						Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
						
						String ref = sol.get("ref").toString();
						
						int count = sol.get("count").asLiteral().getInt();
	//					int refCount = sol.get("refCount").asLiteral().getInt();
						
						AnnotationEditValue aev = null;
						if (value.isResource()) {
							aev = new AnnotationEditValue(value.asResource());
						} else if (value.isLiteral()) {
							aev = new AnnotationEditValue(value.asLiteral());
						}
						
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
				
				
				//get default targets per annotation
				Map<String, String> defaultTargetPropertyMap = new HashMap<>();
				
//				System.out.println(QueryFactory.create(sparqlGen, Syntax.syntaxSPARQL_11));
				
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparqlGen, Syntax.syntaxSPARQL_11))) {
					
					ResultSet rs = qe.execSelect();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						
						RDFNode value = sol.get("value");
						
						String ann = sol.get("t") != null ? sol.get("t").toString() : null;
						String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
	
						Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
						Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
						
						String generator = sol.get("generator").toString();
						
						AnnotationEditValue aev = null;
						if (value.isResource()) {
							aev = new AnnotationEditValue(value.asResource());
						} else if (value.isLiteral()) {
							aev = new AnnotationEditValue(value.asLiteral());
						}
						
						ValueAnnotation va = res.get(aev);
						if (va != null) {
							for (ValueAnnotationDetail vad : va.getDetails()) {
								if (eq(vad.getValue(), ann) && eq(vad.getValue2(), ie) && eq(vad.getStart(), start) && eq(vad.getEnd(), end)) {

									String dp = null;
									if (!defaultTargetPropertyMap.containsKey(generator)) {
										AnnotatorDocument adoc = annotatorDocumentRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(generator)).get();
										
										dp = adoc.getDefaultTarget();
										defaultTargetPropertyMap.put(generator, dp);
									} else {
										dp = defaultTargetPropertyMap.get(generator);
									}
									
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
	
	public PagedAnnotationValidation create(UserPrincipal currentUser, String aegId, String name, String mode) {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));
		if (!aegOpt.isPresent()) {
			return null;
		}		

		// temporary: do not create more that one pavs for an aeg;
		List<PagedAnnotationValidation> pavList = pavRepository.findByAnnotationEditGroupId(new ObjectId(aegId));
		if (pavList.size() > 0) {
			return null;
		}		

		PagedAnnotationValidation pav = null;
		
		try {

			Dataset ds = datasetRepository.findByUuid(aegOpt.get().getDatasetUuid()).get();
			TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			pav = createPagedAnnotationValidation(currentUser, aegOpt.get(), vc);
			
			pav.setUuid(UUID.randomUUID().toString());
			pav.setComplete(false);
			pav.setLifecycle(PagedAnnotationValidationState.STARTED);
			pav.setLifecycleStartedAt(new Date());
			pav.setName(name);
			pav.setMode(mode);
			pav.setUpdatedAt(new Date());
			pav = pavRepository.save(pav);
	
			return pav;
		} catch (Exception ex) {
			ex.printStackTrace();
			
			if (pav != null) {
				pavRepository.deleteById(pav.getId());
			}
			
			return null;
		}
	}
	
	public PagedAnnotationValidation updatePagedAnnotationValidation(PagedAnnotationValidationContainer pavc, String name, String mode) {

		PagedAnnotationValidation pav = pavc.getPagedAnnotationValidation();

		pav.setName(name);
		
		if (!mode.equals(pav.getMode())) {
			pav.setUpdatedAt(new Date());
		}
		
		pav.setMode(mode);
		
		pav = pavRepository.save(pav);
	
		return pav;
	}
	
	// should be updated for other values sorting
	public void recreatePagedAnnotationValidation(UserPrincipal currentUser, PagedAnnotationValidation oldPav) throws Exception {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(oldPav.getAnnotationEditGroupId());

		ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(aegOpt.get().getDatasetUuid()).get();
		TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		PagedAnnotationValidation newPav = createPagedAnnotationValidation(currentUser, aegOpt.get(), vc);
		
//			newPav.setUuid(oldPav.getUuid());
//			newPav.setComplete(oldPav.isComplete());
//			newPav.setName(oldPav.getName());
		
//			oldPav.setState(PagedAnnotationValidationState.RESUMING);
//			oldPav.setLastStateChange(new Date());

		oldPav.setAnnotatorDocumentUuid(newPav.getAnnotatorDocumentUuid());
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
					PagedAnnotationValidationPage pavp = repaginateSimulationView(oldPav, vc, AnnotationValidationRequest.ANNOTATED_ONLY, j, editIds);
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
					PagedAnnotationValidationPage pavp = repaginateSimulationView(oldPav, vc, AnnotationValidationRequest.UNANNOTATED_ONLY, j, editIds);
					if (before > editIds.size()) {
						logger.info("Created: " + pavp.getMode() + " PAGE " + pavp.getPage() + " : " + pavp.getAnnotationsCount() + " " + pavp.getValidatedCount() + " " + pavp.getUnvalidatedCount() + " " + pavp.getAddedCount() + " / " + editIds.size());
						pavpRepository.save(pavp);
					}
				}					
			}

			logger.info("Created updated annotation validation pages for " + oldPav.getId() + ". " +  (editIds.size() > 0 ? edits.size() + "have been missed." : ""));
		}
			
	}
	
	private PagedAnnotationValidation createPagedAnnotationValidation(UserPrincipal currentUser, AnnotationEditGroup aeg, TripleStoreConfiguration vc) throws Exception {

		List<String> annotatorUuids = annotatorDocumentRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList());
		String annfilter = aegService.annotatorFilter("v", annotatorUuids);

		String datasetUri = resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString();
		DatasetCatalog dcg = schemaService.asCatalog(aeg.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		
		String onPropertyPath = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());

		logger.info("Creating paged annotation validation on " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ".");
		
		int annotatedValueCount = getAnnotatedValueCount(datasetUri, onPropertyPath, aeg.getAsProperty(), annfilter, vc, fromClause);
		int annotatedPages = annotatedValueCount / pageSize + (annotatedValueCount % pageSize > 0 ? 1 : 0);

		int nonAnnotatedValueCount = getNonAnnotatedValueCount(datasetUri, onPropertyPath, aeg.getAsProperty(), annfilter, vc, fromClause);
		int nonAnnotatedPages = nonAnnotatedValueCount / pageSize + (nonAnnotatedValueCount % pageSize > 0 ? 1 : 0);

		int annotationsCount = getAnnotationsCount(datasetUri, onPropertyPath, aeg.getAsProperty(), annfilter, vc, fromClause);

		PagedAnnotationValidation pav = new PagedAnnotationValidation();

		pav.setUserId(new ObjectId(currentUser.getId()));
		pav.setAnnotatorDocumentUuid(annotatorUuids);
		pav.setAnnotationEditGroupId(aeg.getId());
		pav.setDatasetUuid(aeg.getDatasetUuid());
		pav.setDatabaseId(database.getId());
		pav.setOnProperty(aeg.getOnProperty());
		pav.setAsProperty(aeg.getAsProperty());

		pav.setPageSize(pageSize);
		pav.setAnnotationsCount(annotationsCount);
		pav.setAnnotatedPagesCount(annotatedPages);
		pav.setNonAnnotatedPagesCount(nonAnnotatedPages);

		logger.info("Created paged annotation validation on " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ": values=" + annotatedValueCount + "/" + nonAnnotatedValueCount + ", pages=" + annotatedPages + "/" + nonAnnotatedPages);

		return pav;
	}
	
	public PagedAnnotationValidatationDataResponse view(UserPrincipal currentUser, TripleStoreConfiguration vc, String pavId, AnnotationValidationRequest mode, int page, boolean ignoreAdded) {
		
		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
		if (!pavOpt.isPresent()) {
			return new PagedAnnotationValidatationDataResponse();
		}		
		
		PagedAnnotationValidation pav = pavOpt.get();

		String datasetUri = resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString();
		DatasetCatalog dcg = schemaService.asCatalog(pav.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		
		
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());
		String annfilter = aegService.annotatorFilter("v", pav.getAnnotatorDocumentUuid());

		AnnotationValuesContainer avc = readPage(datasetUri, pav.getMode(), onPropertyString, pav.getAsProperty(), pav.getAnnotatorDocumentUuid(), annfilter, vc, mode, page, fromClause);
		
		ObjectId uid = new ObjectId(currentUser.getId());
		
		int validatedCount = 0;
		int addedCount = 0;
		int acceptedCount = 0;
		int rejectedCount = 0;
		int neutralCount = 0;
		
		Map<AnnotationEditValue, ValueAnnotation> res = avc.getValueMap();
		
		for (Map.Entry<AnnotationEditValue, ValueAnnotation> entry : res.entrySet()) {
			AnnotationEditValue aev = entry.getKey();
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
		
		if (mode == ANNOTATED_ONLY) {
			pr.setTotalPages(pav.getAnnotatedPagesCount());
		} else {
			pr.setTotalPages(pav.getNonAnnotatedPagesCount());
		}
		
		return pr;
    } 

	private PagedAnnotationValidationPage repaginateSimulationView(PagedAnnotationValidation pav, TripleStoreConfiguration vc, AnnotationValidationRequest mode, int page, Set<String> editIds) {
		
		String datasetUri = resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString();
		DatasetCatalog dcg = schemaService.asCatalog(pav.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());
		String annfilter = aegService.annotatorFilter("v", pav.getAnnotatorDocumentUuid());

		AnnotationValuesContainer avc  = readPage(datasetUri, pav.getMode(), onPropertyString, pav.getAsProperty(), pav.getAnnotatorDocumentUuid(), annfilter, vc, mode, page, fromClause);
    	
		int validatedCount = 0;
		int addedCount = 0;
		int acceptedCount = 0;
		int rejectedCount = 0;
		int neutralCount = 0;
		
		for (Map.Entry<AnnotationEditValue, ValueAnnotation> entry : avc.getValueMap().entrySet()) {
			AnnotationEditValue aev = entry.getKey();
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
	public UpdateLockedPageResponse updateLockedPageIsAssigned(int page, String pavId, AnnotationValidationRequest mode, boolean isAssigned) {
		Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findByPagedAnnotationValidationIdAndModeAndPage(new ObjectId(pavId), mode, page);
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

	public ObjectId lockPage(UserPrincipal currentUser, String pavId, int page, AnnotationValidationRequest mode) {
		ObjectId locked = locksService.obtainLock(currentUser.getId(), pavId, page, mode);
		if(locked != null) {
			return locked;
		}
		else {
			return null;
		}
	}

	public boolean unlockPage(UserPrincipal currentUser, String pavId, int page, AnnotationValidationRequest mode) {
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

	public PagedAnnotationValidatationDataResponse getCurrent(UserPrincipal currentUser, TripleStoreConfiguration vc, String pavId, int currentPage, AnnotationValidationRequest mode, PageRequestMode pgMode) {
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

	public PagedAnnotationValidatationDataResponse determinePageSerial(UserPrincipal currentUser, String pavId, int currentPage, PageRequestMode mode, APIPagedAnnotationValidationController.NavigatePageMode navigation) {
			Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
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
						lockId = lockPage(currentUser, pavId, i, UNANNOTATED_ONLY);
						if (lockId != null) {
							updateLockedPageIsAssigned(i, pavId, UNANNOTATED_ONLY, true);
							res = view(currentUser, vc, pavId, UNANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							break;
						}
					}
				}
				else {
					for (int i = currentPage - 1; i > 0; i--) {
						lockId = lockPage(currentUser, pavId, i, UNANNOTATED_ONLY);
						if (lockId != null) {
							updateLockedPageIsAssigned(i, pavId, UNANNOTATED_ONLY, true);
							res = view(currentUser, vc, pavId, UNANNOTATED_ONLY, i, false);
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
						lockId = lockPage(currentUser, pavId, i, ANNOTATED_ONLY);
						if (lockId != null) {
							updateRes = updateLockedPageIsAssigned(i, pavId, ANNOTATED_ONLY, true);
							if (updateRes.isError()) {
								return new PagedAnnotationValidatationDataResponse("Error on server..");
							}

							//This checks for NOT_VALIDATED MODE
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED)) {
								if (updateRes.getPage() == null || updateRes.getPage().getValidatedCount() == 0) {
									res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i,false);
									res.setLockId(lockId.toString());
									res.setFilter("ANNOTATED_ONLY_NOT_VALIDATED");
									break;
								}
								else {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
							}

							// NOT_COMPLETE mode
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)) {
								// If null then we have a new page, we go to next iteration.
								if (updateRes.getPage() == null) {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
								else {
									if (updateRes.getPage().getValidatedCount() > 0 && updateRes.getPage().getUnvalidatedCount() > 0 )  {
										res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i,false);
										res.setLockId(lockId.toString());
										res.setFilter("ANNOTATED_ONLY_NOT_COMPLETE");
										break;
									}
									else {
										unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
										continue;
									}
								}
							}
							res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							res.setFilter("ANNOTATED_ONLY_SERIAL");
							break;
						}
					}
				}
				else {
					for (int i = currentPage - 1; i > 0; i--) {
						lockId = lockPage(currentUser, pavId, i, ANNOTATED_ONLY);
						if (lockId != null) {
							updateRes = updateLockedPageIsAssigned(i, pavId, ANNOTATED_ONLY, true);
							if (updateRes.isError()) {
								return new PagedAnnotationValidatationDataResponse("Error on server..");
							}

							//This checks for NOT_VALIDATED MODE
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED)) {
								if (updateRes.getPage() == null || updateRes.getPage().getValidatedCount() == 0) {
									res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
									res.setLockId(lockId.toString());
									res.setFilter("ANNOTATED_ONLY_NOT_VALIDATED");
									break;
								}
								else {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
							}

							// NOT_COMPLETE mode
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)) {
								// If null then we have a new page, we go to next iteration.
								if (updateRes.getPage() == null) {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
								else {
									if (updateRes.getPage().getValidatedCount() > 0 && updateRes.getPage().getUnvalidatedCount() > 0 )  {
										res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
										res.setLockId(lockId.toString());
										res.setFilter("ANNOTATED_ONLY_NOT_COMPLETE");
										break;
									}
									else {
										unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
										continue;
									}
								}
							}
							res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
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
				AnnotationValidationRequest req;
				if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SERIAL)) {
					req = UNANNOTATED_ONLY;
				}
				else {
					req = ANNOTATED_ONLY;
				}
				res = getCurrent(currentUser, vc, pavId, currentPage, req, mode);
				return res;
			}
		}

	/*
		This function serves the UNANNOTATED_ONLY_SPECIFIC_PAGE and ANNOTATED_ONLY_SPECIFIC_PAGE PageRequestModes.
	 */
	public PagedAnnotationValidatationDataResponse getSpecificPage(UserPrincipal currentUser, String pavId, int requestedPage, PageRequestMode mode, int currentPage) {
		PagedAnnotationValidatationDataResponse res;
		ObjectId lockId;
		UpdateLockedPageResponse updateRes;

		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
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
				res = getCurrent(currentUser, vc, pavId, currentPage, UNANNOTATED_ONLY, mode);
			}
			else {
				res = getCurrent(currentUser, vc, pavId, currentPage, ANNOTATED_ONLY, mode);
			}
			return res;
		}

		if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE)) {
			lockId = locksService.obtainLock(currentUser.getId(), pavId, requestedPage, UNANNOTATED_ONLY);

			if (lockId != null) {
				// If the page exists, mark it as assigned.
				updateRes = updateLockedPageIsAssigned(requestedPage, pavId, UNANNOTATED_ONLY, true);
				if (updateRes.isError()) {
					return new PagedAnnotationValidatationDataResponse("Error on server..");
				}
				res = view(currentUser, vc, pavId, UNANNOTATED_ONLY, requestedPage, false);
				res.setLockId(lockId.toString());
				return res;
			}
			else {
				// try to lock current page to give it back to user
				res = getCurrent(currentUser, vc, pavId, currentPage, UNANNOTATED_ONLY, mode);
				return res;
			}
		}
		// ANNOTATED_ONLY_SPECIFIC_PAGE request
		else {
			lockId = locksService.obtainLock(currentUser.getId(), pavId, requestedPage, ANNOTATED_ONLY);
			if (lockId != null) {
				updateRes = updateLockedPageIsAssigned(requestedPage, pavId, ANNOTATED_ONLY, true);
				if (updateRes.isError()) {
					return new PagedAnnotationValidatationDataResponse("Error on server..");
				}
				res = view(currentUser, vc, pavId, ANNOTATED_ONLY, requestedPage, false);
				res.setLockId(lockId.toString());
				return res;
			}
			else {
				res = getCurrent(currentUser, vc, pavId, currentPage, ANNOTATED_ONLY, mode);
				return res;
			}
		}
	}


	public void determinePage(UserPrincipal currentUser, String pavId, PageRequestMode mode, int currentPage) {
	}

	public ProgressResponse getProgress(UserPrincipal currentUser, String pavId) {
		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
		int totalAnnotations = 0;
		int totalValidations = 0;
		int totalAdded = 0;
		int totalAccepted = 0;
		int totalRejected = 0;
		int totalNeutral = 0;

		PagedAnnotationValidation pav;
		if (pavOpt.isPresent()) {
			pav = pavOpt.get();
			totalAnnotations = pav.getAnnotationsCount();
		}
		else {
			return null;
		}
		List<PagedAnnotationValidationPage> pages = pavpRepository.findByPagedAnnotationValidationIdAndMode(new ObjectId(pavId), ANNOTATED_ONLY);
		for(PagedAnnotationValidationPage page: pages) {
			totalValidations += page.getValidatedCount();
			totalAdded += page.getAddedCount();
			totalAccepted += page.getAcceptedCount();
			totalRejected += page.getRejectedCount();
			totalNeutral += page.getNeutralCount();
		}

		pages = pavpRepository.findByPagedAnnotationValidationIdAndMode(new ObjectId(pavId), UNANNOTATED_ONLY);
		for(PagedAnnotationValidationPage page: pages) {
			totalAdded += page.getAddedCount();
		}

		ProgressResponse res = new ProgressResponse();
		res.setTotalAnnotations(totalAnnotations);
		res.setTotalValidations(totalValidations);
		res.setTotalAdded(totalAdded);

		res.setTotalAccepted(totalAccepted);
		res.setTotalRejected(totalRejected);
		res.setTotalNeutral(totalNeutral);

		return res;
	}

	
	public void stopValidation(PagedAnnotationValidationContainer pavc) throws Exception {

		pavc.save(ipavc -> {
			PagedAnnotationValidation ipav = ((PagedAnnotationValidationContainer)ipavc).getPagedAnnotationValidation();
			
			ipav.setLifecycle(PagedAnnotationValidationState.STOPPED);
			ipav.setLifecycleCompletedAt(new Date());
			ipav.setComplete(true);
		});
		
	}
	
	@Async("pagedAnnotationValidationExecutor")
	public ListenableFuture<Date> resumeValidation(TaskDescription tdescr, PagedAnnotationValidationContainer pavc, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		PagedAnnotationValidation pav = pavc.getPagedAnnotationValidation();

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

					pm.sendMessage(new LifecycleNotificationObject(pav.getLifecycle(), pavc), pav.getResumingStartedAt());
					
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
				
				pm.sendMessage(new LifecycleNotificationObject(pav.getLifecycle(), pavc), pav.getLifecycleStartedAt());
			}
			
			return new AsyncResult<>(new Date());
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete();
			
			pav.setLifecycle(PagedAnnotationValidationState.RESUMING_FAILED);
			pav.setLifecycleCompletedAt(null);
			pav.setComplete(false);

			pavRepository.save(pav);

			pm.sendMessage(new LifecycleNotificationObject(pav.getLifecycle(), pavc));			

//			return new AsyncResult<>(false);
			throw new TaskFailureException(ex, new Date());
		}
	}

	public List<DatasetProgressResponse> getDatasetProgress(UserPrincipal currentUser, String datasetUuid) {
		List<DatasetProgressResponse> res = new ArrayList<>();
		DatasetProgressResponse dataRes;
		ProgressResponse progRes;

		List<PagedAnnotationValidation> datasetValidations = pavRepository.findByDatasetUuid(datasetUuid);
		for (PagedAnnotationValidation pav : datasetValidations) {
			dataRes = new DatasetProgressResponse();
			dataRes.setValidationId(pav.getId().toString());
//			dataRes.setPropertyName(val.getOnPropertyAsString());
			dataRes.setPropertyName(vocabularyService.onPathStringListAsPrettyString(pav.getOnProperty()));
			dataRes.setPropertyPath(PathElement.onPathElementListAsStringListInverse(pav.getOnProperty(), vocc));
			dataRes.setAsProperty(pav.getAsProperty());

			progRes = getProgress(currentUser, pav.getId().toString());
			
//			System.out.println(datasetUuid+ " " +  pav.getId() + " " + vocabularyService.onPathStringListAsPrettyString(pav.getOnProperty()) + " " + progRes.getTotalValidations() + " " + progRes.getTotalAnnotations() );

			//round the result
			try {
				BigDecimal bd = BigDecimal.valueOf((1.0 * progRes.getTotalValidations() / progRes.getTotalAnnotations()) * 100);
				bd = bd.setScale(2, RoundingMode.HALF_UP);
				dataRes.setProgress(bd.doubleValue());
			} catch(Exception e) {
				dataRes.setProgress(0);
			}
			
			dataRes.setTotalAdded(progRes.getTotalAdded());
			dataRes.setTotalAnnotations(progRes.getTotalAnnotations());
			dataRes.setTotalValidations(progRes.getTotalValidations());
			dataRes.setTotalAccepted(progRes.getTotalAccepted());
			dataRes.setTotalRejected(progRes.getTotalRejected());
			dataRes.setTotalNeutral(progRes.getTotalNeutral());
			
			dataRes.setAnnotatedPagesCount(pav.getAnnotatedPagesCount());
			
			dataRes.setLocked(pavplRepository.findByAnnotationEditGroupId(pav.getAnnotationEditGroupId()).size() > 0);
			
			boolean published = false;
        	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site        	
	        	PublishState ppss = pav.checkPublishState(vc.getId()); // @FIXED FROM .getDatabaseId();
	        	if (ppss != null) {
	    	    	published = ppss.getPublishState() == DatasetState.PUBLISHED;
	        	}
        	}
        	
        	dataRes.setActive(pav.getLifecycle() == PagedAnnotationValidationState.STARTED && !published);
        	
			res.add(dataRes);
		}

		return res;
	}
	

	@Override
	@Async("pagedAnnotationValidationExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();
		
		PagedAnnotationValidationContainer pavc = (PagedAnnotationValidationContainer)tdescr.getContainer();
		TripleStoreConfiguration vc = pavc.getDatasetTripleStoreVirtuosoConfiguration();

		try {
		    Date executeStart = new Date(System.currentTimeMillis());
			
			serviceUtils.clearExecution(pavc);

			pavc.save(ipavc -> {	
				MappingExecuteState ies = ((PagedAnnotationValidationContainer)ipavc).getPagedAnnotationValidation().getExecuteState(fileSystemConfiguration.getId());

				ies.setExecuteState(MappingState.EXECUTING);
				ies.setExecuteStartedAt(executeStart);
				ies.setExecuteShards(0);
				ies.setCount(0);
				ies.clearMessages();
			});
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new TaskFailureException(ex, new Date());
		}

		
		logger.info("Paged Annotation Validation " + pavc.getPrimaryId() + " starting");
		
		em.sendMessage(new ExecuteNotificationObject(pavc));
		
		try (FileSystemRDFOutputHandler outhandler = folderService.createAnnotationValidationExecutionRDFOutputHandler(pavc, shardSize)) {
			Executor exec = new Executor(outhandler, safeExecute);
//			exec.keepSubjects(true);
			
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
				
//				String approveD2rml = dataserviceFolder + env.getProperty("validator.mark-approve.d2rml");
//				D2RMLModel approveMapping = null;
//				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ approveD2rml).getInputStream()) {
//					String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
//					str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
//					
//					approveMapping = D2RMLModel.readFromString(str);
//				}

				PagedAnnotationValidation pav = pavc.getPagedAnnotationValidation();
				
				String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());
				String annfilter = aegService.annotatorFilter("annotation", pav.getAnnotatorDocumentUuid());

				em.createStructure(markMapping, outhandler);
				
				em.sendMessage(new ExecuteNotificationObject(pavc));

				for (AnnotationEdit edit :  annotationEditRepository.findByPagedAnnotationValidationId(pav.getId())) {
				
					if (edit.getAddedByUserId().size() > 0) {
						
						Map<String, Object> params = new HashMap<>();
						params.put("iirdfsource", vc.getSparqlEndpoint());
						params.put("iigraph", resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString());
						params.put("iiproperty", onPropertyString);
						params.put("iivalue", edit.getOnValue().toString());
						params.put("iiannotation", edit.getAnnotationValue());
						params.put("iiconfidence", "1");
						params.put("iiannotator", resourceVocabulary.getAnnotationValidatorAsResource(pav.getUuid()));
						params.put("validator", resourceVocabulary.getAnnotationValidatorAsResource(pav.getUuid()));

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
				    	String sparql = 
			    			"SELECT ?annotation " +
	     			        "WHERE { " + 
	    			        "  GRAPH <" + pav.getAsProperty() + "> { " + 
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
	    		            "  } . " +	    			        
	    			        "  GRAPH <" + resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString() + "> { " +
	    			        "    ?source " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
	                        "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " +		    			        
	    			        "}";

//						System.out.println(edit.getId());
//				    	System.out.println(vc.getSparqlEndpoint());
//						System.out.println(QueryFactory.create(sparql));

						Map<String, Object> params = new HashMap<>();
						params.put("iirdfsource", vc.getSparqlEndpoint());
						params.put("iisparql", sparql);
						params.put("validator", resourceVocabulary.getAnnotationValidatorAsResource(pav.getUuid()));

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
				
				em.complete();
		
				pavc.save(ipavc -> {			    
					MappingExecuteState ies = ((PagedAnnotationValidationContainer)ipavc).getPagedAnnotationValidation().getExecuteState(fileSystemConfiguration.getId());

					ies.setExecuteState(MappingState.EXECUTED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(outhandler.getShards());
					ies.setCount(outhandler.getTotalItems());
	//				ies.setCount(subjects.size());
				});
		
				em.sendMessage(new ExecuteNotificationObject(pavc), outhandler.getTotalItems());

				logger.info("Paged validation executed -- id: " + pavc.getPrimaryId() + ", shards: " + 0);

				try {
					serviceUtils.zipExecution(pavc, outhandler.getShards());
				} catch (Exception ex) {
					ex.printStackTrace();
					
					logger.info("Zipping paged validation execution failed -- id: " + pavc.getPrimaryId());
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
				pavc.save(ipavc -> {			    
					MappingExecuteState ies = ((PagedAnnotationValidationContainer)ipavc).getPagedAnnotationValidation().getExecuteState(fileSystemConfiguration.getId());
	
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
			
//		} finally {
//			try {
//				if (em != null) {
//					em.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
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
	
//	@Override
//	@Async("publishExecutor")
//	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
//		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();	
//
//		PagedAnnotationValidationContainer pavc = (PagedAnnotationValidationContainer)tdescr.getContainer();
//		PagedAnnotationValidation pav = pavc.getPagedAnnotationValidation();
//		
//		TripleStoreConfiguration vc = pavc.getDatasetTripleStoreVirtuosoConfiguration();
//		
//		MappingPublishState ps = pav.getPublishState(vc.getId()); // @FIXED FROM .getDatabaseId();
//		
//		try {
//			ps.startDo(pm);
//			
//			pavc.save();
//
//			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), pavc));
//			
//			tripleStore.publish(vc, pavc);
//	    	
//			pm.forceComplete();
//			
//			ps.completeDo(pm);
//			ps.setExecute(pav.getExecuteState(fileSystemConfiguration.getId()));
//			
//			pavc.save();
//			
//			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), pavc));
//		
//			return new AsyncResult<>(pm.getCompletedAt());
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//
//			pm.forceComplete(ex);
//			
//	        ps.failDo(pm);
//			
//	        pavc.save();
//			
//			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), pavc));
//			
//			throw new TaskFailureException(ex, pm.getCompletedAt());
//		}
//	}

//	@Override
//	@Async("publishExecutor")
//	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
//		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();	
//
//		PagedAnnotationValidationContainer pavc = (PagedAnnotationValidationContainer)tdescr.getContainer();
//		PagedAnnotationValidation pav = pavc.getPagedAnnotationValidation();
//		Dataset dataset = pavc.getDataset();
//		UserPrincipal currentUser = pavc.getCurrentUser();
//		
//		TripleStoreConfiguration vc = pavc.getDatasetTripleStoreVirtuosoConfiguration();
//		
//		MappingPublishState ps = pav.getPublishState(vc.getId()); // @FIXED FROM .getDatabaseId();
//		
//		try {
//			ps.startUndo(pm);
//	
//			pavRepository.save(pav);
//
//			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), pavc));
//			
//			tripleStore.unpublish(vc, pavc);
//			
//			pav.removePublishState(ps);
//			
//			MappingExecuteState es = pav.getExecuteState(fileSystemConfiguration.getId());
//			MappingExecuteState pes = ps.getExecute();
//			if (es != null && pes != null && es.getExecuteStartedAt().compareTo(pes.getExecuteStartedAt()) != 0) {
//				clearExecution(currentUser, dataset, pav, pes);
//			}
//			
//			pavRepository.save(pav);
//	    	
//			pm.forceComplete();
//			pm.sendMessage(new PublishNotificationObject(DatasetState.UNPUBLISHED, pavc));
//		
//			return new AsyncResult<>(pm.getCompletedAt());
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//
//			pm.forceComplete(ex);
//
//			ps.failUndo(pm);
//			
//			pavRepository.save(pav);
//			
//			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), pavc));
//			
//			throw new TaskFailureException(ex, pm.getCompletedAt());
//		}
//	}
	
}
