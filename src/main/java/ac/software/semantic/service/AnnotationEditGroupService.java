package ac.software.semantic.service;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditGroupSearch;
import ac.software.semantic.model.AnnotationEditGroupSearchField;
import ac.software.semantic.model.AnnotatorContext;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.Pagination;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.constants.type.AnnotationValidationMode;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.index.IndexKeyMetadata;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.bson.types.ObjectId;
import org.eclipse.rdf4j.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdApi;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.config.AppConfiguration.JenaRDF2JSONLD;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.payload.request.AnnotationEditGroupUpdateRequest;
import ac.software.semantic.payload.response.AnnotationEditGroupResponse;
import ac.software.semantic.payload.response.FilterAnnotationValidationResponse;
import ac.software.semantic.payload.response.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.response.ResultCount;
import ac.software.semantic.payload.response.modifier.PagedAnnotationValidationResponseModifier;
import ac.software.semantic.payload.response.modifier.ResponseModifier;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.repository.root.DataServiceRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationUtils.AnnotationUtilsContainer;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.PrototypeService.PrototypeContainer;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import edu.ntua.isci.ac.common.db.rdf.RDF4JRemoteSelectIterator;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoConstructIterator;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.FOAFVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class AnnotationEditGroupService implements EnclosedCreatableService<AnnotationEditGroup, AnnotationEditGroupResponse, AnnotationEditGroupUpdateRequest, Dataset>,
                                                   EnclosingService<AnnotationEditGroup, AnnotationEditGroupResponse, Dataset> {

	private Logger logger = LoggerFactory.getLogger(AnnotationEditGroupService.class);

    @Autowired
    @Qualifier("database")
    private Database database;

    @Autowired
    @Qualifier("annotators")
    private Map<String, DataService> annotators;

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;
			
	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

	@Value("${virtuoso.graphs.separate:#{true}}")
	private boolean separateGraphs;
	
	@Autowired
	private APIUtils apiUtils;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;

	@Lazy
	@Autowired
	private AnnotatorService annotatorService;

	@Autowired
	private AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	private PagedAnnotationValidationRepository pavRepository;

    @Lazy
	@Autowired
	private PagedAnnotationValidationService pavService;

	@Autowired
	private FilterAnnotationValidationRepository favRepository;

    @Lazy
	@Autowired
	private FilterAnnotationValidationService favService;

	@Autowired
	DataServiceRepository dsRepository;

	@Autowired
	private SchemaService schemaService;

	@Lazy
	@Autowired
	private DatasetService datasetService;

	@Lazy
	@Autowired
	private PrototypeService prototypeService;
	
	@Lazy
	@Autowired
	private AnnotatorService annotatorsService;

	@Autowired
	private SPARQLService sparqlService;

	@Autowired
	@Qualifier("annotation-jsonld-context")
    private Map<String, Object> annotationContext;
	
	@Autowired
	private ServiceUtils serviceUtils;

	@Autowired
	private VocabularyService vocabularyService;
	
	@Autowired
	private SparqlQueryService sparqlQueryService;

	@Autowired
	private AnnotationUtils annotationUtils;
	
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;
	
	
	private static PagedAnnotationValidationResponseModifier pavModifier = ResponseModifier.createModifier(PagedAnnotationValidationResponseModifier.class, Arrays.asList(new String[] {"base", "states", "dates", "vocabularies"}));

	@Override
	public Class<? extends EnclosedObjectContainer<AnnotationEditGroup,AnnotationEditGroupResponse,Dataset>> getContainerClass() {
		return AnnotationEditGroupContainer.class;
	}
	
	@Override
	public DocumentRepository<AnnotationEditGroup> getRepository() {
		return aegRepository;
	}
	
	public class AnnotationEditGroupContainer extends EnclosedObjectContainer<AnnotationEditGroup,AnnotationEditGroupResponse,Dataset> 
    	                                      implements //EnclosingContainer<AnnotationEditGroup>,
    	                                                 UpdatableContainer<AnnotationEditGroup, AnnotationEditGroupResponse,AnnotationEditGroupUpdateRequest>,
    	                                                 AnnotationContainerBase {

		private ObjectId aegId;
		
		public AnnotationEditGroupContainer(UserPrincipal currentUser, ObjectId aegId) {
			this.aegId = aegId;
			this.currentUser = currentUser;
			
			load();
		}
		
		private AnnotationEditGroupContainer(UserPrincipal currentUser, AnnotationEditGroup aeg) {
			this(currentUser, aeg, null);
		}
		
		private AnnotationEditGroupContainer(UserPrincipal currentUser, AnnotationEditGroup aeg, Dataset dataset) {
			this.currentUser = currentUser;
			
			this.aegId = aeg.getId();
			this.object = aeg;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return aegId;
		}
		
		@Override
		public DocumentRepository<AnnotationEditGroup> getRepository() {
			return aegRepository;
		}
		
		@Override
		public AnnotationEditGroupService getService() {
			return AnnotationEditGroupService.this;
		}
		
		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}

		
//		@Override
//		public void load() {
//			Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(aegId);
//
//			if (!aegOpt.isPresent()) {
//				return;
//			}
//
//			object = aegOpt.get();
//		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(object.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			dataset = datasetOpt.get();
		}
		
		@Override
		public AnnotationEditGroup update(AnnotationEditGroupUpdateRequest ur) throws Exception {

			return update(iac -> {
				AnnotationEditGroup ac = iac.getObject();
				ac.setAutoexportable(ur.isAutoexportable());
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			// TODO Auto-generated method stub
			return false;
		}

//		@Override
//		public AnnotationEditGroup getEnclosingObject() {
//			return getObject();
//		}

		@Override
		public String getDescription() {
			return object.getUuid();
		}

		private TripleStoreConfiguration getDatasetTripleStoreVirtuosoConfiguration() {
			ProcessStateContainer psv = getEnclosingObject().getCurrentPublishState(virtuosoConfigurations.values());
			if (psv != null) {
				return psv.getTripleStoreConfiguration();
			} else {
				return null;
			}
		}
		
		@Override
		public AnnotationEditGroupResponse asResponse() {
			
	    	AnnotationEditGroupResponse response = new AnnotationEditGroupResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setDatasetUuid(object.getDatasetUuid());
	    	response.setAsProperty(object.getAsProperty());
	    	if (object.getOnProperty() != null) {
	    		response.setOnProperty(PathElement.onPathElementListAsStringListInverse(object.getOnProperty(), null));
	    	} else {
	    		response.setOnClass(object.getOnClass());
	    	}
	    	response.setAutoexportable(object.getAutoexportable() != null ? object.getAutoexportable() : false);
	    	
	    	response.setTag(object.getTag());
	    	response.setKeys(object.getKeys());
//	    	response.setSparqlClause(object.getSparqlClause());

	    	List<PagedAnnotationValidationResponse> pavList = new ArrayList<>();
	    	for (PagedAnnotationValidation pav: pavRepository.findByAnnotationEditGroupId(object.getId())) {
	    		pavList.add(pavService.getContainer(currentUser, pav, object).asResponse(pavModifier));
	    	}
	    	
	    	List<FilterAnnotationValidationResponse> favList = new ArrayList<>();
	    	for (FilterAnnotationValidation fav: favRepository.findByAnnotationEditGroupId(object.getId())) {
	    		favList.add(favService.getContainer(currentUser, fav, object).asResponse());
	    	}
	    	
	    	response.setPagedAnnotationValidations(pavList);
	    	response.setFilterAnnotationValidations(favList);

	    	boolean published = false;
	    	if (object.getAnnotatorId() != null) {
		    	for (ObjectId adocId : object.getAnnotatorId()) {
		    		AnnotatorContainer ac = annotatorService.getContainer(currentUser, new SimpleObjectIdentifier(adocId));
		    		if (ac.isPublished()) {
		    			published = true;
		    			break;
		    		}
		    	}
		    	
		    }
	    	
	    	//legacy
	    	if (!published) {
	    		for (AnnotatorDocument adoc : annotatorRepository.findByAnnotatorEditGroupId(object.getId())) {
		    		AnnotatorContainer ac = annotatorService.getContainer(currentUser, new SimpleObjectIdentifier(adoc.getId()));
		    		if (ac.isPublished()) {
		    			published = true;
		    			break;
		    		}
	    		}
	    	}


			response.setPublished(published);

	        return response;
		}

		@Override
		protected String localSynchronizationString() {
			return (":" + getObject().getId().toString()).intern();
		}

		@Override
		public TaskDescription getActiveTask(TaskType type) {
			// TODO Auto-generated method stub
			return null;
		}
		
		public List<AnnotatorDocument> getAnnotators() {
			List<AnnotatorDocument> adocs;
			if (object.getAsProperty() != null) { //legacy
				adocs = annotatorRepository.findByAnnotatorEditGroupId(object.getId());
			} else {
				adocs = annotatorRepository.findByIdIn(object.getAnnotatorId());
			}
			
			return adocs;
		}
		
		@Override
		public List<String> getOnProperty() {
			return object.getOnProperty();
		}

	}
	
	public List<AnnotationEditGroup> getAnnotationEditGroups(Dataset dataset) {
		return aegRepository.findByDatasetId(dataset.getId());
	}

	public void exportAnnotations(ObjectId id, SerializationType format, 
			boolean onlyReviewed, boolean onlyNonRejected, boolean onlyFresh, boolean created, boolean creator, boolean confidence, boolean scope, boolean selector,  
			OutputStream out) throws Exception {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(id);
		if (!aegOpt.isPresent()) {
			return ;
		}		
		
		AnnotationEditGroup aeg = aegOpt.get();
		
		DatasetCatalog dcg = schemaService.asCatalog(aeg.getDatasetUuid());
//		String fromClause = schemaService.buildFromClause(dcg);
		
		TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		List<String> generatorIds = new ArrayList<>();
		if (aeg.getAsProperty() != null) { //legacy
			generatorIds.addAll(annotatorRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(doc -> doc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));
		} else {
			generatorIds.addAll(annotatorRepository.findByIdIn(aeg.getAnnotatorId()).stream().map(doc -> doc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));			
		}
		
		generatorIds.addAll(pavRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> doc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));
		generatorIds.addAll(favRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> doc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));

		String annfilter = sparqlQueryService.generatorFilter("annotation", generatorIds);
		
//		System.out.println(annfilter);
		
		String valFilter = "";
		if (onlyNonRejected) {
			valFilter = " FILTER NOT EXISTS { ?annotation <" + SOAVocabulary.hasValidation + "> [ <" + SOAVocabulary.action + "> <" + SOAVocabulary.Delete + "> ] } . ";
		}
	
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
    	int pathLength = PathElement.onPathLength(aeg.getOnProperty());
		String onPropertyStringAsPath = PathElement.onPathStringListAsMiddleRDFPath(aeg.getOnProperty());
    	if (pathLength == 1) {
    		onPropertyStringAsPath = onPropertyStringAsPath.substring(1, onPropertyStringAsPath.length() - 1);
    	}

    	boolean state = true;

    	// validated first 
    	String whereSparqlValidated = 
//    			fromClause +
//    			"FROM NAMED <" + aeg.getAsProperty() + "> " +
    			"WHERE { " +
//         		"SELECT DISTINCT ?annotation ?created ?body ?confidence ?target ?value ?valueStr ?valueLang ?start ?end ?source ?validationAction ?scope ?generator WHERE { " +
//    			"  ?source " + onPropertyString + " ?value " + 
    			"  GRAPH <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> { " + 
    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
    			     annfilter + 
    			     valFilter +
    			     (created ? " OPTIONAL { ?annotation <" + DCTVocabulary.created + "> ?created . } " : "") +
    			     (confidence ? " OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?confidence . } " : "") +
    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
    			"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
        		"    ?target <" + SOAVocabulary.onValue + "> ?value . BIND (str(?value) AS ?valueStr) . BIND (lang(?value) AS ?valueLang) " +
        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
        		     (onlyFresh ? "FILTER NOT EXISTS  { ?target <" + DCTVocabulary.isReferencedBy + "> ?reference } . " : "") +
        		"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " + 
        		"    ?validation <" + SOAVocabulary.action + "> ?validationAction  . " +
//        		     (creator ? " OPTIONAL { ?annotation <" + DCTVocabulary.creator + "> [ a ?ct ] } .  BIND (IF(bound(?ct), ?ct, <" + ASVocabulary.Application + ">) AS ?creatorType) . " : "") + // manuall created annotations have <http://purl.org/dc/terms/creator> [ a       <http://xmlns.com/foaf/0.1/Person> ] ;
        		     (selector ? " OPTIONAL { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?startV . BIND (STRDT(STR(?startV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?start) } " : "") +
        		     (selector ? " OPTIONAL { ?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?endV . BIND (STRDT(STR(?endV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?end) } " : "") +
        		     (scope ? " OPTIONAL { ?validation <" + OAVocabulary.hasScope + "> ?scope } . " : "") +
//        		     (scope ? " BIND (" + (defaultScope != null ? "IF(bound(?scope), ?scope, <" + defaultScope + ">)" : "?scope") + " AS ?aScope) " : "") +
        		"  } } " 
//                + " } "
        		;
    	
//    	String whereSparqlValidatedGroup = 
//    			"WHERE { " +
//    	        "SELECT ?source ?body ?validationAction ?scope (SAMPLE(?annotation) AS ?sampleAnnotation) (AVG(?confidence) AS ?avgConfidence) { " +
//    			"  GRAPH <" + resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString() + "> { " + 
//    			"    ?source " + onPropertyString + " ?value }  " +
//    			"  GRAPH <" + aeg.getAsProperty() + "> { " + 
//    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
//    			     annfilter + 
//    			     valFilter +
//    			     (confidence ? " OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?confidence . } " : "") +
//    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
//    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
//    			"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
//        		"    ?target <" + SOAVocabulary.onValue + "> ?value . BIND (str(?value) AS ?valueStr) . BIND (lang(?value) AS ?valueLang) " +
//        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//        		     (onlyFresh ? "FILTER NOT EXISTS  { ?target <" + DCTVocabulary.isReferencedBy + "> ?reference } . " : "") +
//        		"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " + 
//        		"    ?validation <" + SOAVocabulary.action + "> ?validationAction  . " +
//        		     (scope ? " OPTIONAL { ?validation <" + OAVocabulary.hasScope + "> ?scope } . " : "") +
//        		"  } } GROUP BY ?source ?body ?validationAction ?scope }";
    	
    	String whereSparqlUnvalidated = 
//    			fromClause +
//    			"FROM NAMED <" + aeg.getAsProperty() + "> " +
    			"WHERE { " +
//        		"SELECT DISTINCT ?annotation ?created ?body ?confidence ?target ?value ?valueStr ?valueLang ?start ?end ?source ?generator WHERE { " +
//    			"  ?source " + onPropertyString + " ?value " + 
    			"  GRAPH <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> { " + 
    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
    			     annfilter + 
    			     valFilter +
    			     (created ? " OPTIONAL { ?annotation <" + DCTVocabulary.created + "> ?created . } " : "") +
    			     (confidence ? " OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?confidence . } " : "") +
    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
        		"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
        		"    ?target <" + SOAVocabulary.onValue + "> ?value . BIND (str(?value) AS ?valueStr) . BIND (lang(?value) AS ?valueLang) " +
        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
        		     (onlyFresh ? "FILTER NOT EXISTS  { ?target <" + DCTVocabulary.isReferencedBy + "> ?reference } . " : "") +
        		"    FILTER NOT EXISTS { ?annotation <" + SOAVocabulary.hasValidation + "> ?val . } " +
//        		     (creator ? " OPTIONAL { ?annotation <" + DCTVocabulary.creator + "> [ a ?ct ] } .  BIND (IF(bound(?ct), ?ct, <" + ASVocabulary.Application + ">) AS ?creatorType) . " : "") +
        		     (selector ? " OPTIONAL {?target <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?startV . BIND (STRDT(STR(?startV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?start) } " : "") +
        		     (selector ? " OPTIONAL {?target <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?endV . BIND (STRDT(STR(?endV), <http://www.w3.org/2001/XMLSchema#nonNegativeInteger>) AS ?end) } " : "") +
//        		     (scope ? " OPTIONAL {?annotation <" + OAVocabulary.hasScope + "> ?scope } . " : "") +
//        		     (scope ? " BIND (" + (defaultScope != null ? "IF(bound(?scope), ?scope, <" + defaultScope + ">)" : "?scope") + " AS ?aScope) " : "") +
        		"  } } " 
//        		+ " }"
        		;
        		

    	if (format == SerializationType.TTL || format == SerializationType.NT || format == SerializationType.RDF_XML || format == SerializationType.JSONLD) {
    		
    		String sparqlValidated = 
			"  CONSTRUCT { " + 
			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
			     (created ? " ?annotation <" + DCTVocabulary.created + "> ?created . " : "") +
//			     (creator ? "?annotation <" + DCTVocabulary.creator + "> [ a ?creatorType ] . " : "" ) +
			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . " +
  		         (confidence ? " ?annotation <" + SOAVocabulary.confidence + "> ?confidence . " : "") +
			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    		     (selector ?  "?target <" + OAVocabulary.hasSelector + "> [" + (pathLength == 1 ? " a <" + SOAVocabulary.RDFPropertySelector + "> ; <" + SOAVocabulary.property + "> " + onPropertyStringAsPath + "" : " a <" + SOAVocabulary.RDFPathSelector + "> ; <" + SOAVocabulary.rdfPath + "> \"" + onPropertyStringAsPath + "\"" ) + " ; <" + SOAVocabulary.destination + "> [ a <" + SOAVocabulary.Literal + "> ; <" + RDFVocabulary.value + "> ?valueStr ; <" + DCTVocabulary.language + "> ?valueLang ] ; <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start ;  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end ] . " : "") +
    		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//    		     (state ? "?annotation <" + OAVocabulary.hasState + "> [ a <" + SOAVocabulary.ValidationState + "> ; <" + RDFVocabulary.value + "> ?validationState ] . " : "" ) +
    		     (state ? "?annotation <" + SOAVocabulary.hasReview + "> [ a <" + SOAVocabulary.Validation + "> ; <" + SOAVocabulary.recommendation + "> ?validationAction ] . " : "" ) +
//    		     (scope ? "?annotation <" + OAVocabulary.hasScope + "> ?aScope . " : "") +
    		     (scope ? "?annotation <" + OAVocabulary.hasScope + "> ?scope . " : "") +
    		 "   ?annotation <" + ASVocabulary.generator + "> ?generator . " +
    		" } " + whereSparqlValidated;
    		
    		String sparqlValidatedCount = "SELECT DISTINCT ?annotation " + whereSparqlValidated ;
    		
//    		String sparqlValidatedGroup = 
//			"  CONSTRUCT { " + 
//			"    ?sampleAnnotation a <" + OAVocabulary.Annotation + ">  . " +
//			"    ?sampleAnnotation <" + OAVocabulary.hasBody + "> ?body . " +
//  		         (confidence ? " ?sampleAnnotation <" + SOAVocabulary.confidence + "> ?avgConfidence . " : "") +
//			"    ?sampleAnnotation <" + OAVocabulary.hasTarget + "> _:target . " + 
//    		"    _:target <" + OAVocabulary.hasSource + "> ?source . " +
//    		     (state ? "?sampleAnnotation <" + SOAVocabulary.hasReview + "> [ a <" + SOAVocabulary.Validation + "> ; <" + SOAVocabulary.recommendation + "> ?validationAction ] . " : "" ) +
//    		     (scope ? "?sampleAnnotation <" + OAVocabulary.hasScope + "> ?scope . " : "") +
//    		" } " + whereSparqlValidatedGroup ;
    		
    		String sparqlUnvalidated = 
			"  CONSTRUCT { " + 
			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
			     (created ? " ?annotation <" + DCTVocabulary.created + "> ?created . " : "") +
//			     (creator ? "?annotation <" + DCTVocabulary.creator + "> [ a ?creatorType ] . " : "" ) +
			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . " +
  	             (confidence ? " ?annotation <" + SOAVocabulary.confidence + "> ?confidence . " : "") +
			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
			     (selector ?  "?target <" + OAVocabulary.hasSelector + "> [" + (pathLength == 1 ? " a <" + SOAVocabulary.RDFPropertySelector + "> ; <" + SOAVocabulary.property + "> " + onPropertyStringAsPath + "" : " a <" + SOAVocabulary.RDFPathSelector + "> ; <" + SOAVocabulary.rdfPath + "> \"" + onPropertyStringAsPath + "\"" ) + " ; <" + SOAVocabulary.destination + "> [ a <" + SOAVocabulary.Literal + "> ; <" + RDFVocabulary.value + "> ?valueStr ; <" + DCTVocabulary.language + "> ?valueLang ] ; <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start ;  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end ] . " : "") +
    		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//    		     (state ? "?annotation <" + OAVocabulary.hasState + "> [ a <" + SOAVocabulary.ValidationState + "> ; <" + RDFVocabulary.value + "> ?validationState ] . " : "" ) +
//    		     (scope ? "?annotation <" + OAVocabulary.hasScope + "> ?aScope . " : "") +
            "    ?annotation <" + ASVocabulary.generator + "> ?generator . " +
    		" } " + whereSparqlUnvalidated;
    		
    		String sparqlUnvalidatedCount = "SELECT DISTINCT ?annotation " + whereSparqlUnvalidated ;
//    		

			int counter = 0;
			
			String[] queries;
			String[] countQueries;
			
//			System.out.println(onlyReviewed + " " + onlyNonRejected);
			if (onlyReviewed) {
				queries = new String [ ] { sparqlValidated } ;
				countQueries = new String [ ] { sparqlValidatedCount } ;
			} else {
				queries = new String [ ] { sparqlValidated, sparqlUnvalidated } ;
				countQueries = new String [ ] { sparqlValidatedCount, sparqlUnvalidatedCount } ;
			}
//			queries = new String [ ] { sparqlValidatedGroup } ;
			
			JsonGenerator jsonGenerator = null;
			ByteArrayOutputStream jsonbos = null;
			ObjectMapper mapper = null;
			
			try {
				if (format == SerializationType.JSONLD) {
					jsonbos = new ByteArrayOutputStream();
					mapper = new ObjectMapper();
				}
	
//				int tc = 0;
				for (int i = 0; i < queries.length; i++) {
					String sparql = queries[i];
	
//					System.out.println(QueryFactory.create(sparql));
					
					// it is strange that constuct works in this way. in other similar construct queries the limit/offset works by counting triples 
					try (VirtuosoConstructIterator vs = new VirtuosoConstructIterator(vc.getSparqlEndpoint(), sparql, 300)) {
						
						Model model1 = ModelFactory.createDefaultModel();
						
						while (vs.hasNext()) {
	//						Model model1 = vs.next();
							
							Model modelx = vs.next();
							model1.add(modelx);
							
							//possible memory issues here split !
							
	//						System.out.println("-- " + modelx.size());
						}
						
//						tc += model1.listStatements(null, RDFVocabulary.type, OAVocabulary.Annotation).toList().size();
//						System.out.println(model1.listStatements(null, RDFVocabulary.type, OAVocabulary.Annotation).toList().size());
						model1.clearNsPrefixMap();
						
						UpdateRequest ur = UpdateFactory.create();
						
						// remove empty language fields
						ur.add("DELETE { ?x <" + DCTVocabulary.language + "> \"\" } WHERE { ?x <" + DCTVocabulary.language + "> \"\" }" );
						
						// rename recommendations
						ur.add("DELETE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Approve + "> } INSERT { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Accept + "> } WHERE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Approve + "> }");
						ur.add("DELETE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Delete + "> } INSERT { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Reject + "> } WHERE { ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Delete + "> }");
						
						// move start, end to refinedBy object 
						ur.add("DELETE { ?selector <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start . ?selector  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } INSERT { ?selector <" + OAVocabulary.refinedBy + "> [ a <" + OAVocabulary.TextPositionSelector + "> ; <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start ;  <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end ] } WHERE { ?selector <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start . ?selector <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end }");
						
						//  add annotator uris 
						try (QueryExecution qe = QueryExecutionFactory.create("select distinct ?generator {?annotation <" + ASVocabulary.generator + "> ?generator }", model1)) {
							ResultSet qr = qe.execSelect();
							while (qr.hasNext()) {
								String generator = qr.next().get("generator").toString();

								if (resourceVocabulary.isAnnotator(generator)) {
									AnnotatorDocument adoc = annotatorRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(generator)).get();
									DataService ds = dsRepository.findByIdentifierAndType(adoc.getAnnotator(), DataServiceType.ANNOTATOR).get();
									
									if (ds.getUri() != null) {
										ur.add("INSERT { ?annotation <" + DCTVocabulary.creator + "> <" + ds.getUri() + "> . <" + ds.getUri() + "> a <" + ASVocabulary.Application + "> " + (ds.getTitle() != null ? "; <" + FOAFVocabulary.name + "> \"" + ds.getTitle() + "\"": "") + " } WHERE { ?annotation <" + ASVocabulary.generator + "> <" + generator + "> }");
									} else {
										ur.add("INSERT { ?annotation <" + DCTVocabulary.creator + "> [ a <" + ASVocabulary.Application + "> " + (ds.getTitle() != null ? "; <" + FOAFVocabulary.name + "> \"" + ds.getTitle() + "\"": "") + "] } WHERE { ?annotation <" + ASVocabulary.generator + "> <" + generator + "> }");
									}
									
									if (scope && adoc.getDefaultTarget() != null) {
										ur.add("INSERT { ?annotation <" + OAVocabulary.hasScope + "> <" + adoc.getDefaultTarget() + "> } WHERE { ?annotation <" + ASVocabulary.generator + "> <" + generator + "> . FILTER NOT EXISTS { ?annotation <" + OAVocabulary.hasScope + "> ?scope } } ");
									}
									
								} else if (resourceVocabulary.isAnnotationValidator(generator)) {
									
									ur.add("INSERT { ?annotation <" + DCTVocabulary.creator + "> [ a <" + FOAFVocabulary.Person + "> ] } WHERE { ?annotation <" + ASVocabulary.generator + "> <" + generator + "> }");
								}
							}
							
						}

						
						// delete sage generator
						ur.add("DELETE { ?annotation <" + ASVocabulary.generator + "> ?generator } WHERE { ?annotation <" + ASVocabulary.generator + "> ?generator }");

						// delete scope from rejected annotations
						ur.add("DELETE { ?annotation <" + OAVocabulary.hasScope + "> ?scope } WHERE { ?annotation <" + OAVocabulary.hasScope + "> ?scope . ?annotation <" + SOAVocabulary.hasReview + "> ?review . ?review <" + SOAVocabulary.recommendation + "> <" + SOAVocabulary.Reject + "> }");

						UpdateAction.execute(ur, model1);

			    		Model model = model1;
	
				        if (model.size() > 0) {
	
	//						try (QueryExecution qe = QueryExecutionFactory.create(sp, model1)) {
	//							model = qe.execConstruct();
	//							model.clearNsPrefixMap();
	//						}
	
							String suffix = "";
							
							try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
	
								if (format == SerializationType.TTL) {
									RDFDataMgr.write(bos, model, Lang.TURTLE);
									suffix = "ttl";
									
									bos.flush();
									writeStream(out, aeg.getUuid() + (counter == 0 ? "" : "_" + counter) + "." + suffix, bos.toByteArray());
									counter++;

								} else if (format == SerializationType.NT) {
									RDFDataMgr.write(bos, model, Lang.TURTLE);
									suffix = "nt";
									
									bos.flush();
									writeStream(out, aeg.getUuid() + (counter == 0 ? "" : "_" + counter) + "." + suffix, bos.toByteArray());
									counter++;

								} else if (format == SerializationType.RDF_XML ) {
									RDFDataMgr.write(bos, model, Lang.RDFXML);
									suffix = "xml";
									
									bos.flush();
									writeStream(out, aeg.getUuid() + (counter == 0 ? "" : "_" + counter) + "." + suffix, bos.toByteArray());
									counter++;

								} else if (format == SerializationType.JSONLD) {
									suffix = "jsonld";
									
		//							System.out.println(">>> " + annotationContext);
									
							       	Map<String, Object> frame = new HashMap<>();
							    	frame.put("@type" , "http://www.w3.org/ns/oa#Annotation");
							    	frame.put("@context", ((Map)annotationContext.get("annotation-jsonld-context")).get("@context")); // why ????
		
							        JsonLdOptions options = new JsonLdOptions();
							        options.setCompactArrays(true);
							        options.useNamespaces = true ; 
							        options.setUseNativeTypes(true); 	      
							        options.setOmitGraph(false);
							        options.setPruneBlankNodeIdentifiers(true);
								        
							        final RDFDataset jsonldDataset = (new JenaRDF2JSONLD()).parse(DatasetFactory.wrap(model).asDatasetGraph());
							        Object obj = (new JsonLdApi(options)).fromRDF(jsonldDataset, true);
		//						    
							        Map<String, Object> jn = JsonLdProcessor.frame(obj, frame, options);
							        
//							        mapper.writerWithDefaultPrettyPrinter().writeValue(bos, jn);
							        
									if (jsonGenerator == null) { // first json-ld part 
										JsonFactory jsonFactory = new JsonFactory();				
										jsonGenerator = jsonFactory.createGenerator(jsonbos, JsonEncoding.UTF8);
										jsonGenerator = jsonGenerator.useDefaultPrettyPrinter();
										jsonGenerator.writeStartObject();

										jsonGenerator.writeFieldName("@context");
										mapper.setDefaultPrettyPrinter(jsonGenerator.getPrettyPrinter());
										mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, jn.get("@context"));
										
										jsonGenerator.writeFieldName("@graph");
										jsonGenerator.writeStartArray();
										
									}
									
									for (Object element : (List)jn.get("@graph")) {
										mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, element);
									}
									
								}
							}
				        }						
					}
				}
				
				if (format == SerializationType.JSONLD) {
					if (jsonGenerator == null) { // no annotations, write empty file
						JsonFactory jsonFactory = new JsonFactory();				
						jsonGenerator = jsonFactory.createGenerator(jsonbos, JsonEncoding.UTF8);
						jsonGenerator = jsonGenerator.useDefaultPrettyPrinter();
						jsonGenerator.writeStartObject();

						jsonGenerator.writeFieldName("@context");
						mapper.setDefaultPrettyPrinter(jsonGenerator.getPrettyPrinter());
//						mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, jn.get("@context"));
						jsonGenerator.writeStartObject();
						jsonGenerator.writeEndObject();
						
						jsonGenerator.writeFieldName("@graph");
						jsonGenerator.writeStartArray();
					}
					
					jsonGenerator.writeEndArray();
					jsonGenerator.writeEndObject();
					jsonGenerator.flush();
					
					writeStream(out, aeg.getUuid() + ".jsonld", jsonbos.toByteArray());
				}
				
			} finally {
				if (jsonbos != null) {
					jsonbos.close();
				}
			}
				
		}

     		
//    	} else if (format == SerializationType.CSV) {
//    		
//        	String sparql = 
//        			"SELECT ?annotation " +
//                    (created ? "?created " : " ") +
//                    (score ? "?score " : " ") +
//                    "?body " +
//                    (selector ? "?start " : " ") +
//                    (selector ? "?end " : " ") +
//	                "?source " +
//	                whereSparqlUnvalidated ;
//    		
//			try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
//					Writer writer = new BufferedWriter(new OutputStreamWriter(bos));
//					CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Item", "AnnotationIRI", "AnnotationLiteral", "Score", "SourceLiteralLexicalForm", "SourceLiteralLanguage", "SourceProperty"));
//					ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
//				
//				try (VirtuosoSelectIterator vs = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {
//					while (vs.hasNext()) {
//						QuerySolution sol = vs.next();
//						Resource sid = sol.get("source").asResource();
//						RDFNode node1 = sol.get("body");
//	//					RDFNode node2 = sol.get("body2");
//						RDFNode scorev = sol.get("score");
//						RDFNode value = sol.get("value");
//	
//						List<Object> line = new ArrayList<>();
//						line.add(sid);
//						
//						if (node1.isResource()) {
//							line.add(node1.asResource());
//							line.add(null);
//						} else if (node1.isLiteral()) {
//							line.add(null);
//							line.add(NodeFactory.createLiteralByValue(Utils.escapeJsonTab(node1.asLiteral().getLexicalForm()), node1.asLiteral().getLanguage(), node1.asLiteral().getDatatype()));
//						}
//						
//						if (scorev != null) {
//							line.add(scorev.asLiteral().getDouble());
//						} else {
//							line.add(null);
//						}
//						
//						if (value != null) {
//							line.add(value.asLiteral().getLexicalForm());
//							line.add(value.asLiteral().getLanguage());
//							line.add(onPropertyString);
//						} else {
//							line.add(null);
//							line.add(null);
//							line.add(null);
//						}
//						
//	
//	//					if (node2 != null) {
//	//						if (node2.isResource()) {
//	//							line.add(node2.asResource());
//	//							line.add(null);
//	//						} else if (node2.isLiteral()) { 
//	//							line.add(null);
//	//							line.add(NodeFactory.createLiteralByValue(Utils.escapeJsonTab(node2.asLiteral().getLexicalForm()), node2.asLiteral().getLanguage(), node2.asLiteral().getDatatype()));
//	//						}
//	//					}
//						
//						csvPrinter.printRecord(line);
//					}
//					
//				}
//	
//				csvPrinter.flush();
//				bos.flush();
//				
//				try (ZipOutputStream zos = new ZipOutputStream(baos)) {
//					ZipEntry entry = new ZipEntry(aeg.getUuid() + ".csv");
//	
//					zos.putNextEntry(entry);
//					zos.write(bos.toByteArray());
//					zos.closeEntry();
//	
//				} catch (IOException ioe) {
//					ioe.printStackTrace();
//				}
//	
//				return new ByteArrayResource(baos.toByteArray());
//			}
//    	}
//    	
//    	return null;
	}
	
	private void writeStream(OutputStream out, String filename, byte[] bytes) throws Exception {
		if (out instanceof ZipOutputStream) {
			ZipEntry entry = new ZipEntry(filename);
			
			((ZipOutputStream)out).putNextEntry(entry);
			out.write(bytes);
			((ZipOutputStream)out).closeEntry();
		} else if (out instanceof TarArchiveOutputStream) {
			
			TarArchiveEntry tarEntry = new TarArchiveEntry(filename);
			tarEntry.setSize(bytes.length);
			
	        ((TarArchiveOutputStream)out).putArchiveEntry(tarEntry);
	        out.write(bytes);
	        ((TarArchiveOutputStream)out).closeArchiveEntry();
		}

	}
	
	
//	public List<Map<String, Object>> computeValidationDistribution(UserPrincipal currentUser, String id, int accuracy) throws Exception {
//
//		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(id));
//		if (!aegOpt.isPresent()) {
//			return null;
//		}		
//		
//		AnnotationEditGroup aeg = aegOpt.get();
//		
//		Optional<ac.software.semantic.model.Dataset> dopt = datasetRepository.findByUuid(aeg.getDatasetUuid());
//		TripleStoreConfiguration vc = dopt.get().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
//
//		List<String> generatorIds = new ArrayList<>();
//		generatorIds.addAll(annotatorRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(doc -> "<" + resourceVocabulary.getAnnotatorAsResource(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
//		generatorIds.addAll(pavRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> "<" + resourceVocabulary.getAnnotationValidatorAsResource(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
//		generatorIds.addAll(favRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> "<" + resourceVocabulary.getAnnotationValidatorAsResource(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
//
//		String annfilter = generatorFilter("annotation", generatorIds);
//		
//    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
//    	
//    	List<Map<String, Object>> result = new ArrayList<>();
//
//    	for (String validation : new String[] { SOAVocabulary.Delete.toString(), SOAVocabulary.Approve.toString() } ) {
//    		List<Distribution> distr = new ArrayList<>();
//    		
//	    	for (int i = 0; i < 100/accuracy; i++) {
//		    	String sparql = 
//		    			"SELECT (count(*) AS ?count) " +
//		    			"WHERE { " +
//		    			"  GRAPH <" + resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString() + "> { " + 
//		    			"    ?source " + onPropertyString + " ?value }  " +
//		    			"  GRAPH <" + aeg.getAsProperty() + "> { " + 
//		    			"    ?annotation a <" + OAVocabulary.Annotation + ">  . " +
//		    			     annfilter + 
//		    			"    ?annotation <" + SOAVocabulary.hasValidation + "> [ <" + SOAVocabulary.action + "> <" + validation + "> ] } " +
//		    			"    ?annotation <" + SOAVocabulary.score + "> ?score . " +
//		    			"    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
//		    			"    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
//		        		"    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
//		        		"    ?target <" + SOAVocabulary.onValue + "> ?value . " +
//		        		"    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//		        		"    FILTER ( ?score > " + i*accuracy/(double)100 + " && ?score <= " + (i + 1)*accuracy/(double)100 + ") " +
//		        		"  }  ";
//		    	
////		    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
//		    	
//		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//
//					ResultSet results = qe.execSelect();
//					
//					while (results.hasNext()) {
//						QuerySolution qs = results.next();
//						
//						Distribution d = new Distribution();
//						d.setCount(qs.get("count").asLiteral().getInt());
//						d.setLowerBound(i*accuracy/(double)100);
//						d.setLowerBoundIncluded(false);
//						d.setUpperBound((i + 1)*accuracy/(double)100);
//						d.setUpperBoundIncluded(true);
//						
//						distr.add(d);
//					}
//		    	}
//		    	
//	    	}
//	    	
//	    	Map<String, Object> map = new HashMap<>();
//	    	map.put("key", validation);
//	    	map.put("distribution", distr);
//	    	
//	    	result.add(map);
//    	}
// 
//    	return result;
//	}
	
	public ValueResponseContainer<ValueAnnotation> view(UserPrincipal currentUser, AnnotationEditGroupContainer aegc, AnnotationValidationMode mode, int page) {

		AnnotationEditGroup aeg = aegc.getObject();
		TripleStoreConfiguration vc = aegc.getDatasetTripleStoreVirtuosoConfiguration();

		DatasetCatalog dcg = schemaService.asCatalog(aeg.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		String annotatorFromClause = "";
		
		List<AnnotatorDocument> adocs = aegc.getAnnotators();
		AnnotatorDocument fadoc = adocs.get(0);
		List<IndexKeyMetadata> keyMetadata = adocs.get(0).getStructure().getKeysMetadata();  /// wrong !!!!! > replace with aeg.keys;
		
		Map<String, AnnotatorDocument> annotatorMap = new HashMap<>();
		for (AnnotatorDocument adoc : adocs) {
			annotatorMap.put(adoc.getUuid(), adoc);
			
			if (separateGraphs) {
				annotatorFromClause += "FROM <" + resourceVocabulary.getAnnotatorAsResource(adoc) + "> ";
			}
		}
		
		String annfilter = sparqlQueryService.generatorFilter("v", annotatorMap.values().stream().map(doc -> doc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));

		if (!separateGraphs) {
			annotatorFromClause += "FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> "; 
		} else {
			annotatorFromClause += "FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> "; // for compatibility to remove
		}
		
//		System.out.println(annotatorMap);
		
		
		String inSelect = "";
		String inWhere = "";
		
		if (aeg.getOnProperty() != null) { 
			inSelect = " (count(DISTINCT ?value) AS ?valueCount) " ;
			inWhere = " ?target <" + SOAVocabulary.onValue + "> ?value . ";
		} else {
			for (IndexKeyMetadata ikm : keyMetadata) {
				inSelect += " (count(DISTINCT ?value_" + ikm.getIndex() + ") AS ?value" + ikm.getIndex() + "Count) " ;
				if (ikm.getOptional() != null && ikm.getOptional() == true) {
					inWhere += " OPTIONAL { ?target <" +  SOAVocabulary.onBinding + "> ?vvv_" + ikm.getIndex() + " . ?vvv_" + ikm.getIndex() + " <" +  SOAVocabulary.value + "> ?value_" + ikm.getIndex() + " . ?vvv_" + ikm.getIndex() + " <" + SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" } . " ;
				} else {
					inWhere += " ?target <" +  SOAVocabulary.onBinding + "> ?vvv_" + ikm.getIndex() + " . ?vvv_" + ikm.getIndex() + " <" +  SOAVocabulary.value + "> ?value_" + ikm.getIndex() + " . ?vvv_" + ikm.getIndex() + " <" + SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" . " ;
				}
			}
		}	
		
		String sparql = 
				"SELECT (COUNT(?v) AS ?annCount) (COUNT(DISTINCT ?source) AS ?sourceCount) " + inSelect +
//				"FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> " +
				annotatorFromClause + 
		        "WHERE { " +  
		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
		        annfilter + 
//			    " ?v <" + OAVocabulary.hasTarget + "> <" + OAVocabulary.hasSource + "> ?source ; <" + SOAVocabulary.onValue + "> ?value ] } ";
		        " ?v <" + OAVocabulary.hasTarget + "> ?target . " +
		        " ?target <" + OAVocabulary.hasSource + "> ?source . " +
                  inWhere +
                "}";
		
	
		int annCount = 0;
		int sourceCount = 0;
//		int valueCount = 0;
		List<ResultCount> rc  = new ArrayList<>();
		
//		System.out.println(sparql);
//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				
				annCount = sol.get("annCount").asLiteral().getInt();
				sourceCount = sol.get("sourceCount").asLiteral().getInt();
//				valueCount = sol.get("valueCount").asLiteral().getInt();
				
				if (aeg.getOnProperty() != null) {
					rc.add(new ResultCount("" + 0, sol.get("valueCount").asLiteral().getInt()));
				} else {
					for (IndexKeyMetadata ikm : keyMetadata) {
						if (sol.get("value" + ikm.getIndex() + "Count") != null) {
							rc.add(new ResultCount("" + ikm.getName() , sol.get("value" + ikm.getIndex() + "Count").asLiteral().getInt()));
						}
					}
				}	
			}
		}

		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
		vrc.setTotalCount(annCount);
		vrc.setDistinctSourceTotalCount(sourceCount);
//		vrc.addDistinctValueTotalCount(new ResultCount("",valueCount));
		vrc.setDistinctValueTotalCount(rc);
		
		String spath = null;
		List<ValueCount> values;
		String variablesString = "";
		String bindString = "";
		ExecutionOptions eo = null;
		String valuesString = "";
		
//		System.out.println("D");
		if (aeg.getOnProperty() != null) {
			spath = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
			variablesString = "?value";
	        bindString = " ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" . " + 
			             " ?r <" + SOAVocabulary.onValue + "> ?value . ";
	        valuesString = " ?c_0 " +spath + " ?value . FILTER (isLiteral(?value)) . ";
		} else {
		    
			SPARQLStructure ss = sparqlService.toSPARQL(fadoc.getStructure().getElement(), AnnotatorDocument.getKeyMetadataMap(keyMetadata), false);
			spath = ss.getTreeWhereClause();
			
//			System.out.println("A " + spath);
//			System.out.println("B " + ss.getWhereClause());
//			
//			System.out.println(fadoc.getName());
			eo = ((AnnotatorContainer)annotatorsService.getContainer(currentUser, fadoc)).buildExecutionParameters();
//			
//			values = getMultiValuesForPage(adoc, spath, eo.getTargets(), rdfDataset, page, fromClause, ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values()));
		
			String bindings = "";
			List<String> variables = new ArrayList<>();
			for (Map.Entry<String, IndexKeyMetadata> entry : eo.getTargets().entrySet()) {
				String key = entry.getKey();
				Boolean optional = entry.getValue().getOptional();
//				System.out.println(key + " " + entry.getValue().getName() + " " + optional);

				if (optional != null && optional == true) {
					bindings += " OPTIONAL { ?r <" + SOAVocabulary.onBinding + "> ?vvv_" + key + " . ?vvv_" + key + " <" + SOAVocabulary.variable + "> \"" + key + "\" . ?vvv_" + key + " <" + SOAVocabulary.value + "> ?value_" + key + " } . ";
				} else {
					bindings += " ?r <" + SOAVocabulary.onBinding + "> ?vvv_" + key + " . ?vvv_" + key + " <" + SOAVocabulary.variable + "> \"" + key + "\" . ?vvv_" + key + " <" + SOAVocabulary.value + "> ?value_" + key + " . ";
				}
				
				variables.add("?value_" + key);
				
			}
			
			for (String s : variables) {
				variablesString += s + " ";
			}
			
			bindString = "  ?r <" + SOAVocabulary.onGraphTree + "> \"" + spath + "\" . " + 
		                 "     " + bindings + " ";
			
//			valuesString = " ?c_0 " + spath + " . ";
			valuesString = " " + ss.getWhereClause();
			valuesString = valuesString.replaceAll("> \\?r([0-9]+) ", "> ?value_r$1 ");
			valuesString = valuesString.replaceAll(" VALUES \\?r([0-9]+) \\{ .*? \\} ", ""); // very hack.... temporary should not have VALUES on ?r...
			
		}
		
		sparql = null;
		if (mode == AnnotationValidationMode.ALL) {
			sparql = "SELECT " + variablesString + " ?t ?start ?end ?score ?count ?generator " +
			        fromClause + 
//			        "FROM NAMED <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> " +
			        annotatorFromClause +
					"{ " +
//					"SELECT distinct ?value ?t ?ie ?start ?end (COUNT(?ac) AS ?acCount) (SAMPLE(?ac) AS ?action) (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count)" + 
					"SELECT DISTINCT " + variablesString + " ?t ?start ?end ?generator (AVG(?sc) AS ?score) (COUNT(distinct ?c_0) AS ?count) " +
			        "WHERE { " + 
			        valuesString +
//			        "  OPTIONAL { GRAPH <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> { " + 
			        "  OPTIONAL { " +
				    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
			        "     <" + OAVocabulary.hasTarget + "> ?r . " + 
				    annfilter + 
				    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } " +  
				    bindString + 
				    "  ?r   <" + OAVocabulary.hasSource + "> ?c_0 . " +
                    " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +					
				    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
				    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " + 
//				    " } " + 
				    "} }" + 
				    "GROUP BY ?t " + variablesString + " ?start ?end ?generator " +
					"ORDER BY desc(?count) " + variablesString + " ?start ?end ?generator" +
					"} LIMIT " + PAGE_SIZE + " OFFSET " + PAGE_SIZE * (page - 1);
		} else if (mode == AnnotationValidationMode.ANNOTATED_ONLY) {
			sparql = 
					"SELECT " + variablesString + " ?t ?start ?end ?score ?count ?generator " + 
			        fromClause + 
//			        "FROM NAMED <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> " +
			        annotatorFromClause +
					"{ " +
//			        "SELECT distinct ?value ?t ?ie ?start ?end (COUNT(?ac) AS ?acCount) (SAMPLE(?ac) AS ?action) (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count)" + 
			        "SELECT DISTINCT " + variablesString + " ?t ?start ?end ?generator (AVG(?sc) AS ?score) (COUNT(distinct ?c_0) AS ?count) " +
			        "WHERE { " + 
			        valuesString + 
//					"  GRAPH <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> { " + 
		            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
					"     <" + OAVocabulary.hasTarget + "> ?r . " + 
		            annfilter + 
		            " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } " + 
				    bindString + 
				    "  ?r   <" + OAVocabulary.hasSource + "> ?c_0 . " +
                    " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +					
		            " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
		            " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " + "   } " +
//		            " } " + 
		            "GROUP BY ?t " + variablesString + " ?start ?end ?generator " +
					"ORDER BY desc(?count) " + variablesString + " ?start ?end ?generator " +
					"} LIMIT " + PAGE_SIZE + " OFFSET " + PAGE_SIZE * (page - 1);
		} else if (mode == AnnotationValidationMode.UNANNOTATED_ONLY) {
			sparql = 
					"SELECT " + variablesString + " ?count " +
			        fromClause + 
//			        "FROM NAMED <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> " + 
			        annotatorFromClause +
					"{ " +
			        "SELECT DISTINCT " + variablesString + " (COUNT(distinct ?c_0) AS ?count) " +
					"WHERE { " + 
					valuesString + 
//		            " FILTER NOT EXISTS { GRAPH <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> { " + 
					" FILTER NOT EXISTS { " +
					"  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "     <" + OAVocabulary.hasTarget + "> ?r . " + 
					annfilter +
				    bindString + 
				    "  ?r   <" + OAVocabulary.hasSource + "> ?c_0 . " +
//					"} " + 
				    "} }" + 
					"GROUP BY " + variablesString +  
					"ORDER BY desc(?count) " + variablesString + 
					"} LIMIT " + PAGE_SIZE + " OFFSET " + PAGE_SIZE * (page - 1);
		}    	

//		System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
		Map<RDFTermHandler, ValueAnnotation> res = new LinkedHashMap<>();

		//grouping does not work well with paging!!! annotations of some group may be split in different pages
		//it should be fixed somehow;
		//also same blank node annotation are repeated
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
		
			ResultSet rs = qe.execSelect();
			
			RDFTermHandler prev = null;
			ValueAnnotation va = null;
		
			Map<String, AnnotatorContext> annotatorInfoMap = new HashMap<>();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				
//				RDFNode value = sol.get("value");
				RDFTermHandler aev = null;
				if (fadoc.getOnProperty() != null) {
					RDFNode value = sol.get("value");
					
					if (value.isResource()) {
						aev = new SingleRDFTerm(value.asResource());
					} else if (value.isLiteral()) {
						aev = new SingleRDFTerm(value.asLiteral());
					}
				} else {
					List<SingleRDFTerm> list = new ArrayList<>();
					for (Map.Entry<String,IndexKeyMetadata> entry : eo.getTargets().entrySet()) {
						RDFNode value = sol.get("value_" + entry.getKey());
						
						if (value != null) {
							SingleRDFTerm st = null;
							if (value.isResource()) {
								st = new SingleRDFTerm(value.asResource());
							} else if (value.isLiteral()) {
								st = new SingleRDFTerm(value.asLiteral());
							}
							st.setName(entry.getValue().getName());
						
							list.add(st);
						}
					}
					
//						System.out.println(list);
					
					aev = new MultiRDFTerm(list);
				}
				
				String ann = sol.get("t") != null ? sol.get("t").toString() : null;
				Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
				Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
				int count = sol.get("count").asLiteral().getInt();
				Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
//				String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
				
				String generator = sol.get("generator") != null ? sol.get("generator").toString() : null;

//				System.out.println(ann + " " + generator);
				
				AnnotatorContext ai = null;

				if (generator != null) {
					
					ai = annotatorInfoMap.get(generator);
					
					if (ai == null) {
						AnnotatorDocument adoc = annotatorMap.get(resourceVocabulary.getUuidFromResourceUri(generator));
//						System.out.println("ADOC " + adoc.getThesaurus());
						
						DataService annotatorService = annotators.get(adoc.getAnnotator());

						ai = new AnnotatorContext();
						if (annotatorService != null) {
							ai.setName(annotatorService.getTitle());
	
//							if (adoc.getThesaurus() != null) {
							if (adoc.getThesaurusId() != null) {
//								Dataset dataset = datasetRepository.findByIdentifierAndDatabaseId(adoc.getThesaurus(), database.getId()).get();
								Dataset dataset = datasetRepository.findById(adoc.getThesaurusId()).get();
	
								ai.setId(dataset.getId());												
								ai.setVocabularyContainer(datasetService.createVocabularyContainer(dataset.getId()));
	
							}
						} else {
							
							PrototypeContainer pc = prototypeService.getContainer(currentUser, new SimpleObjectIdentifier(adoc.getAnnotatorId()));
							ai.setName(pc.getObject().getName());
						}
						
						annotatorInfoMap.put(generator, ai);

					} 
				}
				
				ValueAnnotationDetail vad = null;
				if (!aev.equals(prev)) {
					if (prev != null) {
						res.put(prev, va);
					}

					prev = aev;
					
					va = new ValueAnnotation();
					va.setOnValue(aev);
					va.setCount(count);
						
					if (ann != null) {
						vad  = new ValueAnnotationDetail();
					}
				} else {
					vad  = new ValueAnnotationDetail();
				}

				if (vad != null) {
					vad.setValue(ann);
//					vad.setValue2(ie);
					vad.setStart(start);
					vad.setEnd(end);
					vad.setScore(score);

					if (ai != null) {
						vad.setAnnotatorInfo(ai);
						if (ai.getVocabularyContainer() != null) {
							vad.setLabel(vocabularyService.getLabel(ann, ai.getVocabularyContainer().resolve(ann), false));
						} else {
							vad.setLabel(vocabularyService.getLabel(ann, null, true));
						}
					}

					va.getDetails().add(vad);
				}
			}
			
			if (prev != null) {
				res.put(prev, va);
			}
		}
		
		vrc.setValues(new ArrayList<>(res.values()));
		
		return vrc;
    } 

	private static int PAGE_SIZE = 50;

	public ListResult<ValueAnnotation> view2(UserPrincipal currentUser, AnnotationEditGroupContainer aegc, AnnotationValidationMode mode, int page, AnnotationEditGroupSearch filter) {
		
		AnnotationEditGroup aeg = aegc.getObject();
		TripleStoreConfiguration vc = aegc.getDatasetTripleStoreVirtuosoConfiguration();

		DatasetCatalog dcg = schemaService.asCatalog(aeg.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);
		
		String annotatorFromClause = "";
		
		Map<String, AnnotatorDocument> annotatorMap = new HashMap<>();
		for (AnnotatorDocument adoc : aegc.getAnnotators()) {
			annotatorMap.put(adoc.getUuid(), adoc);
			
			if (separateGraphs) {
				annotatorFromClause += "FROM <" + resourceVocabulary.getAnnotatorAsResource(adoc) + "> ";
			}
		}
		
		if (!separateGraphs) {
			annotatorFromClause += "FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> "; 
		} else {
			annotatorFromClause += "FROM <" + aeg.getTripleStoreGraph(resourceVocabulary) + "> "; // for compatibility to remove
		}

		AnnotationUtilsContainer auc = annotationUtils.createAnnotationUtilsContainer(aegc, filter);
		
		ValueResponseContainer<ValueAnnotation> vrc = annotationUtils.countAnnotations(aegc, auc.getKeyMetadata(), vc, null, auc.getInSelect(), annotatorFromClause, auc.getAnnfilter(), auc.getInWhere(), auc.getFilterString());
		
		String bodyClause = auc.getBodyClause();
		List<String> bodyVariables = auc.getBodyVariables();
		String groupBodyVariables = auc.getGroupBodyVariables();
		
		String isparql = "SELECT ?t ?start ?end (AVG(?sc) AS ?score) ?generator (count(distinct ?s) AS ?count) " + groupBodyVariables  +
		        annotatorFromClause +
				"{ " +
			    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		        "     <" + OAVocabulary.hasTarget + "> ?r . " + 
		        "  ?r <" + OAVocabulary.hasSource + "> ?s . " + 	
			    auc.getAnnfilter() + 
			    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } " +  
//			    bindString + 
			    " #####VALUES##### " + 
                bodyClause + 
                " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +					
			    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
			    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " +
			    "} " + 
			    "GROUP BY ?t ?start ?end ?generator " +
				"ORDER BY ?start ?end ?generator" ;
		
		String vsparqlCore = null; 
		
		if (mode == AnnotationValidationMode.ALL) {
			vsparqlCore = 
			        "WHERE { " + 
			        auc.getValuesString() +
			        auc.getFilterString() +
			        " }" + 
				    "GROUP BY " + auc.getVariablesString();
			
		} else if (mode == AnnotationValidationMode.ANNOTATED_ONLY) {
			vsparqlCore = 
			        "WHERE { " + 
			        auc.getValuesString() +
			        auc.getFilterString() +
//			        "   {  SELECT ?c_0 (count(distinct ?b) as ?db) WHERE { ?v  a <http://www.w3.org/ns/oa#Annotation> . ?v  <http://www.w3.org/ns/oa#hasBody>  ?b .?v  <http://www.w3.org/ns/oa#hasTarget>/<http://www.w3.org/ns/oa#hasSource>  ?c_0 . } group by ?c_0    } FILTER (?db > 1) " +
			        
					"FILTER EXISTS { " +
					"  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "     <" + OAVocabulary.hasTarget + "> ?r . " + 
		            "  ?r   <" + OAVocabulary.hasSource + "> ?c_0 . " +
		               auc.getAnnfilter() +
		               (aeg.getOnProperty() != null ? auc.getHasValueClauses().get(0) : "") + // different behaviour for onClass because of optionals
			        "} " +
		            "} " + 
				    "GROUP BY " + auc.getVariablesString();

		} else if (mode == AnnotationValidationMode.UNANNOTATED_ONLY) {
			vsparqlCore = 
			        "WHERE { " + 
			        auc.getValuesString() +
			        auc.getFilterString() +
					"FILTER NOT EXISTS { " +
					"  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "     <" + OAVocabulary.hasTarget + "> ?r . " + 
		            "  ?r   <" + OAVocabulary.hasSource + "> ?c_0 . " +
		               auc.getAnnfilter() +
		               (aeg.getOnProperty() != null ? auc.getHasValueClauses().get(0) : "") + // different behaviour for onClass because of optionals
			        "} }" + 
				    "GROUP BY " + auc.getVariablesString();

		}
		
		String cvsparql = 
				"SELECT (COUNT(*) AS ?count) " +
				fromClause + 
				annotatorFromClause +
				" { " +
				"SELECT " + auc.getVariablesString() +
				vsparqlCore +
				" }";
		
		String vsparql =
				"SELECT " + auc.getVariablesString() + " ?count " +
				fromClause + 
				annotatorFromClause +
				" { " +
				"SELECT " + auc.getVariablesString() + " (COUNT(distinct ?c_0) AS ?count) " +
				vsparqlCore + 
				"ORDER BY desc(?count) " + auc.getVariablesString() +
				" } LIMIT " + PAGE_SIZE + " OFFSET " + PAGE_SIZE * (page - 1);				
						
		
		Pagination pg = annotationUtils.createPagination(vc, null, cvsparql, page, PAGE_SIZE);
		
//    	System.out.println("VSPARQL");
//    	System.out.println(vsparql);
//    	System.out.println(QueryFactory.create(cvsparql, Syntax.syntaxARQ));
//    	System.out.println(QueryFactory.create(vsparql, Syntax.syntaxARQ));
//    	
		Map<RDFTermHandler, ValueAnnotation> res = new LinkedHashMap<>();

		//grouping does not work well with paging!!! annotations of some group may be split in different pages
		//it should be fixed somehow;
		//also same blank node annotation are repeated

		try (QueryExecution vqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(vsparql, Syntax.syntaxSPARQL_11))) { // QueryFactory.create(vsparql, Syntax.syntaxSPARQL_11) fails
//		try (RDF4JRemoteSelectIterator vqe = new RDF4JRemoteSelectIterator(vc.getSparqlEndpoint(), vsparql, false)) { 
			
			Map<String, AnnotatorContext> annotatorInfoMap = new HashMap<>();

			ResultSet vrs = vqe.execSelect();
			
			int index = 0;
			while (vrs.hasNext()) {
//			while (vqe.hasNext()) {
				QuerySolution vsol = vrs.next();
//				BindingSet vsol = vqe.next();
				
				index++;
				
				int count = vsol.get("count").asLiteral().getInt();
//				int count = RDF4JRemoteSelectIterator.value2RDFNode(vsol.getBinding("count").getValue()).asLiteral().getInt();
				
				String restr = "";
				for (int i = 0; i < auc.getVariables().size(); i++) {
					String var = auc.getVariables().get(i);
					RDFNode value = vsol.get(var);
//					RDFNode value = vsol.getBinding(var) != null ? RDF4JRemoteSelectIterator.value2RDFNode(vsol.getBinding(var).getValue()) : null ;

					if (value == null) {
						restr += auc.getHasNoValueClauses().get(i);
					} else {
						restr += auc.getHasValueClauses().get(i) + " VALUES ?" + var + " { " + RDFTerm.createRDFTerm(value).toRDFString() + " } ";
					}
				}
				
				RDFTermHandler aev = annotationUtils.createValueRDFTermHandler(aegc, vsol, auc.getrNameToEntryMap());
				
				
				ValueAnnotation va = new ValueAnnotation();
				va.setOnValue(aev);
				va.setCount(count);
				va.setIndex(PAGE_SIZE * (page - 1) + index);

				res.put(aev, va);
				
				int pos = isparql.indexOf("#####VALUES#####");
				String csparql = isparql.substring(0, pos) + restr + isparql.substring(pos + "#####VALUES#####".length());
//				String csparql = isparql.replaceAll("#####VALUES#####", restr); // causes problems with special characters.
				
//				System.out.println("CSPARQL");
//				System.out.println(csparql);
//		    	System.out.println(QueryFactory.create(csparql, Syntax.syntaxSPARQL_11));
		    	
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(csparql, Syntax.syntaxSPARQL_11))) {
					
					ResultSet rs = qe.execSelect();
					
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						
						String ann = sol.get("t") != null ? sol.get("t").toString() : null;
						Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
						Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
//						int count = sol.get("count").asLiteral().getInt();
						Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
						String generator = sol.get("generator") != null ? sol.get("generator").toString() : null;

						AnnotatorContext ai = null;

						if (generator != null) {
							ai = annotatorInfoMap.get(generator);
							
							if (ai == null) {
								AnnotatorDocument adoc = annotatorMap.get(resourceVocabulary.getUuidFromResourceUri(generator));
								ai = annotationUtils.createAnnotationContext(adoc, aegc.getEnclosingObject());
							} 
						}
						
						ValueAnnotationDetail vad = new ValueAnnotationDetail();

						vad.setValue(ann);
						vad.setStart(start);
						vad.setEnd(end);
						vad.setScore(score);

						if (ai != null) {
							vad.setAnnotatorInfo(ai);
							if (ai.getVocabularyContainer() != null) {
								vad.setLabel(vocabularyService.getLabel(ann, ai.getVocabularyContainer().resolve(ann), false));
							} else {
								vad.setLabel(vocabularyService.getLabel(ann, null, true));
							}
						}
						
						if (bodyVariables.size() > 0) {
							Model model = ModelFactory.createDefaultModel();
							Resource r = ResourceFactory.createResource();
			    			for (int j = 0; j < bodyVariables.size(); j++) {
			    				if (sol.get(bodyVariables.get(j)) != null) {
			    					for (String v : sol.get(bodyVariables.get(j)).toString().split("\\|\\|")) {
			    						model.add(r, ResourceFactory.createProperty(auc.getBodyProperties().get(j)), v);
			    					}
			    				}
			    			}
			    			
			    			if (model.size() > 0) {
				    			Map<String, Object> jn = apiUtils.jsonLDFrame(model, new HashMap<>());
				    			vad.setControlGraph(jn);
			    			}
						}	

						va.getDetails().add(vad);
					}
				}
			}
			
			pg.setCurrentElements(index);
		}

		
		ListResult<ValueAnnotation> list = new ListResult<>(new ArrayList<>(res.values()), pg);
		list.setMetadata(vrc);
		
		return list;
    } 

	@Override
	public AnnotationEditGroup create(UserPrincipal currentUser, Dataset dataset, AnnotationEditGroupUpdateRequest ur) throws Exception {

		AnnotationEditGroup aeg = new AnnotationEditGroup(dataset);
		aeg.setUserId(new ObjectId(currentUser.getId()));
		aeg.setAsProperty(ur.getAsProperty());
		aeg.setAutoexportable(ur.isAutoexportable());
		aeg.setOnProperty(ur.getOnProperty());
		aeg.setOnClass(ur.getOnClass());
		if (ur.getOnClass() != null) {
			aeg.setKeys(ur.getKeys());
		}
		aeg.setTag(ur.getTag());
//		aeg.setSparqlClause(ur.getSparqlClause());

		return create(aeg);
	}
	
	@Override
	public AnnotationEditGroupContainer getContainer(UserPrincipal currentUser, AnnotationEditGroup aeg, Dataset dataset) {
		AnnotationEditGroupContainer ac = new AnnotationEditGroupContainer(currentUser, aeg, dataset);

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}
	}
	
	@Override
	public AnnotationEditGroupContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		AnnotationEditGroupContainer ac = new AnnotationEditGroupContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	@Override
	public ListPage<AnnotationEditGroup> getAllByUser(ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(aegRepository.findByDatabaseIdAndUserId(database.getId(), userId));
		} else {
			return ListPage.create(aegRepository.findByDatabaseIdAndUserId(database.getId(), userId, page));
		}
	}

	
	@Override
	public ListPage<AnnotationEditGroup> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(aegRepository.findByDatasetIdInAndUserId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId));
		} else {
			return ListPage.create(aegRepository.findByDatasetIdInAndUserId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId, page));
		}
	}

	
}
