package ac.software.semantic.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.PropertyValue;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.payload.notification.ExecuteNotificationObject;
import ac.software.semantic.payload.notification.PublishNotificationObject;
import ac.software.semantic.payload.request.ClustererUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.ClustererDocumentResponse;
import ac.software.semantic.payload.response.MappingResponse;
import ac.software.semantic.payload.response.ResultCount;
import ac.software.semantic.payload.response.ValueResponse;

import org.apache.http.impl.client.cache.ManagedHttpCacheStorage;
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
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.DefaultIndexedColorMap;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFCreationHelper;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFHyperlink;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ClustererDocument;
import ac.software.semantic.model.ConditionInstruction;
import ac.software.semantic.model.ControlProperty;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.PrepareState;
import ac.software.semantic.model.constants.state.ThesaurusLoadState;
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.DataServiceRank;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.Pagination;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.PreprocessInstruction;
import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.SchemaSelector;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.ClustererDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.root.DataServiceRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService.AnnotationEditGroupContainer;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.IdentifiersService.GraphLocation;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.PrototypeService.PrototypeContainer;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.DataServiceContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PreparableContainer;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.monitor.ExecuteMonitor;
import ac.software.semantic.service.monitor.GenericMonitor;
import edu.ntua.isci.ac.common.db.rdf.RDFLibrary;
import edu.ntua.isci.ac.common.utils.Counter;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;
import edu.ntua.isci.ac.d2rml.model.map.ValueMap.TermMapType;
import edu.ntua.isci.ac.d2rml.monitor.SimpleMonitor;
import edu.ntua.isci.ac.d2rml.output.RDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.StringOutputHandler;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.JenaStringRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;


@Service
public class ClustererService implements ExecutingPublishingService<ClustererDocument, ClustererDocumentResponse>, 
                                         EnclosedCreatableService<ClustererDocument, ClustererDocumentResponse, ClustererUpdateRequest, Dataset>,
                                         IdentifiableDocumentService<ClustererDocument, ClustererDocumentResponse> {

	private Logger logger = LoggerFactory.getLogger(ClustererService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	private SchemaService schemaService;

	@Lazy
	@Autowired
	private DatasetService datasetService;

	@Lazy
	@Autowired
	private AnnotatorService annotatorService;

	@Autowired
	private PrototypeDocumentRepository prototypeRepository;

	@Lazy
	@Autowired
	private VocabularyService vocabularyService;

	@Autowired
	private SPARQLService sparqlService;

	@Autowired
	private PrototypeService prototypeService;

	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;

	@Autowired
	private ModelMapper mapper;
	
	@Autowired
	private FolderService folderService;

//	@Autowired
//	private AnnotationUtils annotationUtils;
	
	@Autowired
	private APIUtils apiUtils;

	@Lazy
	@Autowired
	private UserService userService;

//    @Autowired
//    @Qualifier("annotators")
//    private Map<String, DataService> annotators;

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;

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

	@Value("${dataservice.definition.folder}")
	private String dataserviceFolder;
	
	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Autowired
	private ClustererDocumentRepository clustererRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private DataServicesService dataServicesService;

	@Autowired
	private DataServiceRepository dataServicesRepository;

	@Autowired
	private TripleStore tripleStore;
	
	@Autowired
	private IdentifiersService idService;
	
	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private ServiceUtils serviceUtils;
	
    @Autowired
    @Qualifier("clusterers")
    private Map<String, DataService> clusterers;
    
	@Autowired
	private ResourceLoader resourceLoader;

//	@Autowired
//    @Qualifier("rdf-vocabularies")
//    private VocabularyContainer<Vocabulary> vocc;
	
	@Override
	public Class<? extends EnclosedObjectContainer<ClustererDocument, ClustererDocumentResponse, Dataset>> getContainerClass() {
		return ClustererContainer.class;
	}

	@Override 
	public DocumentRepository<ClustererDocument> getRepository() {
		return clustererRepository;
	}

	public class ClustererContainer extends EnclosedObjectContainer<ClustererDocument, ClustererDocumentResponse, Dataset> 
	                                implements DataServiceContainer<ClustererDocument, ClustererDocumentResponse, MappingExecuteState,Dataset>, 
	                                           PublishableContainer<ClustererDocument, ClustererDocumentResponse, MappingExecuteState, MappingPublishState, Dataset>, 
	                                           UpdatableContainer<ClustererDocument, ClustererDocumentResponse, ClustererUpdateRequest>
	                                           {
		private ObjectId clustererId;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		private ClustererContainer(UserPrincipal currentUser, ObjectId clustererId) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.clustererId = clustererId;
			load();
		}
		
		private ClustererContainer(UserPrincipal currentUser, ClustererDocument doc) {
			this(currentUser, doc, null);
		}
		
		private ClustererContainer(UserPrincipal currentUser, ClustererDocument doc, Dataset dataset) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.clustererId = doc.getId();
			this.object = doc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return clustererId;
		}
		
		@Override 
		public DocumentRepository<ClustererDocument> getRepository() {
			return clustererRepository;
		}
		
		@Override
		public ClustererService getService() {
			return ClustererService.this;
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
		
			dataset = datasetOpt.get();
		}

//		@Override
//		public DataService getDataService() {
//			if (object.getClusterer() != null) {
//				return dataServicesRepository.findByIdentifierAndType(object.getClusterer(), DataServiceType.CLUSTERER).orElse(null);
//			} else {
//				return null;
//			}
//		}

		@Override
		public ClustererDocument update(ClustererUpdateRequest ur) throws Exception {

			return update(iac -> {
//				ur.normalize();
//				
				ClustererDocument adoc = iac.getObject();
				adoc.setName(ur.getName());
				adoc.setIdentifier(ur.getIdentifier());
				adoc.setAnnotatorTags(ur.getAnnotatorTags());
				adoc.setControls(ur.getControls());
			
				List<DataServiceParameter> prototypeParameters;
//				
				if (ur.getClustererId() != null) {
					adoc.setClustererId(new ObjectId(ur.getClustererId()));
					adoc.setClusterer(null);
					
					PrototypeContainer pc = prototypeService.getContainer(currentUser, new SimpleObjectIdentifier(new ObjectId(ur.getClustererId())));
					prototypeParameters = pc.getObject().getFields();

				} else {
					adoc.setClustererId(null);
					adoc.setClusterer(ur.getClusterer());
					
					prototypeParameters = clusterers.get(ur.getClusterer()).getFields();
				}
				
//				adoc.setVariant(ur.getVariant());
//				adoc.setAsProperty(ur.getAsProperty());
				adoc.setParameters(ur.getParameters());
////				adoc.setThesaurus(ur.getThesaurus());
//				if (ur.getThesaurusId() != null) {
//					adoc.setThesaurusId(new ObjectId(ur.getThesaurusId()));
//				}
//				adoc.setPreprocess(ur.getPreprocess());
//				adoc.setDefaultTarget(ur.getDefaultTarget());
//				
//				adoc.setTags(ur.getTags());
//				
//				if (adoc.getOnProperty() != null) {
//					adoc.setOnProperty(adoc.getOnProperty());
//					adoc.setKeysMetadata(null);
//					adoc.setElement(null);
//				} else {
					adoc.setOnClass(adoc.getOnClass());
//					adoc.setKeysMetadata(ur.getKeysMetadata());
//					adoc.setElement(ur.getIndexStructure());
//				}	
//				
//				if (adoc.getOnClass() != null && prototypeParameters != null) {
//					updateRequiredParameters(prototypeParameters, adoc);
//				}
//
//				AnnotationEditGroupUpdateRequest aegr = new AnnotationEditGroupUpdateRequest();
//				aegr.setAsProperty(ur.getAsProperty());
//				aegr.setAutoexportable(false);
//				
//				if (adoc.getOnProperty() != null) {
//					aegr.setOnProperty(adoc.getOnProperty());
//				} else {
//					aegr.setOnClass(adoc.getOnClass());
//					aegr.setKeys(ur.getKeys());
////					SPARQLStructure ss = sparqlService.toSPARQL(adoc.getElement(), false);
////					aegr.setSparqlClause(ss.getWhereClause());
//				}
//				
//				Optional<AnnotationEditGroup> aegOpt;
//				
//				if (ur.getAsProperty() != null) { // legacy
//					AnnotationEditGroup aeg = null;
//					aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
//					if (!aegOpt.isPresent()) {
//						aeg = aegService.create(currentUser, dataset, aegr);
//					} else {
//						aeg = aegOpt.get();
//					}
//						
//					adoc.setAnnotatorEditGroupId(aeg.getId());
////					aeg.addAnnotatorId(adoc.getId());
//					
//				} else {
//					if (ur.getTags() == null) {
//						AnnotationEditGroup aeg = null;
//						
//						if (ur.getOnPath() != null) {
//							aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagExistsAndUserId(dataset.getId(), adoc.getOnProperty(), false, new ObjectId(currentUser.getId()));
//						} else {
////							aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagExistsAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), false, new ObjectId(currentUser.getId()));
//							aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagExistsAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), false, new ObjectId(currentUser.getId()));
//						}
//						if (!aegOpt.isPresent()) {
//							aeg = aegService.create(currentUser, dataset, aegr);
//						} else {
//							aeg = aegOpt.get();
//						}
//						
//						AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
//						aegc.update(iaegr -> {
//							AnnotationEditGroup iaeg = iaegr.getObject();
//							iaeg.addAnnotatorId(adoc.getId());
//						});
//						
////						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), adoc.getId(), new ObjectId(currentUser.getId()))) {
////						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), adoc.getId(), new ObjectId(currentUser.getId()))) {
//						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), adoc.getId(), new ObjectId(currentUser.getId()))) {
//							if (xaeg.getId().equals(aeg.getId())) {
//								continue;
//							}
//							
//							if (xaeg.getTag() != null) {
//								AnnotationEditGroupContainer xaegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, xaeg);
//								xaegc.update(iaegr -> {
//									AnnotationEditGroup iaeg = iaegr.getObject();
//									iaeg.removeAnnotatorId(adoc.getId());
//								});	
//								
//								if (xaegc.getObject().getAnnotatorId() == null) {
//									annotationEditGroupRepository.delete(xaegc.getObject());
//								}
//							}
//						}	
//					} else {
//						Set<ObjectId> aegIds = new HashSet<>();
//						for (String tag : ur.getTags()) {
//							AnnotationEditGroup aeg = null;
//							
//							if (ur.getOnPath() != null) {
//								aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagAndUserId(dataset.getId(), adoc.getOnProperty(), tag, new ObjectId(currentUser.getId()));
//							} else {
////								aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), tag, new ObjectId(currentUser.getId()));
//								aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), tag, new ObjectId(currentUser.getId()));
//							}
//							if (!aegOpt.isPresent()) {
//								aegr.setTag(tag);
//								aeg = aegService.create(currentUser, dataset, aegr);
//							} else {
//								aeg = aegOpt.get();
//							}
//							
//							aegIds.add(aeg.getId());
//							
//							AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
//							aegc.update(iaegr -> {
//								AnnotationEditGroup iaeg = iaegr.getObject();
//								iaeg.addAnnotatorId(adoc.getId());
//							});	
//						}
//						
////						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), adoc.getId(), new ObjectId(currentUser.getId()))) {
////						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), adoc.getId(), new ObjectId(currentUser.getId()))) {
//						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), adoc.getId(), new ObjectId(currentUser.getId()))) {
//							if (aegIds.contains(xaeg.getId())) {
//								continue;
//							}
//							
//							if (xaeg.getTag() == null || !ur.getTags().contains(xaeg.getTag())) {
//								AnnotationEditGroupContainer xaegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, xaeg);
//								xaegc.update(iaegr -> {
//									AnnotationEditGroup iaeg = iaegr.getObject();
//									iaeg.removeAnnotatorId(adoc.getId());
//								});
//								
//								if (xaegc.getObject().getAnnotatorId() == null) {
//									annotationEditGroupRepository.delete(xaegc.getObject());
//								}
//							}
//						}	
//					}					
//					
//				
//				}
				
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			
			synchronized (saveSyncString()) {
				clearExecution();
					
				clustererRepository.delete(object);

//				if (object.getAsProperty() != null) { // legacy
//					if (annotatorRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(object.getDatasetUuid(), object.getOnProperty().toArray(new String[] {}), object.getAsProperty(), new ObjectId(currentUser.getId())).isEmpty()) {
//						annotationEditRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(object.getDatasetUuid(), object.getOnProperty(), object.getAsProperty(), new ObjectId(currentUser.getId()));
//						annotationEditGroupRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(object.getDatasetUuid(), object.getOnProperty().toArray(new String[] {}), object.getAsProperty(), new ObjectId(currentUser.getId()));
//					}
//					
//				} else {
//					for (AnnotationEditGroup aeg : annotationEditGroupRepository.findByAnnotatorId(object.getId())) {
//						AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
//						aegc.update(iaegc -> {
//							AnnotationEditGroup iaeg = iaegc.getObject();
//							iaeg.removeAnnotatorId(object.getId());
//						});
//						
//						if (aegc.getObject().getAnnotatorId() == null) {
//							annotationEditRepository.deleteByAnnotationEditGroupId(aeg.getId());
//							annotationEditGroupRepository.delete(aegc.getObject());
//						}
//					}
//					
//
////					for (AnnotatorTag tag : object.getTags()) {
////						if (annotatorRepository.findByDatasetIdAndOnPropertyAndTagsAndUserId(object.getDatasetId(), object.getOnProperty().toArray(new String[] {}), tag, new ObjectId(currentUser.getId())).isEmpty()) {
////							annotationEditRepository.deleteByDatasetUuidAndOnPropertyAndTagAndUserId(object.getDatasetUuid(), object.getOnProperty(), tag, new ObjectId(currentUser.getId()));
////							annotationEditGroupRepository.deleteByDatasetIdAndOnPropertyAndTagAndUserId(object.getDatasetId(), object.getOnProperty().toArray(new String[] {}), tag, new ObjectId(currentUser.getId()));
////						}
////					}
//				}
				return true;
			}
		}
		
		@Override
		public String localSynchronizationString() {
			return (getContainerFileSystemConfiguration().getId().toString() + ":" + getObject().getId().toString()).intern();
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
		public ClustererDocumentResponse asResponse() {
			ClustererDocumentResponse response = new ClustererDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());
	    	response.setIdentifier(object.getIdentifier());
//	    	if (object.getOnProperty() != null) {
//	    		response.setOnProperty(PathElement.onPathElementListAsStringListInverse(object.getOnProperty(), vocc));
//	    	} else if (object.getOnClass() != null) {
	    		response.setOnClass(object.getOnClass());
//	    		response.setElement(mapper.indexStructure2IndexStructureResponse(object.getElement()));
//	    		response.setKeysMetadata(object.getKeysMetadata());
//	    	}
//	    	response.setAsProperty(object.getAsProperty());
	    	response.setClusterer(object.getClusterer());
	    	response.setAnnotatorTags(object.getAnnotatorTags());
	    	response.setControls(object.getControls());
	    	
	    	if (currentUser != null) {
	    		response.setOwnedByUser(currentUser.getId().equals(object.getUserId().toString()));
	    	}
	    	
	    	List<DataServiceParameter> parameterDef = null;
//	    	
	    	if (object.getClustererId() != null) {
	    		response.setClustererId(object.getClustererId().toString());
	    		
	   			Optional<PrototypeDocument> pdoc  = prototypeRepository.findById(object.getClustererId());
	   			if (pdoc.isPresent()) {
	   				response.setClustererName(pdoc.get().getName());
	   				
	   				parameterDef = pdoc.get().getParameters();
	   			}
	    		
	    	} else {
	    		// TODO also for system annotators
	    	}
//	    	
	    	response.setParameters(DataServiceParameter.getShowedParameters(isCurrentUserOwner(), parameterDef, object.getParameters()));
//	    	response.setPreprocess(object.getPreprocess());
//	    	response.setVariant(object.getVariant());
//	    	response.setDefaultTarget(vocc.arrayPrefixize(object.getDefaultTarget()));
//	    	
//	    	response.setTags(object.getTags());
	    	
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	response.copyStates(object, getDatasetTripleStoreVirtuosoConfiguration(), fileSystemConfiguration);
	    	
	        return response;
		}
		
		@Override
		public String getDescription() {
			return object.getName();
		}
		
		@Override
		public TaskType getExecuteTask() {
			return TaskType.CLUSTERER_EXECUTE;
		}

		@Override
		public TaskType getClearLastExecutionTask() {
			return TaskType.CLUSTERER_CLEAR_LAST_EXECUTION;
		}

		@Override
		public TaskType getPublishTask() {
			return TaskType.CLUSTERER_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return TaskType.CLUSTERER_UNPUBLISH;
		}

		@Override
		public TaskType getRepublishTask() {
			return TaskType.CLUSTERER_REPUBLISH;
		}
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByClustererIdAndFileSystemConfigurationId(getObject().getId(), getContainerFileSystemConfiguration().getId(), type).orElse(null);
		}	
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}

		@Override
		public DataService getDataService() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ExecutionOptions buildExecutionParameters() {
			Map<String, Object> params = new HashMap<>();
			Map<String, IndexKeyMetadata> targets = new HashMap<>();
			
//			targets = new HashMap<>();
//			for (IndexKeyMetadata entry : object.getControl().getKeysMetadata()) {
//				targets.put("r" + entry.getIndex(), entry);
//			}
			
			applyParameters(params);
			
			return new ExecutionOptions(params, targets);

		}

		@Override
		public String applyPreprocessToMappingDocument(ExecutionOptions eo) throws Exception { // targets e.g. r0 -> title
			Map<String, Object> params = eo.getParams();
//			Map<String, IndexKeyMetadata> targets = eo.getTargets();
			
			DataServiceRank rank = eo.getRank();// == null ? DataServiceRank.SINGLE : DataServiceRank.MULTIPLE;
			
			String str = null;
			if (object.getClusterer() != null) {
				str = dataServicesService.readMappingDocument(object, params, rank) ;
			} else {
//				str = prototypeRepository.findById(object.getAnnotatorId()).get().getContent() ;
			}
			
			str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getClusterAsResource("").toString());
			
			return str;
		}		



//		@Override
//		public List<ClustererDocument> getClusterers() {
//			List<ClustererDocument> res = new ArrayList<>();
//			res.add(object);
//			
//			return res;
//		}

	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
//	private void updateRequiredParameters(List<DataServiceParameter> params, AnnotatorDocument adoc) {
//		if (params == null || adoc.getKeysMetadata() == null) {
//			return;
//		}
//		
//		Map<String, DataServiceParameter> nameMap = new HashMap<>();
//		Map<Integer, DataServiceParameter> indexMap = new HashMap<>();
//		for (DataServiceParameter dsp : params) {
//			nameMap.put(dsp.getName(), dsp);
//		}
//		
//		for (IndexKeyMetadata ikm : adoc.getKeysMetadata()) {
//			DataServiceParameter dsp = nameMap.get(ikm.getName());
//			ikm.setOptional(!dsp.isRequired());
//
//			indexMap.put(ikm.getIndex(), dsp);
//			
//		}
//		
//		adoc.getElement().updateRequiredParameters(indexMap);
//	}
	
	
	@Override
	public ClustererDocument create(UserPrincipal currentUser, Dataset dataset, ClustererUpdateRequest ur) throws Exception {
//		ur.normalize();
//		
		ClustererDocument adoc = new ClustererDocument(dataset);
		adoc.setUserId(new ObjectId(currentUser.getId()));
		adoc.setName(ur.getName());
		adoc.setIdentifier(ur.getIdentifier());
		adoc.setAnnotatorTags(ur.getAnnotatorTags());
		adoc.setControls(ur.getControls());
//
//		List<String> flatOnPath = null;
//		if (ur.getOnPath() != null) {
//			flatOnPath = PathElement.onPathElementListAsStringList(ur.getOnPath());
//		}
//
//		if (ur.getOnPath() != null) {
//			adoc.setOnProperty(flatOnPath);
//			adoc.setKeysMetadata(null);
//			adoc.setElement(null);
//		} else {
			adoc.setOnClass(ur.getOnClass());
//			adoc.setKeysMetadata(ur.getKeysMetadata());
//			adoc.setElement(ur.getIndexStructure());
//		}
//		
		List<DataServiceParameter> prototypeParameters;
//		
		if (ur.getClustererId() != null) {
			adoc.setClustererId(new ObjectId(ur.getClustererId()));
			adoc.setClusterer(null);
//			
			PrototypeContainer pc = prototypeService.getContainer(currentUser, new SimpleObjectIdentifier(new ObjectId(ur.getClustererId())));
			prototypeParameters = pc.getObject().getFields();
//			
		} else {
			adoc.setClustererId(null);
			adoc.setClusterer(ur.getClusterer());
//			
			prototypeParameters = clusterers.get(ur.getClusterer()).getFields();
		}
//		
//		if (adoc.getOnClass() != null && prototypeParameters != null) {
//			updateRequiredParameters(prototypeParameters, adoc);
//		}
//		
//		adoc.setVariant(ur.getVariant());
//		adoc.setAsProperty(ur.getAsProperty());
		adoc.setParameters(ur.getParameters());
////		adoc.setThesaurus(ur.getThesaurus());
//		if (ur.getThesaurusId() != null) {
//			adoc.setThesaurusId(new ObjectId(ur.getThesaurusId()));
//		}
//		adoc.setPreprocess(ur.getPreprocess());
//		adoc.setDefaultTarget(ur.getDefaultTarget());
//		
//		adoc.setTags(ur.getTags());
//
//		AnnotationEditGroupUpdateRequest aegr = new AnnotationEditGroupUpdateRequest();
//		aegr.setAsProperty(ur.getAsProperty());
//		aegr.setAutoexportable(false);
//		if (ur.getOnPath() != null) {
//			aegr.setOnProperty(flatOnPath);
//		} else {
//			aegr.setOnClass(ur.getOnClass());
//			aegr.setKeys(ur.getKeys());
////			SPARQLStructure ss = sparqlService.toSPARQL(adoc.getElement(), false);
////			aegr.setSparqlClause(ss.getWhereClause());
//		}
//
//		AnnotationEditGroup aeg;
//		Optional<AnnotationEditGroup> aegOpt;
//		
//		if (adoc.getAsProperty() != null) { // legacy
//			if (ur.getOnPath() != null) {
//				aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(dataset.getUuid(), flatOnPath, ur.getAsProperty(), new ObjectId(currentUser.getId()));
//			} else {
//				aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnClassAndAsPropertyAndUserId(dataset.getUuid(), ur.getOnClass(), ur.getAsProperty(), new ObjectId(currentUser.getId()));
//			}
//			
//			if (!aegOpt.isPresent()) {
//				aeg = aegService.create(currentUser, dataset, aegr);
//			} else {
//				aeg = aegOpt.get();
//			}
//			adoc.setAnnotatorEditGroupId(aeg.getId());
////			aeg.addAnnotatorId(adoc.getId());
//		}
//		
		create(adoc);
//		
//		if (adoc.getAsProperty() == null) { 
//			if (ur.getTags() == null) {
//				if (ur.getOnPath() != null) {
//					aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagExistsAndUserId(dataset.getId(), flatOnPath, false, new ObjectId(currentUser.getId()));
//				} else {
////					aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagExistsAndUserId(dataset.getId(), ur.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), false, new ObjectId(currentUser.getId()));
//					aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagExistsAndUserId(dataset.getId(), ur.getOnClass(), ur.getKeys(), false, new ObjectId(currentUser.getId()));
//				}
//				if (!aegOpt.isPresent()) {
//					aeg = aegService.create(currentUser, dataset, aegr);
//				} else {
//					aeg = aegOpt.get();
//				}
//				
//				AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
//				aegc.update(iaegr -> {
//					AnnotationEditGroup iaeg = iaegr.getObject();
//					iaeg.addAnnotatorId(adoc.getId());
//				});
//			} else {
//				for (String tag : ur.getTags()) {
//					if (ur.getOnPath() != null) {
//						aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagAndUserId(dataset.getId(), flatOnPath, tag, new ObjectId(currentUser.getId()));
//					} else {
////						aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagAndUserId(dataset.getId(), ur.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), tag, new ObjectId(currentUser.getId()));
//						aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagAndUserId(dataset.getId(), ur.getOnClass(), ur.getKeys(), tag, new ObjectId(currentUser.getId()));
//					}
//					if (!aegOpt.isPresent()) {
//						aegr.setTag(tag);
//						aeg = aegService.create(currentUser, dataset, aegr);
//					} else {
//						aeg = aegOpt.get();
//					}
//					
//					AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
//					aegc.update(iaegr -> {
//						AnnotationEditGroup iaeg = iaegr.getObject();
//						iaeg.addAnnotatorId(adoc.getId());
//					});					
//				}
//			}
//		} 

		return adoc;
	}
	
	@Override
	public ClustererContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		ClustererContainer ac = new ClustererContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}
	}
	
	@Override
	public ClustererContainer getContainer(UserPrincipal currentUser, ClustererDocument adoc) {
		ClustererContainer ac = new ClustererContainer(currentUser, adoc);

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}	}

	
	@Override
	public ClustererContainer getContainer(UserPrincipal currentUser, ClustererDocument adoc, Dataset dataset) {
		ClustererContainer ac = new ClustererContainer(currentUser, adoc, dataset);

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}
	}
	
	
//	@Override
//	@Async("mappingExecutor")
//	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
//		return serviceUtils.execute(tdescr, wsService);
//	}
	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();

		ClustererContainer ac = (ClustererContainer)tdescr.getContainer();
		ClustererDocument adoc = ac.getObject();
		UserPrincipal currentUser = ac.getCurrentUser();

		TripleStoreConfiguration vc = ac.getDatasetTripleStoreVirtuosoConfiguration();
		
		try {
//			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), ac));
			
			ac.update(iac -> {
				PublishState ips = ((PublishableContainer)iac).getPublishState();
				ips.startDo(pm);
			});
			
			pm.sendMessage(new PublishNotificationObject(ac));
	
			tripleStore.publish(vc, ac);

			pm.complete();
			
			ac.update(iac -> {
				MappingPublishState ips = (MappingPublishState)((PublishableContainer)iac).getPublishState();
				ips.completeDo(pm);
				ips.setExecute(((ExecutableContainer<ClustererDocument,ClustererDocumentResponse,MappingExecuteState,Dataset>)iac).getExecuteState());
//				ips.setAsProperty(adoc.getTripleStoreGraph(resourceVocabulary));
			});

			logger.info("Publication of " + adoc.asResource(resourceVocabulary) + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(ac));
		
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);
			
			try {
				ac.update(iac -> {
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
		
		ClustererContainer ac = (ClustererContainer)tdescr.getContainer();
		ClustererDocument adoc = ac.getObject();
		UserPrincipal currentUser = ac.getCurrentUser();

		TripleStoreConfiguration vc = ac.getDatasetTripleStoreVirtuosoConfiguration();

		try {
			ac.update(iac -> {
				MappingPublishState ips = ((PublishableContainer<ClustererDocument,ClustererDocumentResponse,MappingExecuteState, MappingPublishState,Dataset>)iac).getPublishState();
				ips.startUndo(pm);
			});
			
			pm.sendMessage(new PublishNotificationObject(ac));

			tripleStore.unpublish(vc, ac);

			pm.complete();
			
			ac.update(iac -> {
				PublishableContainer<ClustererDocument, ClustererDocumentResponse, MappingExecuteState, MappingPublishState,Dataset> ipc = (PublishableContainer)iac;
				ExecutableContainer<ClustererDocument, ClustererDocumentResponse, MappingExecuteState,Dataset> iec = (ExecutableContainer<ClustererDocument,ClustererDocumentResponse,MappingExecuteState,Dataset>)iac;
				
				MappingPublishState ips = ipc.getPublishState();
				
				ipc.removePublishState(ips);
				
				MappingExecuteState ies = (MappingExecuteState)iec.getExecuteState();
				MappingExecuteState ipes = ips.getExecute();
				if (ies != null && ipes != null && ies.getExecuteStartedAt().compareTo(ipes.getExecuteStartedAt()) != 0) {
					iec.clearExecution(ipes);
				}
			});
			
//			// update annotation edit group change date
//			if (adoc.getAsProperty() != null) {
//				Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
//				AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
//				
//				aeg.setLastPublicationStateChange(new Date());
//				
//				annotationEditGroupRepository.save(aeg);
//			} else if (adoc.getTags() != null) {
////				for (AnnotatorTag tag : adoc.getTags()) {
////					Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagAndUserId(adoc.getDatasetId(), adoc.getOnProperty(), tag, new ObjectId(currentUser.getId()));
////					AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
////					
////					aeg.setLastPublicationStateChange(new Date());
////					
////					annotationEditGroupRepository.save(aeg);	
////				}
//			}
			
			logger.info("Unpublication of " + adoc.asResource(resourceVocabulary) + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(ac));
		
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);

			try {
				ac.update(iac -> {
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
	
	
	@Autowired
	@Qualifier("date-format")
	private SimpleDateFormat dateFormat;

	
	
	
	private boolean isEqual(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return true;
		} else if (o1 != null && o2 == null) {
			return false;
		} else if (o1 == null && o2 != null) {
			return false;
		} 

		return o1.equals(o2);
		
	}
	
	
	private class Fields implements Comparable<Fields> {
		List<RDFNode> fields;
		
		public Fields() {
			fields = new ArrayList<>();
		}
		
		public void addValue(RDFNode s) {
			fields.add(s);
		}
		
		public List<RDFNode> getValues() {
			return fields;
		}
		
		public String valuesToString() {
			String s = "";
			for (int i = 0; i < fields.size(); i++) {
				if (i > 0) {
					s += " | ";
				}
				RDFNode v = fields.get(i);
				if (v == null) {
					s += "---";
				} else {
					s += v.toString().replaceAll("[\n\r\t]", " ");
				}
			}
			return s;
		}
		
		public int hashCode() {
			return fields.hashCode();
		}
		
		public boolean equals(Object obj) {
			if (!(obj instanceof Fields)) {
				return false;
			}
			
			Fields f = (Fields)obj;
			
			if (fields.size() != f.fields.size()) {
				return false;
			}
			
			for (int i = 0; i < fields.size(); i++) {
				if (fields.get(i) == f.fields.get(i)) {
					continue;
				}
				if (fields.get(i) != null && f.fields.get(i) == null || fields.get(i) == null && f.fields.get(i) != null) {
					return false;
				}
				if (!fields.get(i).equals(f.fields.get(i))) {
					return false;
				}
			}
			
			return true;
		}

		@Override
		public int compareTo(Fields o) {
			
			for (int i = 0; i < Math.min(fields.size(), o.fields.size()); i++) {
				RDFNode f1 = fields.get(i);
				RDFNode f2 = o.fields.get(i);
				
				if (f1 == null && f2 == null) {
					continue;
				} else if (f1 == null && f2 != null) {
					return -1;
				} else if (f1 != null && f2 == null) {
					return 1;
				}
				
				int c = f1.toString().compareTo(f2.toString());
				
				if (c != 0) {
					return c;
				}
					
			}
			
			return 0;
		}
		
	}
	
//	private void buildClusters(Map<String, Set<String>> clustering, org.apache.jena.query.Dataset rdfDataset) {
//		String sparql = 
//				"SELECT ?source ?body " + 
//		        "WHERE { " +  
//		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
//			    " ?v <" + OAVocabulary.hasTarget + "> [ <" + OAVocabulary.hasSource + "> ?source ] . " +
//			    " ?v <http://sw.islab.ntua.gr/annotation/score> ?score . FILTER (?score > 0.0)  " +
//		        " ?v <" + OAVocabulary.hasBody + "> ?body } ";
//
//		
//		try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
//			ResultSet rs = qe.execSelect();
//			
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//					
//				String source = sol.get("source").asResource().toString();
//				String body = sol.get("body").asResource().toString();
//				
////				if (source.equals("http://data.europa.eu/s66/resource/organisations/17b9df8c-9b0e-4c13-a0f4-c692059e630f")) {
////					System.out.println("B " + body);
////				}
////				if (body.equals("http://data.europa.eu/s66/resource/organisations/17b9df8c-9b0e-4c13-a0f4-c692059e630f")) {
////					System.out.println("S " + source);
////				}
//				
//				Set<String> sourceSet = clustering.get(source);
//				Set<String> bodySet = clustering.get(body);
//				
//				Set<String> set = null;
//				if (sourceSet == null) {
//					set = bodySet;
//				} else if (bodySet == null) {
//					set = sourceSet;
//				} else if (bodySet != sourceSet) {
//					set = sourceSet;
//					set.addAll(bodySet);
//					for (String s : bodySet) {
//						clustering.put(s, set);
//					}
//					
//				} else {
//					set = sourceSet;
//				}
//				
//				if (set == null) {
//					set = new TreeSet<>();
//				}
//				
//				if (sourceSet == null) {
//					clustering.put(source, set);
//				}
//				if (bodySet == null) {
//					clustering.put(body, set);
//				}
//				set.add(source);
//				set.add(body);
//			}
//		}
//	}
	
//	private void buildClusters(Map<String, Set<String>> clustering, TripleStoreConfiguration vc) {
//		String sparql = 
//				"SELECT ?source ?body " + 
//		        "WHERE { " +  
//		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
//			    " ?v <" + OAVocabulary.hasTarget + "> [ <" + OAVocabulary.hasSource + "> ?source ] . " +
//			    " ?v <http://sw.islab.ntua.gr/annotation/score> ?score . FILTER (?score > 0.0)  " +
//		        " ?v <" + OAVocabulary.hasBody + "> ?body } ";
//
//		
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
//			ResultSet rs = qe.execSelect();
//			
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//					
//				String source = sol.get("source").asResource().toString();
//				String body = sol.get("body").asResource().toString();
//				
////				if (source.equals("http://data.europa.eu/s66/resource/organisations/17b9df8c-9b0e-4c13-a0f4-c692059e630f")) {
////					System.out.println("B " + body);
////				}
////				if (body.equals("http://data.europa.eu/s66/resource/organisations/17b9df8c-9b0e-4c13-a0f4-c692059e630f")) {
////					System.out.println("S " + source);
////				}
//				
//				Set<String> sourceSet = clustering.get(source);
//				Set<String> bodySet = clustering.get(body);
//				
//				Set<String> set = null;
//				if (sourceSet == null) {
//					set = bodySet;
//				} else if (bodySet == null) {
//					set = sourceSet;
//				} else if (bodySet != sourceSet) {
//					set = sourceSet;
//					set.addAll(bodySet);
//					for (String s : bodySet) {
//						clustering.put(s, set);
//					}
//					
//				} else {
//					set = sourceSet;
//				}
//				
//				if (set == null) {
//					set = new TreeSet<>();
//				}
//				
//				if (sourceSet == null) {
//					clustering.put(source, set);
//				}
//				if (bodySet == null) {
//					clustering.put(body, set);
//				}
//				set.add(source);
//				set.add(body);
//			}
//		}
//	}	

	public static void addClusterElement(Map<String, Set<String>> clustering, String source, String body) {
		Set<String> sourceSet = clustering.get(source);
		Set<String> bodySet = clustering.get(body);
		
		Set<String> set = null;
		if (sourceSet == null) {
			set = bodySet;
		} else if (bodySet == null) {
			set = sourceSet;
		} else if (bodySet != sourceSet) {
			set = sourceSet;
			set.addAll(bodySet);
			for (String s : bodySet) {
				clustering.put(s, set);
			}
			
		} else {
			set = sourceSet;
		}
		
		if (set == null) {
			set = new TreeSet<>();
		}
		
		if (sourceSet == null) {
			clustering.put(source, set);
		}
		if (bodySet == null) {
			clustering.put(body, set);
		}
		set.add(source);
		set.add(body);
	}
	
	public static void buildClusters(Map<String, Set<String>> clustering, org.apache.jena.query.Dataset rdfDataset, String fromClause, TripleStoreConfiguration vc) {
		String sparql = 
				"SELECT ?source ?body ?score " + 
//						"?name " + 
		        "WHERE { " +  
		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
			    " ?v <" + OAVocabulary.hasTarget + "> ?target . ?target  <" + OAVocabulary.hasSource + "> ?source . " +
		        
//			    " ?target <http://sw.islab.ntua.gr/annotation/onBinding> [ <http://sw.islab.ntua.gr/annotation/name> \"normalisedLegalName\" ; <http://sw.islab.ntua.gr/annotation/value> ?name ] . " +

			    " ?v <http://sw.islab.ntua.gr/annotation/score> ?score . FILTER (?score > 0.0)  " +
		        " ?v <" + OAVocabulary.hasBody + "> ?body } ";

		
		try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
					
				String source = sol.get("source").asResource().toString();
				String body = sol.get("body").asResource().toString();

//				String name = sol.get("name").toString();
////				List<String> l = Arrays.asList(new String[] { "univ studi", "univ studi trieste", "univ studi trieste - dipartimento psicologia", "univ studi trieste", "univ trieste", "univ studi trieste", "univ studi trieste - dipartimento energetica", "univ trieste", "univ studi trieste", "univ studi trieste" } );
////				List<String> l = Arrays.asList(new String[] { "univ studi" } );
//				List<String> l = Arrays.asList(new String[] { "centro ricerca energie alternative rinnovabili crear univ florence", "centro ricerche energie alternative rinnovabili univ firenze", "chirurgica univ florence dipartimento area critica medico", "department energy florence univ", "department internal medicine univ florence", "dip scienze agronomiche gestione territorio agroforestale univ studi firenze", "dipartimento energetica sergio stecco univ studi firenze", "dipartimento energetica univ studi trieste", "dipartimento fisiopatologia clinica univ firenze", "dipartimento fisiopatologia clinica univ florence", "dipartimento meccanica tecnologie industriali univ studi firenze", "dipartimento psicologia univ studi trieste", "firenze univ", "inst medicina legale univ deglil studi", "inst patologia zoologia forestale agraria univ firenze", "magnetic resonance centre univ florence", "univ deglil studi inst medicina legale", "univ firenze", "univ firenze centro ricerche energie alternative rinnovabili", "univ firenze dipartimento fisiopatologia clinica", "univ firenze inst patologia zoologia forestale agraria", "univ florence", "univ florence centro ricerca energie alternative rinnovabili crear", "univ florence department chemistry", "univ florence department internal medicine", "univ florence dipartimento area critica medico chirurgica", "univ florence dipartimento fisiopatologia clinica", "univ florence magnetic resonance centre", "univ studi", "univ studi firenze", "univ studi firenze department animal biology genetics", "univ studi firenze dip scienze agronomiche gestione territorio agroforestale", "univ studi firenze dipartimento energetica sergio stecco", "univ studi firenze dipartimento farmacologia", "univ studi firenze dipartimento farmacologia preclinica clinica", "univ studi firenze dipartimento meccanica tecnologie industriali", "univ studi firenze florence", "univ studi trieste", "univ studi trieste dipartimento energetica", "univ studi trieste dipartimento psicologia", "univ trieste" }); 
////				
//				if (l.contains(name)) {
////					System.out.println(name + " > S " + body);
//					
//					String xsparql = 
//							"SELECT ?name " + fromClause +
//					        "WHERE { " +  
//					        " <" + body + "> <http://data.europa.eu/s66#legalName> ?name } "; 
//	
//					
//					try (QueryExecution qe2 = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(xsparql, Syntax.syntaxSPARQL_11))) {
//						String n2 = qe2.execSelect().next().get("name").toString();
////						System.out.println("  " + n2);
//						
//						n2 = n2.toLowerCase();
//						
//						if ( (name.toLowerCase().contains("trieste") && (n2.contains("florence") || n2.contains("firenze"))) ||  
//							 (n2.contains("trieste") && (name.toLowerCase().contains("florence") || name.toLowerCase().contains("firenze"))) ||  
//							 ((!name.toLowerCase().contains("trieste") && !name.toLowerCase().contains("firenze") && !name.toLowerCase().contains("florence")) && (n2.contains("trieste") || n2.contains("florence") || n2.contains("firenze"))) ||
//							 ((!n2.contains("trieste") && !n2.contains("firenze") && !n2.contains("florence")) && (name.toLowerCase().contains("trieste") || name.toLowerCase().contains("florence") || name.toLowerCase().contains("firenze"))) 
//								) {
//							System.out.println("********" + name + " " + n2 + " " + source + " " + body);
//							
//						}
//					}
//					
//				}

//				if (source.equals("http://data.europa.eu/s66/resource/organisations/70591123-552c-42a8-9c17-a6f0244edf23")) {
//					System.out.println("B " + body);
//					
//					String xsparql = 
//							"SELECT ?name " + fromClause +
//					        "WHERE { " +  
//					        " <" + body + "> <http://data.europa.eu/s66#legalName> ?name } "; 
//
//					
//					try (QueryExecution qe2 = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(xsparql, Syntax.syntaxSPARQL_11))) {
//						System.out.println("  " + qe2.execSelect().next().get("name"));
//					}
//				}
//				if (body.equals("http://data.europa.eu/s66/resource/organisations/70591123-552c-42a8-9c17-a6f0244edf23")) {
//					System.out.println("S " + source);
//					String xsparql = 
//							"SELECT ?name " + fromClause +
//					        "WHERE { " +  
//					        " <" + source + "> <http://data.europa.eu/s66#legalName> ?name } "; 
//
//					
//					try (QueryExecution qe2 = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(xsparql, Syntax.syntaxSPARQL_11))) {
//						System.out.println("  " + qe2.execSelect().next().get("name"));
//					}
//				}
				
				addClusterElement(clustering, source, body);
			}
		}
	}

	
	public static void buildClusters1(Map<String, Set<Link>> links, org.apache.jena.query.Dataset rdfDataset, String fromClause, TripleStoreConfiguration vc) {
		String sparql = 
				"SELECT ?source ?body ?score " + 
//						"?name " + 
		        "WHERE { " +  
		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
			    " ?v <" + OAVocabulary.hasTarget + "> ?target . ?target  <" + OAVocabulary.hasSource + "> ?source . " +
		        
//			    " ?target <http://sw.islab.ntua.gr/annotation/onBinding> [ <http://sw.islab.ntua.gr/annotation/name> \"normalisedLegalName\" ; <http://sw.islab.ntua.gr/annotation/value> ?name ] . " +

			    " ?v <http://sw.islab.ntua.gr/annotation/score> ?score . FILTER (?score > 0.0)  " +
		        " ?v <" + OAVocabulary.hasBody + "> ?body } ";

		try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
					
				String source = sol.get("source").asResource().toString();
				String body = sol.get("body").asResource().toString();
				double score = sol.get("score").asLiteral().getDouble();
				
				Set<Link> sl = links.get(source);
				if (sl == null) {
					sl = new TreeSet<>();
					links.put(source, sl);
				}
				sl.add(new Link(body, score));
				
				Set<Link> bl = links.get(body);
				if (bl == null) {
					bl = new TreeSet<>();
					links.put(body, bl);
				}
				bl.add(new Link(source, score));
			}
		}
	}
	
	public static List<Set<String>> finalizeClusters(Map<String, Set<String>> clustering) {
		Set<Set<String>> sclusters = new HashSet<>();
		sclusters.addAll(clustering.values());

		List<Set<String>> clusters = new ArrayList<>();
		clusters.addAll(sclusters);
		Collections.sort(clusters, new ClusterComparator());

		return clusters;
	}
	
	public static void checkClusters(List<Set<String>> clustering, String fromClause, TripleStoreConfiguration vc) {
		for (int i = 0; i < clustering.size(); i++) {
			Set<String> cluster = clustering.get(i);
			String values = "";
			for (String s : cluster) {
				values += "<" + s + "> "; 
			}
			
			String sparql = "SELECT DISTINCT ?wiki " + fromClause + " WHERE { " +
					"?v <https://sage-cordis.ails.ece.ntua.gr/ontology/wikidataUri> ?wiki " +
					"VALUES ?v { " + values + " } }";
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
				ResultSet rs = qe.execSelect();
				
				List<RDFNode> common = new ArrayList<>();
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					common.add(sol.get("wiki"));
				}
				
				if (common.size() > 1) {
					System.out.println(">> CLUSTER " + (i + 1) + " WIKI " + common);
				}
			}
					
		}
	}
	
	
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private class ClusterProperty {
		private String property;
		
		@JsonIgnore
		private RDFNode value;
		
		@JsonProperty("value")
		private String stringValue;
		
		ClusterProperty(String property, RDFNode value) {
			this.property = property;
			this.value = value;
		}
		
		public String getProperty() {
			return property;
		}
		public void setProperty(String property) {
			this.property = property;
		}
		public RDFNode getValue() {
			return value;
		}
		public void setValue(RDFNode value) {
			this.value = value;
		}

		public String getStringValue() {
			if (value != null) {
				return value.toString();
			} else {
				return null;
			}
		}

	}
	
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private class ClusterObject {
		private int size;
		private String name;
		private List<String> members;
		private List<ClusterObject> partitions;
		private List<ClusterProperty> properties;
		
		public ClusterObject(String name, Collection<String> members) {
			this.members = new ArrayList<>();
			this.members.addAll(members);
			this.size = members.size();
		}
		
		public int getSize() {
			return size;
		}

		public List<String> getMembers() {
			return members;
		}

		public List<ClusterObject> getPartition() {
			return partitions;
		}

		public void addPartition(ClusterObject partition) {
			if (partitions == null) {
				partitions = new ArrayList<>();
			}
			
			partitions.add(partition);
		}
		
		public List<ClusterProperty> getProperties() {
			return properties;
		}
		
		public void addProperty(ClusterProperty property) {
			if (properties == null) {
				properties = new ArrayList<>();
			}
			
			properties.add(property);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
	
	private static XSSFColor lightblue1 = new XSSFColor(new byte[] { (byte)153, (byte)230, (byte)255 }, new DefaultIndexedColorMap());
	private static XSSFColor lightblue2 = new XSSFColor(new byte[] { (byte)204, (byte)242, (byte)255 }, new DefaultIndexedColorMap());

	private static XSSFColor lightgreen1 = new XSSFColor(new byte[] { (byte)179, (byte)230, (byte)204 }, new DefaultIndexedColorMap());
	private static XSSFColor lightgreen2 = new XSSFColor(new byte[] { (byte)214, (byte)242, (byte)230 }, new DefaultIndexedColorMap());

	private static XSSFColor lightred1 = new XSSFColor(new byte[] { (byte)255, (byte)153, (byte)153 }, new DefaultIndexedColorMap());
	private static XSSFColor lightred2 = new XSSFColor(new byte[] { (byte)255, (byte)204, (byte)204 }, new DefaultIndexedColorMap());

	private static String getCharForNumber(int i) {
	    return i > 0 && i < 27 ? String.valueOf((char)(i + 'A' - 1)) : null;
	}

	private class ExcelWriter {
		XSSFWorkbook workbook;
		XSSFCreationHelper createHelper;
		XSSFSheet clustersSheet;
		Map<Integer,XSSFSheet> dataSheetMap;
		
		XSSFRow clustersRow = null;
		XSSFCell clustersCell = null;

		XSSFRow dataRow = null;
		XSSFCell dataCell = null;
		
		XSSFCellStyle clusterHeaderStyle;
		XSSFCellStyle clusterHeaderBoldStyle;
		XSSFCellStyle fieldHeaderStyle;
		XSSFCellStyle fieldHeaderBoldStyle;
		XSSFCellStyle defaultStyle;
		XSSFCellStyle defaultBoldStyle;
		XSSFCellStyle evenDataRowStyle, evenDataRowStyle2;
		XSSFCellStyle oddDataRowStyle, oddDataRowStyle2;
		XSSFCellStyle singleDefaultStyle;
		XSSFCellStyle singleFieldHeaderStyle;
		XSSFCellStyle singleClusterHeaderBoldStyle;
		XSSFCellStyle evenDataClusterStyle;
		XSSFCellStyle oddDataClusterStyle;

		int clustersRowIndex = 1;
		Map <Integer, Counter> dataRowIndexMap;
		Map <Integer,Counter> aaMap;

		ClustererContainer oc;
		List<SchemaSelector> controls;
		
		public ExcelWriter(ClustererContainer oc) {
			this.oc = oc;
			this.controls = oc.getObject().getControls();
		
			workbook = new XSSFWorkbook();
			createHelper = workbook.getCreationHelper();
			clustersSheet = workbook.createSheet("Clusters");
			
			dataSheetMap = new HashMap<>();
			dataRowIndexMap = new HashMap<>();
			aaMap = new HashMap<>();
			for (int i = 0 ; i < controls.size(); i++) {
				if (controls.get(i).isClusterKey()) {
					dataSheetMap.put(i, workbook.createSheet("Data-" + controls.get(i).getName()));
					aaMap.put(i, new Counter(1));
					dataRowIndexMap.put(i, new Counter(1));
				}
			}
			
//			dataSheet.protectSheet("");
			
			XSSFFont clusterHeaderBoldFont = ((XSSFWorkbook) workbook).createFont();
			clusterHeaderBoldFont.setFontName("Calibri");
			clusterHeaderBoldFont.setFontHeightInPoints((short) 11);
			clusterHeaderBoldFont.setBold(true);

			XSSFFont clusterHeaderFont = ((XSSFWorkbook) workbook).createFont();
			clusterHeaderFont.setFontName("Calibri");
			clusterHeaderFont.setFontHeightInPoints((short) 11);
			
			clusterHeaderStyle = workbook.createCellStyle();
			clusterHeaderStyle.setFont(clusterHeaderFont);

			clusterHeaderBoldStyle = workbook.createCellStyle();
			clusterHeaderBoldStyle.setFont(clusterHeaderBoldFont);

			XSSFFont fieldHeaderBoldFont = ((XSSFWorkbook) workbook).createFont();
			fieldHeaderBoldFont.setFontName("Calibri");
			fieldHeaderBoldFont.setFontHeightInPoints((short) 11);
			fieldHeaderBoldFont.setColor(IndexedColors.DARK_RED.getIndex());
			fieldHeaderBoldFont.setBold(true);

			XSSFFont fieldHeaderFont = ((XSSFWorkbook) workbook).createFont();
			fieldHeaderFont.setFontName("Calibri");
			fieldHeaderFont.setFontHeightInPoints((short) 11);
			fieldHeaderFont.setColor(IndexedColors.DARK_RED.getIndex());
			
			XSSFFont defaultFont = ((XSSFWorkbook) workbook).createFont();
			defaultFont.setFontName("Calibri");
			defaultFont.setFontHeightInPoints((short) 11);

			XSSFFont defaultBoldFont = ((XSSFWorkbook) workbook).createFont();
			defaultBoldFont.setFontName("Calibri");
			defaultBoldFont.setFontHeightInPoints((short) 11);
			defaultBoldFont.setBold(true);
			
			fieldHeaderStyle = workbook.createCellStyle();
			fieldHeaderStyle.setFont(fieldHeaderFont);

			fieldHeaderBoldStyle = workbook.createCellStyle();
			fieldHeaderBoldStyle.setFont(fieldHeaderBoldFont);

			defaultStyle = workbook.createCellStyle();
			defaultStyle.setFont(defaultFont);

			defaultBoldStyle = workbook.createCellStyle();
			defaultBoldStyle.setFont(defaultBoldFont);

//			defaultStyle.setLocked(true);

			evenDataRowStyle = workbook.createCellStyle();
			evenDataRowStyle.setFillBackgroundColor(IndexedColors.BLACK.index);
			evenDataRowStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			evenDataRowStyle.setFillForegroundColor(lightblue1);
			evenDataRowStyle.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle.setBorderBottom(BorderStyle.THIN);
			evenDataRowStyle.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle.setBorderTop(BorderStyle.THIN);
			evenDataRowStyle.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle.setBorderRight(BorderStyle.THIN);
			evenDataRowStyle.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle.setBorderLeft(BorderStyle.THIN);				
			evenDataRowStyle.setFont(defaultFont);
			evenDataRowStyle.setLocked(true);

			oddDataRowStyle = workbook.createCellStyle();
			oddDataRowStyle.setFillBackgroundColor(IndexedColors.BLACK.index);
			oddDataRowStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			oddDataRowStyle.setFillForegroundColor(lightblue2);
			oddDataRowStyle.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle.setBorderBottom(BorderStyle.THIN);
			oddDataRowStyle.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle.setBorderTop(BorderStyle.THIN);
			oddDataRowStyle.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle.setBorderRight(BorderStyle.THIN);
			oddDataRowStyle.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle.setBorderLeft(BorderStyle.THIN);
			oddDataRowStyle.setFont(defaultFont);
			oddDataRowStyle.setLocked(true);
			
			evenDataRowStyle2 = workbook.createCellStyle();
			evenDataRowStyle2.setFillBackgroundColor(IndexedColors.BLACK.index);
			evenDataRowStyle2.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			evenDataRowStyle2.setFillForegroundColor(lightred1);
			evenDataRowStyle2.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle2.setBorderBottom(BorderStyle.THIN);
			evenDataRowStyle2.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle2.setBorderTop(BorderStyle.THIN);
			evenDataRowStyle2.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle2.setBorderRight(BorderStyle.THIN);
			evenDataRowStyle2.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataRowStyle2.setBorderLeft(BorderStyle.THIN);				
			evenDataRowStyle2.setFont(defaultFont);
			evenDataRowStyle2.setLocked(true);

			oddDataRowStyle2 = workbook.createCellStyle();
			oddDataRowStyle2.setFillBackgroundColor(IndexedColors.BLACK.index);
			oddDataRowStyle2.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			oddDataRowStyle2.setFillForegroundColor(lightred2);
			oddDataRowStyle2.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle2.setBorderBottom(BorderStyle.THIN);
			oddDataRowStyle2.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle2.setBorderTop(BorderStyle.THIN);
			oddDataRowStyle2.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle2.setBorderRight(BorderStyle.THIN);
			oddDataRowStyle2.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataRowStyle2.setBorderLeft(BorderStyle.THIN);
			oddDataRowStyle2.setFont(defaultFont);
			oddDataRowStyle2.setLocked(true);
			
			evenDataClusterStyle = workbook.createCellStyle();
			evenDataClusterStyle.setFillBackgroundColor(IndexedColors.BLACK.index);
			evenDataClusterStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			evenDataClusterStyle.setFillForegroundColor(lightblue1);
			evenDataClusterStyle.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataClusterStyle.setBorderBottom(BorderStyle.THIN);
			evenDataClusterStyle.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataClusterStyle.setBorderTop(BorderStyle.THIN);
			evenDataClusterStyle.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataClusterStyle.setBorderRight(BorderStyle.THIN);
			evenDataClusterStyle.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			evenDataClusterStyle.setBorderLeft(BorderStyle.THIN);				
			evenDataClusterStyle.setFont(defaultFont);
			
			oddDataClusterStyle = workbook.createCellStyle();
			oddDataClusterStyle.setFillBackgroundColor(IndexedColors.BLACK.index);
			oddDataClusterStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			oddDataClusterStyle.setFillForegroundColor(lightred1);
			oddDataClusterStyle.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataClusterStyle.setBorderBottom(BorderStyle.THIN);
			oddDataClusterStyle.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataClusterStyle.setBorderTop(BorderStyle.THIN);
			oddDataClusterStyle.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataClusterStyle.setBorderRight(BorderStyle.THIN);
			oddDataClusterStyle.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			oddDataClusterStyle.setBorderLeft(BorderStyle.THIN);				
			oddDataClusterStyle.setFont(defaultFont);
			
			singleDefaultStyle = workbook.createCellStyle();
			singleDefaultStyle.setFillBackgroundColor(IndexedColors.BLACK.index);
			singleDefaultStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			singleDefaultStyle.setFillForegroundColor(lightblue2);
			singleDefaultStyle.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleDefaultStyle.setBorderBottom(BorderStyle.THIN);
			singleDefaultStyle.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleDefaultStyle.setBorderTop(BorderStyle.THIN);
			singleDefaultStyle.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleDefaultStyle.setBorderRight(BorderStyle.THIN);
			singleDefaultStyle.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleDefaultStyle.setBorderLeft(BorderStyle.THIN);
			singleDefaultStyle.setFont(defaultFont);
			
			singleFieldHeaderStyle = workbook.createCellStyle();
			singleFieldHeaderStyle.setFillBackgroundColor(IndexedColors.BLACK.index);
			singleFieldHeaderStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			singleFieldHeaderStyle.setFillForegroundColor(lightblue2);
			singleFieldHeaderStyle.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleFieldHeaderStyle.setBorderBottom(BorderStyle.THIN);
			singleFieldHeaderStyle.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleFieldHeaderStyle.setBorderTop(BorderStyle.THIN);
			singleFieldHeaderStyle.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleFieldHeaderStyle.setBorderRight(BorderStyle.THIN);
			singleFieldHeaderStyle.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleFieldHeaderStyle.setBorderLeft(BorderStyle.THIN);
			singleFieldHeaderStyle.setFont(fieldHeaderFont);
			
			singleClusterHeaderBoldStyle = workbook.createCellStyle();
			singleClusterHeaderBoldStyle.setFillBackgroundColor(IndexedColors.BLACK.index);
			singleClusterHeaderBoldStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			singleClusterHeaderBoldStyle.setFillForegroundColor(IndexedColors.LIGHT_GREEN.getIndex());
			singleClusterHeaderBoldStyle.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleClusterHeaderBoldStyle.setBorderBottom(BorderStyle.THIN);
			singleClusterHeaderBoldStyle.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleClusterHeaderBoldStyle.setBorderTop(BorderStyle.THIN);
			singleClusterHeaderBoldStyle.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleClusterHeaderBoldStyle.setBorderRight(BorderStyle.THIN);
			singleClusterHeaderBoldStyle.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
			singleClusterHeaderBoldStyle.setBorderLeft(BorderStyle.THIN);
			singleClusterHeaderBoldStyle.setFont(clusterHeaderBoldFont);			

			clustersSheet.setColumnWidth(0, 5000);
			clustersSheet.setDefaultColumnStyle(0, clusterHeaderStyle);
			
			clustersSheet.setColumnWidth(1, 6000);
			clustersSheet.setDefaultColumnStyle(1, fieldHeaderStyle);
			
			clustersSheet.setColumnWidth(3, 22000);
			clustersSheet.setDefaultColumnStyle(2, defaultStyle);
			
			clustersSheet.setColumnWidth(5, 22000);
			
			clustersRow = clustersSheet.createRow(0);
			clustersRow.setRowStyle(defaultBoldStyle);
			
			clustersCell = clustersRow.createCell(0);
			clustersCell.setCellStyle(defaultBoldStyle);
			clustersCell.setCellValue("Cluster");
			
			clustersCell = clustersRow.createCell(1);
			clustersCell.setCellStyle(defaultBoldStyle);
			clustersCell.setCellValue("Property");
			
			clustersCell = clustersRow.createCell(2);
			clustersCell.setCellStyle(defaultBoldStyle);
			clustersCell.setCellValue("Group");
			
			clustersCell = clustersRow.createCell(3);
			clustersCell.setCellStyle(defaultBoldStyle);
			clustersCell.setCellValue("Value");
			
			clustersCell = clustersRow.createCell(4);
			clustersCell.setCellStyle(defaultBoldStyle);
			clustersCell.setCellValue("Mark");
			
			clustersCell = clustersRow.createCell(5);
			clustersCell.setCellStyle(defaultBoldStyle);
			clustersCell.setCellValue("User value");
			
			clustersSheet.createFreezePane(0, 1);
	
			for (XSSFSheet dataSheet : dataSheetMap.values()) {
				dataSheet.setColumnWidth(0, 5000);
				dataSheet.setDefaultColumnStyle(0, defaultStyle);
				
				dataSheet.setDefaultColumnStyle(1, defaultStyle);
	
				dataSheet.setColumnWidth(2, 25000);
				dataSheet.setDefaultColumnStyle(2, defaultStyle);
	
				dataRow = dataSheet.createRow(0);
				dataRow.setRowStyle(defaultBoldStyle);
				
				dataCell = dataRow.createCell(0);
				dataCell.setCellStyle(defaultBoldStyle);
				dataCell.setCellValue("Cluster");
	
				dataCell = dataRow.createCell(1);
				dataCell.setCellStyle(defaultBoldStyle);
				dataCell.setCellValue("Group");
				
				dataCell = dataRow.createCell(2);
				dataCell.setCellStyle(defaultBoldStyle);
				dataCell.setCellValue("URI");
	
	//			List<XSSFColor> controlColor = new ArrayList<>();
	
				for (int jj = 0; jj < controls.size(); jj++) {
					
					dataSheet.setColumnWidth(jj + 3, 10000);
					dataSheet.setDefaultColumnStyle(jj + 3, defaultStyle);
					
					dataCell = dataRow.createCell(jj + 3);
					dataCell.setCellStyle(defaultBoldStyle);
					dataCell.setCellValue(controls.get(jj).getName());
				}
				
				dataSheet.createFreezePane(0, 1);
			}
		}
		
		public void newCluster(int k, Set<String> values, List<Map<Fields, List<String>>> labelToValuesMaps, Map<String, Map<String, Fields>> uriToFieldsMap) {
			clustersRow = clustersSheet.createRow(clustersRowIndex++);
			if (k > 0) {
				clustersRow = clustersSheet.createRow(clustersRowIndex++);
			}
			
			clustersCell = clustersRow.createCell(0);
			clustersCell.setCellStyle(clusterHeaderBoldStyle);

			int clusterSize = values.size();

			clustersCell.setCellValue("CLUSTER " + (k + 1) + " [" + clusterSize + "]");
			
			XSSFRow startClusterRow = clustersRow;
			
			boolean allSingle = true;
			
			for (int j = 0; j < labelToValuesMaps.size(); j++) {
    			Map<Fields, List<String>> labelToValuesMap = labelToValuesMaps.get(j);
    			if (labelToValuesMap.size() > 0) {
    				
    				clustersRow = clustersSheet.createRow(clustersRowIndex++);
    				
    				clustersCell = clustersRow.createCell(0);
					clustersCell.setCellValue("CLUSTER " + (k + 1));
					
    				clustersCell = clustersRow.createCell(1);
   					clustersCell.setCellStyle(fieldHeaderBoldStyle);
    				clustersCell.setCellValue(controls.get(j).getName() + " [" + labelToValuesMap.keySet().size() + "]");
					
		    		for (Map.Entry<Fields, List<String>> fentry : labelToValuesMap.entrySet()) {
		    			Fields fields = fentry.getKey();
		    			List<String> uriList = fentry.getValue();
		    			
		    			boolean single = false;
		    			if (uriList.size() == clusterSize && controls.get(j).getClusterMultiplicity() == DataServiceRank.SINGLE) {
		    				single = true;
		    			} else if (controls.get(j).getClusterMultiplicity() == DataServiceRank.SINGLE) {
		    				allSingle = false;
		    			}
		    			
		    			clustersRow = clustersSheet.createRow(clustersRowIndex++);
		    			if (single) {
							clustersRow.setRowStyle(singleDefaultStyle);
						} else {
							clustersRow.setRowStyle(defaultStyle);
						}
		    			
	    				clustersCell = clustersRow.createCell(0);
		    			if (single) {
		    				clustersCell.setCellStyle(singleDefaultStyle);
						} else {
							clustersCell.setCellStyle(defaultStyle);
						}	    				
						clustersCell.setCellValue("CLUSTER " + (k + 1));
						
	    				clustersCell = clustersRow.createCell(1);
						if (single) {
							clustersCell.setCellStyle(singleFieldHeaderStyle);
						} else {
							clustersCell.setCellStyle(fieldHeaderStyle);
						}
	    				clustersCell.setCellValue(controls.get(j).getName());

		    			clustersCell = clustersRow.createCell(3);
		    			if (single) {
		    				clustersCell.setCellStyle(singleDefaultStyle);
						} else {
							clustersCell.setCellStyle(defaultStyle);
						}			    			
		    			clustersCell.setCellValue(fields.valuesToString() + " [" + uriList.size() + "]");
						
		    			if (single) {
		    				clustersCell = clustersRow.createCell(4);
		    				if (single) {
			    				clustersCell.setCellStyle(singleDefaultStyle);
							} else {
								clustersCell.setCellStyle(defaultStyle);
							}
		    				clustersCell.setCellValue("A");
	    				}
						
						if (controls.get(j).isClusterKey()) {
							Counter aa = aaMap.get(j);
							Counter dataRowIndex = dataRowIndexMap.get(j);
							
							clustersCell = clustersRow.createCell(2);
			    			if (single) {
			    				clustersCell.setCellStyle(singleDefaultStyle);
							} else {
								clustersCell.setCellStyle(defaultStyle);
							}								
							clustersCell.setCellValue((j + 1) + "/" + aa.getValue());
							
		    				XSSFHyperlink link = createHelper.createHyperlink(HyperlinkType.DOCUMENT);
		    				link.setAddress("'Data-" + controls.get(j).getName() +"'!A" + (dataRowIndex.getValue() + 1) + ":" + getCharForNumber(controls.size() + 3) + (dataRowIndex.getValue() + uriList.size()));
		    			    clustersCell.setHyperlink(link);

		    			    XSSFSheet dataSheet = dataSheetMap.get(j);
		    			    
			    			for (String uri : uriList) {
			    				dataRow = dataSheet.createRow(dataRowIndex.getValue());
			    				
			    				dataRowIndex.increase();
			    				
			    				if ((k + 1) % 2 == 0) {
				    				if (aa.getValue() % 2 == 0) {
				    					dataRow.setRowStyle(evenDataRowStyle);
				    				} else {
				    					dataRow.setRowStyle(oddDataRowStyle);
				    				}
			    				} else {
			    					if (aa.getValue() % 2 == 0) {
				    					dataRow.setRowStyle(evenDataRowStyle2);
				    				} else {
				    					dataRow.setRowStyle(oddDataRowStyle2);
				    				}
			    				}

			    				dataCell = dataRow.createCell(0);
								if ((k + 1) % 2 == 0) {
									dataCell.setCellStyle(evenDataClusterStyle);
								} else {
									dataCell.setCellStyle(oddDataClusterStyle);
								}
								dataCell.setCellValue("CLUSTER " + (k + 1));
								
			    				dataCell = dataRow.createCell(1);
			    				if ((k + 1) % 2 == 0) {
				    				if (aa.getValue() % 2 == 0) {
				    					dataCell.setCellStyle(evenDataRowStyle);
				    				} else {
				    					dataCell.setCellStyle(oddDataRowStyle);
				    				}
			    				} else {
			    					if (aa.getValue() % 2 == 0) {
			    						dataCell.setCellStyle(evenDataRowStyle2);
				    				} else {
				    					dataCell.setCellStyle(oddDataRowStyle2);
				    				}
			    				}
			    				dataCell.setCellValue((j + 1) + "/" + aa.getValue());
//			    				dataCell.setCellStyle(evenDataRowStyle);
			    				
			    				dataCell = dataRow.createCell(2);
			    				if ((k + 1) % 2 == 0) {
				    				if (aa.getValue() % 2 == 0) {
				    					dataCell.setCellStyle(evenDataRowStyle);
				    				} else {
				    					dataCell.setCellStyle(oddDataRowStyle);
				    				}
			    				} else {
			    					if (aa.getValue() % 2 == 0) {
			    						dataCell.setCellStyle(evenDataRowStyle2);
				    				} else {
				    					dataCell.setCellStyle(oddDataRowStyle2);
				    				}
			    				}			    				
			    				dataCell.setCellValue(uri);
//			    				dataCell.setCellStyle(evenDataRowStyle);
			    				
			    				XSSFHyperlink externalLink = createHelper.createHyperlink(HyperlinkType.URL);
			    				externalLink.setAddress("http://apps-tomcat.islab.ntua.gr:8888/lodview/queryResource?iri=" + URLEncoder.encode(uri));
			    			    dataCell.setHyperlink(externalLink);
			    			    
			    				Map<String, Fields> ff = uriToFieldsMap.get(uri);
			    				
			    				for (int jj = 0; jj < controls.size(); jj++) {
			    					dataCell = dataRow.createCell(jj + 3);
				    				if ((k + 1) % 2 == 0) {
					    				if (aa.getValue() % 2 == 0) {
					    					dataCell.setCellStyle(evenDataRowStyle);
					    				} else {
					    					dataCell.setCellStyle(oddDataRowStyle);
					    				}
				    				} else {
				    					if (aa.getValue() % 2 == 0) {
				    						dataCell.setCellStyle(evenDataRowStyle2);
					    				} else {
					    					dataCell.setCellStyle(oddDataRowStyle2);
					    				}
				    				}
			    					Fields f = ff.get(controls.get(jj).getName());
			    					if (f != null) {
			    						dataCell.setCellValue(f.valuesToString());
			    					}
			    				}
			    			}

			    			aa.increase();

						}
		    		}
    			}
			}
			
			if (allSingle) {
				startClusterRow.setRowStyle(singleClusterHeaderBoldStyle);
				startClusterRow.getCell(0).setCellStyle(singleClusterHeaderBoldStyle);
			}

		}
		
		public void complete() throws IOException { 
			File file = folderService.createExecutionFile(oc, oc.checkExecuteState(), FileType.xlsx);
			
			FileOutputStream outputStream = new FileOutputStream(file);
			workbook.write(outputStream);
			workbook.close();
		}
	}
	
	private class TextWriter {

		ClustererContainer oc;
		List<SchemaSelector> controls;
		
		PrintStream outputStream;

		public TextWriter(ClustererContainer oc) throws FileNotFoundException {
			this.oc = oc;
			this.controls = oc.getObject().getControls();

			File file = folderService.createExecutionFile(oc, oc.checkExecuteState(), FileType.txt);

			outputStream = new PrintStream(file);

		}
		
		public void newCluster(int k, Set<String> values, List<Map<Fields, List<String>>> labelToValuesMaps, Map<String, Map<String, Fields>> uriToFieldsMap) {
			if (k > 0) {
				outputStream.println("");
			}
			outputStream.println("CLUSTER " + (k + 1) + " [" + values.size() + "]");

    		for (int j = 0; j < labelToValuesMaps.size(); j++) {
//    			aa++;
    			Map<Fields, List<String>> labelToValuesMap = labelToValuesMaps.get(j);
    			if (labelToValuesMap.size() > 0) {
    				outputStream.println("\t" + controls.get(j).getName() + " [" + labelToValuesMap.keySet().size() + "]");
		    		for (Map.Entry<Fields, List<String>> fentry : labelToValuesMap.entrySet()) {
		    			Fields fields = fentry.getKey();
		    			List<String> uriList = fentry.getValue();
		    			outputStream.println("\t\t" + fields.valuesToString() + " [" + uriList.size() + "]" + "\t\t");
		    			
//		    			for (String l : uriList) {
//		    				ssBuffer.append(aa + "\t" + l + "\n");
//		    			}
		    		}
    			}
    		}

		}
		
		public void complete(int total) {
			System.out.println();
			System.out.println("TOTAL " + total);
			outputStream.close();
		}
	}

		
	@Override
	@Async("mappingExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();

		ClustererContainer oc = (ClustererContainer)tdescr.getContainer();
		UserPrincipal user = oc.getCurrentUser();
		
		try {
			Date executeStart = new Date(System.currentTimeMillis());
			
			serviceUtils.clearExecution(oc);
			
			oc.update(iac -> {	
				MappingExecuteState ies = ((ClustererContainer)iac).getExecuteState();
	
				ies.setExecuteState(MappingState.EXECUTING);
				ies.setExecuteStartedAt(executeStart);
				ies.setExecuteMessage("Calculating clusters...");
				ies.setExecuteShards(0);
				ies.setCount(0);
				ies.clearMessages();
			});
		} catch (Exception ex) {
			throw new TaskFailureException(ex, new Date());
		}		

		logger.info("Clusterer " + oc.getPrimaryId() + " starting");
		
		em.sendMessage(new ExecuteNotificationObject(oc));
		
		try (FileSystemRDFOutputHandler outhandler = folderService.createExecutionRDFOutputHandler(oc, shardSize)) {
			ExecutionOptions eo = oc.buildExecutionParameters();

			String str = oc.applyPreprocessToMappingDocument(eo);

			Executor exec = new Executor(outhandler, safeExecute);
			
			folderService.checkCreateExtractTempFolder(oc.getCurrentUser());
			
			try {
		    	DatasetCatalog dcg = schemaService.asCatalog(oc.getEnclosingObject());
		    	String fromClause = schemaService.buildFromClause(dcg);
	
		    	TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		    	
		    	ClustererDocument clusterer = (ClustererDocument)oc.getObject();
				
				Map<String, Set<String>> clustering = new HashMap<>();

				Set<AnnotatorDocument> adocs = new HashSet<>();
				for (String s : clusterer.getAnnotatorTags()) {
					adocs.addAll(annotatorRepository.findByDatasetIdAndTags(clusterer.getDatasetId(), s));
				}

				for (AnnotatorDocument adoc : adocs) {
					AnnotatorContainer ac = annotatorService.getContainer(user, adoc);
					
					ExecuteState es = ac.getExecuteDocument().checkExecuteState(fileSystemConfiguration.getId());
						
					if (es instanceof MappingExecuteState) {
						MappingExecuteState mes = (MappingExecuteState)es;
						
						if (mes.getExecuteShards() != null) {
							for (int i = 0; i < mes.getExecuteShards(); i++) {
								File f = folderService.getExecutionTrigFile(ac, mes, i);
								if (f != null) {
//									System.out.println("Loading " + i) ;
									RDFAccessWrapper ds = RDFAccessWrapper.create(RDFLibrary.JENA);
		
									ds.load(f);
									
									buildClusters(clustering, ((JenaAccessWrapper)ds).getDataset(), fromClause, vc);
//									break;
								}
							}
						}
					}
				}
				
				List<Set<String>> clusters = finalizeClusters(clustering);
//				checkClusters(clusters, fromClause, vc);
	
				List<SchemaSelector> controls = clusterer.getControls();
				
				List<SPARQLStructure> sss = new ArrayList<>();
				for (SchemaSelector control : controls) {
					sss.add(sparqlService.toSPARQL(control.getElement(), AnnotatorDocument.getKeyMetadataMap(control.getKeysMetadata()), false));
				}
				
				exec.setMonitor(em);

				D2RMLModel d2rml = D2RMLModel.readFromString(str);
				
				exec.configureFileExtraction(extractMinSize, folderService.getExtractTempFolder(oc.getCurrentUser()), d2rml.usesCaches() ? restCacheSize : 0);
				
				em.createStructure(d2rml, outhandler);
				em.sendMessage(new ExecuteNotificationObject(oc));
				
				exec.keepSubjects(false);

				em.setStateMessage("Serializing clusters...");
				
				StringBuffer ssBuffer = new StringBuffer();
				
				ExcelWriter excelWriter = new ExcelWriter(oc);
				TextWriter textWriter = new TextWriter(oc);

				int total = 0;
				for (int k = 0; k < clusters.size(); k++) {
					Set<String> values = clusters.get(k);
	
//					ClusterObject co = new ClusterObject(null,values);
					
					total += values.size();
					
					String uris = "";
					for (String v : values) {
						uris += " <" + v + ">";
					}
					
					List<Map<Fields, List<String>>> labelToValuesMaps = new ArrayList<>();
					Map<String, Map<String, Fields>> uriToFieldsMap = new HashMap<>();
					
					for (int t = 0; t < sss.size(); t++) {
						if (Thread.interrupted()) {
							Exception ex = new InterruptedException("The task was interrupted.");
							em.currentConfigurationFailed(ex);
							throw ex;
						}
						
						SPARQLStructure ss = sss.get(t);
								
						String lsparql = 
								"SELECT ?c_0 " + ss.returnVariables(true, false) + " " + 
						        fromClause + 
						        " WHERE { " + 
						        ss.whereClause() + " " + ss.filterClauses(true, true, false) + " " + ss.predefinedValueClauses() + // fixed values given by the annotator should go at end of query after bind
						        " VALUES ?c_0 { " + uris + " } " +
						        "}" ;
//						        (ss.isGroupBy() ? ss.getGroupByClause() + ss.returnVariables(true, false) : "");
						
						
//						System.out.println(lsparql);
//						System.out.println(QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11));
//						System.out.println(ss.getKeys());
		
//						Map<String, List<Fields>> uriToFieldsMap = new HashMap<>();
						Map<Fields, List<String>> labelToValuesMap = new TreeMap<>();
						
						labelToValuesMaps.add(labelToValuesMap);
						
						try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
							ResultSet rs = qe.execSelect();
							
							while (rs.hasNext()) {
								QuerySolution sol = rs.next();
									
								String uri = sol.get("c_0").toString();
								
								Fields fields = new Fields();
								for (String v : ss.getKeys()) {
									if (sol.get("lexicalValue_" + v) != null) {
										fields.addValue(sol.get("lexicalValue_" + v));
									} else {
										fields.addValue(null);
									}
								}
								
								Map<String,Fields> uriFields = uriToFieldsMap.get(uri);
								if (uriFields == null) {
									uriFields = new HashMap<>();
									uriToFieldsMap.put(uri, uriFields);
								}
								uriFields.put(controls.get(t).getName(), fields);
								
								List<String> array = labelToValuesMap.get(fields);
								if (array == null) {
									array = new ArrayList<>();
									labelToValuesMap.put(fields, array);
								}
								array.add(uri);
							}
						}
						
//						for (Map.Entry<Fields, List<String>> entry : labelToValuesMap.entrySet()) {
//		
//							ClusterObject pco = new ClusterObject(controls.get(t).getName(), entry.getValue());
//							co.addPartition(pco);
//							
//			    			for (int j = 0; j < control.getKeysMetadata().size(); j++) {
//		    					pco.addProperty(new ClusterProperty(control.getKeysMetadata().get(j).getName(), entry.getKey().fields.get(j)));
//			    			}
//						}
					}
					
					excelWriter.newCluster(k, values, labelToValuesMaps, uriToFieldsMap);
					textWriter.newCluster(k, values, labelToValuesMaps, uriToFieldsMap);
					
		    		
//		    		Map<String, Counter> wordMap = new HashMap<>();
//		    		List<String[]> labelWords = new ArrayList<>();
//		    		Map<String, Integer> wordIndexMap = new HashMap<>();
//		    		Map<Integer, String> indexWordMap = new HashMap<>();
//		    		
//		    		for (Map.Entry<Fields, List<String>> fentry : labelToValuesMap.entrySet()) {
////		    			System.out.println((fentry.getKey().getValues().size() > 1 ? fentry.getKey().getValues() : fentry.getKey().getValues().get(0)) + " >> " + fentry.getValue().size());
//		    			System.out.println((fentry.getKey().getValues().size() > 1 ? fentry.getKey().getValues() : fentry.getKey().getValues().get(0)));
//		    			
//		    			String label = fentry.getKey().getValues().get(0).toString();
//		    			String[] words = label.split(" ");
//		    			labelWords.add(words);
//		    			for (String w : words) {
//			    			Counter cc = wordMap.get(label);
//			    			if (cc == null) {
//			    				cc = new Counter(0);
//			    				wordMap.put(w, cc);
//			    				int index = wordIndexMap.size();
//			    				wordIndexMap.put(w, index);
//			    				indexWordMap.put(index, w);
//			    			}
//			    			cc.increase();
//		    			}
//		    		}	
//		    		
//		    		System.out.println("COMPUTATION");
//		    		System.out.println(wordMap);
//		    		
//		    		boolean[][] matrix = new boolean[wordIndexMap.size()][wordIndexMap.size()];
//		    		
//		    		for (String[] words1 : labelWords) {
//		    			System.out.println(Arrays.toString(words1));
//		    			for (String[] words2 : labelWords) {
//		    				
//		    				for (String w1 : words1) {
//		    					int i1 = wordIndexMap.get(w1);
//		    					for (String w2 : words2) {
//		    						int i2 = wordIndexMap.get(w2);
//		    						
//		    						matrix[i1][i2] = true;
//		    					}
//		    				}
//		    			}
//		    		}
//		    		
//		    		for (int i = 0; i < matrix.length; i++) {
//		    			for (int j = i + 1; j < matrix.length; j++) {
//		    				if (!matrix[i][j]) {
//		    					System.out.println(indexWordMap.get(i) + " --- " + indexWordMap.get(j));
//		    				}
//		    			}
//		    		}
		    		
						
//					ObjectMapper mapper = new ObjectMapper();
//					String clusterJson = mapper.writeValueAsString(co);
//					
//					Map<String, Object> params = new HashMap<>();
//					params.put("iiclusterjson", clusterJson);
//					params.put("iiclustersize", clusters.size());
//
//					exec.partialExecute(d2rml, params);
	
				}
				
				excelWriter.complete();
				textWriter.complete(total);
				
				exec.completeExecution();
				
				em.setStateMessage(null);
				
				em.complete();
			
				oc.update(iac -> {			    
					MappingExecuteState ies = ((ClustererContainer)iac).getExecuteState();

					ies.setExecuteState(MappingState.EXECUTED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(outhandler.getShards());
//					ies.setCount(outhandler.getTotalItems());
//					ies.setCount(subjects.size());
					
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getContent().getProgress());
					ies.setExecuteMessage(null);
				});

				em.sendMessage(new ExecuteNotificationObject(oc));

				logger.info("Clusterer executed -- id: " + oc.getPrimaryId() + ", shards: " + outhandler.getShards());

				if (outhandler.getTotalItems() > 0) {
					try {
						serviceUtils.zipExecution(oc, outhandler.getShards());
					} catch (Exception ex) {
						ex.printStackTrace();
						
						logger.info("Zipping clusterer execution failed -- id: " + oc.getPrimaryId());
					}
				}
				
				return new AsyncResult<>(em.getCompletedAt());
//
			} catch (Exception ex) {
				logger.info("Clusterer failed -- id: " + oc.getPrimaryId());
				
				em.currentConfigurationFailed(ex);

				throw ex;
//			} finally {
//				exec.finalizeFileExtraction();
//				
//				try {
//					if (em != null) {
//						em.close();
//					}
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			em.complete(ex);

			try {
				oc.update(iac -> {			    
					MappingExecuteState ies = ((ClustererContainer)iac).getExecuteState();
	
					ies.setExecuteState(MappingState.EXECUTION_FAILED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(0);
					ies.setCount(0);
					ies.setMessage(em.getFailureMessage());
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getContent().getProgress());
					ies.setExecuteMessage(null);
				});
			} catch (Exception iex) {
				throw new TaskFailureException(iex, em.getCompletedAt());
			}
			
			em.sendMessage(new ExecuteNotificationObject(oc));
			
			throw new TaskFailureException(ex, em.getCompletedAt());
		}

	}
	

	public ValueResponseContainer<ValueAnnotation> clusterview(ClustererContainer ac, int page) throws Exception {

		Map<String, Set<String>> clustering = new HashMap<>();
		
		ExecuteState es = ac.getExecuteDocument().checkExecuteState(fileSystemConfiguration.getId());
			
		if (es instanceof MappingExecuteState) {
			MappingExecuteState mes = (MappingExecuteState)es;
			
			if (mes.getExecuteShards() != null) {
				for (int i = 0; i < mes.getExecuteShards(); i++) {
					File f = folderService.getExecutionTrigFile(ac, mes, i);
					if (f != null) {
						System.out.println("Loading " + i) ;
						RDFAccessWrapper ds = RDFAccessWrapper.create(RDFLibrary.JENA);

						ds.load(f);
						
						buildClusters(clustering, ((JenaAccessWrapper)ds).getDataset(),"",null);
					}
				}
			}
		}
		
		ClustererDocument adoc = ac.getObject();

    	DatasetCatalog dcg = schemaService.asCatalog(ac.getEnclosingObject());
    	String fromClause = schemaService.buildFromClause(dcg);

    	TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		Set<String> all = new TreeSet<>();
//		int i = 1;
		Set<Set<String>> sclusters = new HashSet<>();
		sclusters.addAll(clustering.values());

		List<Set<String>> clusters = new ArrayList<>();
		clusters.addAll(sclusters);
		Collections.sort(clusters, new ClusterComparator());

		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
		vrc.setTotalCount(clusters.size());

		List<ValueAnnotation> vaList = new ArrayList<>();
		
//		String cExtra = "";
		String wExtra = "";
		String selectVars = "";
		List<String> selectVarsList = new ArrayList<>();

//    	List<List<PathElement>> cPaths = new ArrayList<>();
//		if (adoc.getControlProperties() != null) {
//			for (int j = 0; j < adoc.getControlProperties().size(); j++) {
//				ControlProperty extra = adoc.getControlProperties().get(j);
//				List<PathElement> extraPath = PathElement.onPathElementListAsStringListInverse(extra.getOnProperty(), null); 
//				cPaths.add(extraPath);
////			}
//			
////			for (int j = 0; j < cPaths.size(); j++) {
////				List<PathElement> extraPath = cPaths.get(j);
//				String pew = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(extraPath), "?VAR_ZZZ_" + j + "_");
////				wExtra += " OPTIONAL { ?s " + pew + "?VAR_ZZZ_" + j + " } ";
//				wExtra += (extra.isOptional() ? " OPTIONAL" : " ") + " { ?s " + pew + "?VAR_ZZZ_" + j + " } ";
//				
//				List<PathElement> extraResultPath = new ArrayList<>();
//				extraResultPath.add(extraPath.get(extraPath.size() - 1));
////				String pec = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(extraPath), "?VAR_ZZZ_" + j + "_");
//				
////				cExtra += " ?s " + pec + "?VAR_ZZZ_" + j ;
//				
//				selectVars += "?VAR_ZZZ_" + j ;
//				selectVarsList.add("?VAR_ZZZ_" + j );
//			}
//
//		}
		
//		System.out.println(cExtra);
//		System.out.println(wExtra);
		
		Map<Integer,Counter> histogram = new TreeMap<>();
		int cj = 1;
		for (Set<String> values : clusters) {
//			System.out.println(i++ + " " + values);
			all.addAll(values);
			
			String s = "";
			for (String v : values) {
				s += " <" + v + ">";
			}
			
//			String lsparql = 
//					"SELECT DISTINCT ?label " + 
//			        fromClause +
//			        "WHERE { " +  
//			        " ?v <http://data.europa.eu/s66#legalName> ?label . " +
//			        " VALUES ?v { " + s + " }  } ";

			String lsparql = 
					"SELECT ?label (GROUP_CONCAT(?fp;separator=\"|\") AS ?gfp) (GROUP_CONCAT(?pic;separator=\"|\") AS ?gpic) " + 
			        fromClause +
			        "WHERE { " +  
			        " ?v <http://data.europa.eu/s66#legalName> ?label . " +
			        " OPTIONAL { ?v <http://purl.org/dc/terms/identifier> ?ofp } . " +
			        " OPTIONAL { ?v <http://data.europa.eu/s66#identifier> ?pic } . " +
			        " VALUES ?v { " + s + " }  " +
			        " BIND(if(bound(?ofp),?ofp,\"FPH\") AS ?fp) } " +
			        " GROUP BY ?label" ;
			
			System.out.println(QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11));
			
			System.out.println("");
			System.out.println("CLUSTER " + cj++ + " [" + values.size() + "]" );
			
			List<String> labels = new ArrayList<>();
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
						
					String label = sol.get("label").toString();
					Object gfp = sol.get("gfp");
					
					Set<String> fpSet = new TreeSet<>();
					if (gfp != null) {
						for (String v : gfp.toString().split("\\|")) {
							if (v.length() > 0) {
								fpSet.add(v.substring(0, 3));
							}
						}
					}
					
					Object gpic = sol.get("gpic");
					Set<String> picSet = new TreeSet<>();
					if (gpic != null) {
						for (String v : gpic.toString().split("\\|")) {
							if (v.length() > 0) {
								picSet.add(v);
							}
						}
					}
					
//					labels.add(label);
					System.out.println(label + " " + (fpSet.size() > 0 ? fpSet : "") + " " + (picSet.size() > 0 ? picSet : "") );
				}
			}
			
//			System.out.println(labels);
			
			Counter cc = histogram.get(values.size());
			if (cc == null) {
				cc = new Counter(0);
				histogram.put(values.size(), cc);
			}
			cc.increase();
		}
		vrc.setDistinctSourceTotalCount(all.size());

		for (Map.Entry<Integer, Counter> entry : histogram.entrySet()) {
			System.out.println(entry.getKey() + ";" + entry.getValue().getValue());
		}
		
		for (int k = (page - 1)*pageSize; k < Math.min(clusters.size(), page*pageSize) ; k++) {
			Set<String> values = clusters.get(k);

			String uris = "";
			for (String v : values) {
				uris += " <" + v + ">";
			}
			
			
			String lsparql = 
					"SELECT ?s " + selectVars + " " + 
			        fromClause +
			        "WHERE { " +  
			        wExtra + 
			        " VALUES ?s { " + uris + " }  } ";
			
//			System.out.println(QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11));

			Map<Fields, List<String>> labelToValuesMap = new HashMap<>();
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
						
					String uri = sol.get("s").toString();
					
					Fields fields = new Fields();
					for (String v : selectVarsList) {
						if (sol.get(v) != null) {
							fields.addValue(sol.get(v));
						} else {
							fields.addValue(null);
						}
					}
					
					List<String> array = labelToValuesMap.get(fields);
					if (array == null) {
						array = new ArrayList<>();
						labelToValuesMap.put(fields, array);
					}
					array.add(uri);
				}
			}
			
//			System.out.println(labelToValuesMap);
			
			ValueAnnotation va = new ValueAnnotation();
			vaList.add(va);
			
			va.setCount(clusters.get(k).size());
			
			for (Map.Entry<Fields, List<String>> entry : labelToValuesMap.entrySet()) {
				ValueAnnotationDetail vad = new ValueAnnotationDetail();
				vad.setValueList(entry.getValue());

				Model model = ModelFactory.createDefaultModel();

//    			for (int j = 0; j < adoc.getControlProperties().size(); j++) {
//    				List<String> cp = adoc.getControlProperties().get(j).getOnProperty();
//    				if (entry.getKey().fields.get(j) != null) {
//    					model.add(ResourceFactory.createResource(), ResourceFactory.createProperty(cp.get(cp.size() - 1)), entry.getKey().fields.get(j));
//    				}
//    			}
    			
    			Map<String, Object> jn = apiUtils.jsonLDFrame(model, new HashMap<>());
    			vad.setControlGraph(jn);
    			
				va.getDetails().add(vad);
			}
			
		}
		
		vrc.setValues(vaList);

		return vrc;
    }
	
	static class ClusterComparator implements Comparator<Set<String>> {
	    @Override
	    public int compare(Set<String> a, Set<String> b) {
	        return Integer.compare(b.size(), a.size());
	    }
	}
	
	@Override
	public ListPage<ClustererDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, page);
	}
	
	@Override
	public ListPage<ClustererDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(clustererRepository.find(userId, dataset, null, database.getId()));
		} else {
			return ListPage.create(clustererRepository.find(userId, dataset, null, database.getId(), page));
		}
	}
	
	public void executionResultsToModel(org.apache.jena.query.Dataset ds, UserPrincipal currentUser, List<ObjectId> clustererIds) throws IOException {
		if (ds == null) {
			return;
		}
		
		if (clustererIds != null) {
			for (ObjectId id : clustererIds) {
				ClustererContainer ac = this.getContainer(null, new SimpleObjectIdentifier(id));
				if (ac == null) {
					continue;
				}
	
				ClustererDocument doc = ac.getObject();
	
				if (currentUser == null) {
					currentUser = userService.getContainer(null, new SimpleObjectIdentifier(doc.getUserId())).asUserPrincipal();
				}
	
				MappingExecuteState es = doc.getExecuteState(fileSystemConfiguration.getId());
		
				if (es.getExecuteState() == MappingState.EXECUTED) {
					if (es.getExecuteShards() != null) {
				        for (int i = 0; i < es.getExecuteShards(); i++) {
				        	File file = folderService.getClustererExecutionTrigFile(currentUser, ac.getEnclosingObject(), doc, es, i);
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
