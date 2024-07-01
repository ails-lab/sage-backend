package ac.software.semantic.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.IntStream;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SizeAction;

import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.PropertyValue;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.payload.notification.PublishNotificationObject;
import ac.software.semantic.payload.request.AnnotationEditGroupUpdateRequest;
import ac.software.semantic.payload.request.AnnotatorUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.AnnotatorDocumentResponse;
import ac.software.semantic.payload.response.MappingResponse;
import ac.software.semantic.payload.response.ResultCount;
import ac.software.semantic.payload.response.ValueResponse;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotatorContext;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ConditionInstruction;
import ac.software.semantic.model.ControlProperty;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.PrepareState;
import ac.software.semantic.model.constants.state.ThesaurusLoadState;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.AnnotatorPublishState;
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
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.AnnotationEditRepository;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.root.DataServiceRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService.AnnotationEditGroupContainer;
import ac.software.semantic.service.AnnotationUtils.AnnotationUtilsContainer;
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
import ac.software.semantic.service.lookup.AnnotatorLookupProperties;
import ac.software.semantic.service.monitor.GenericMonitor;
import edu.ntua.isci.ac.common.db.rdf.RDFLibrary;
import edu.ntua.isci.ac.common.utils.Counter;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;
import edu.ntua.isci.ac.d2rml.model.map.ValueMap.TermMapType;
import edu.ntua.isci.ac.d2rml.monitor.SimpleMonitor;
import edu.ntua.isci.ac.d2rml.output.RDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.StringOutputHandler;
import edu.ntua.isci.ac.d2rml.output.JenaStringRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.StringDistances;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import smile.clustering.HierarchicalClustering;
import smile.clustering.XMeans;
import smile.clustering.linkage.Linkage;
import smile.clustering.linkage.SingleLinkage;
import smile.clustering.linkage.UPGMALinkage;
import smile.clustering.linkage.CompleteLinkage;
import smile.math.distance.Distance;
import smile.math.distance.EuclideanDistance;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;


@Service
public class AnnotatorService implements ExecutingPublishingService<AnnotatorDocument, AnnotatorDocumentResponse>, 
										 EnclosedCreatableLookupService<AnnotatorDocument, AnnotatorDocumentResponse, AnnotatorUpdateRequest, Dataset, AnnotatorLookupProperties>,
                                         IdentifiableDocumentService<AnnotatorDocument, AnnotatorDocumentResponse> {

	private Logger logger = LoggerFactory.getLogger(AnnotatorService.class);
	
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
	private ModelMapper mapper;
	
	@Autowired
	private FolderService folderService;

	@Autowired
	private AnnotationUtils annotationUtils;
	
	@Autowired
	private APIUtils apiUtils;

	@Lazy
	@Autowired
	private UserService userService;

    @Autowired
    @Qualifier("annotators")
    private Map<String, DataService> annotators;

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;

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

	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private AnnotationEditRepository annotationEditRepository;

	@Autowired
	private AnnotationEditGroupRepository annotationEditGroupRepository;

	@Autowired
	private AnnotationEditGroupService aegService;

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
    @Qualifier("preprocess-functions")
    private Map<Resource, List<String>> functions;
    
	@Autowired
	private ResourceLoader resourceLoader;

	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;
	
	@Override
	public Class<? extends EnclosedObjectContainer<AnnotatorDocument, AnnotatorDocumentResponse, Dataset>> getContainerClass() {
		return AnnotatorContainer.class;
	}

	@Override 
	public DocumentRepository<AnnotatorDocument> getRepository() {
		return annotatorRepository;
	}

	public class AnnotatorContainer extends EnclosedObjectContainer<AnnotatorDocument, AnnotatorDocumentResponse, Dataset> 
	                                implements DataServiceContainer<AnnotatorDocument, AnnotatorDocumentResponse, MappingExecuteState,Dataset>, 
	                                           PublishableContainer<AnnotatorDocument, AnnotatorDocumentResponse, MappingExecuteState, AnnotatorPublishState, Dataset>, 
	                                           UpdatableContainer<AnnotatorDocument, AnnotatorDocumentResponse, AnnotatorUpdateRequest>, 
	                                           PreparableContainer,
	                                           AnnotationContainerBase {
		private ObjectId annotatorId;
		
		private FileSystemConfiguration containerFileSystemConfiguration;
	
		private AnnotatorContainer(UserPrincipal currentUser, ObjectId annotatorId) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.annotatorId = annotatorId;
			load();
		}
		
		private AnnotatorContainer(UserPrincipal currentUser, AnnotatorDocument adoc) {
			this(currentUser, adoc, null);
		}
		
		private AnnotatorContainer(UserPrincipal currentUser, AnnotatorDocument adoc, Dataset dataset) {
			containerFileSystemConfiguration = fileSystemConfiguration;
			this.currentUser = currentUser;
			
			this.annotatorId = adoc.getId();
			this.object = adoc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return annotatorId;
		}
		
		@Override 
		public DocumentRepository<AnnotatorDocument> getRepository() {
			return annotatorRepository;
		}
		
		@Override
		public AnnotatorService getService() {
			return AnnotatorService.this;
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

		@Override
		public DataService getDataService() {
			if (object.getAnnotator() != null) {
				return dataServicesRepository.findByIdentifierAndType(object.getAnnotator(), DataServiceType.ANNOTATOR).orElse(null);
			} else {
				return null;
			}
		}

		@Override
		public AnnotatorDocument update(AnnotatorUpdateRequest ur) throws Exception {

			return update(iac -> {
				ur.normalize();
				
				AnnotatorDocument adoc = iac.getObject();
				adoc.setName(ur.getName());
				adoc.setIdentifier(ur.getIdentifier());
			
				List<DataServiceParameter> prototypeParameters;
				
				if (ur.getAnnotatorId() != null) {
					adoc.setAnnotatorId(new ObjectId(ur.getAnnotatorId()));
					adoc.setAnnotator(null);
					
					PrototypeContainer pc = prototypeService.getContainer(currentUser, new SimpleObjectIdentifier(new ObjectId(ur.getAnnotatorId())));
					prototypeParameters = pc.getObject().getFields();

				} else {
					adoc.setAnnotatorId(null);
					adoc.setAnnotator(ur.getAnnotator());
					
					prototypeParameters = annotators.get(ur.getAnnotator()).getFields();
				}
				adoc.setVariant(ur.getVariant());
				adoc.setAsProperty(ur.getAsProperty());
				adoc.setParameters(ur.getParameters());
//				adoc.setThesaurus(ur.getThesaurus());
				if (ur.getThesaurusId() != null) {
					adoc.setThesaurusId(new ObjectId(ur.getThesaurusId()));
				}
				adoc.setPreprocess(ur.getPreprocess());
				adoc.setDefaultTarget(ur.getDefaultTarget());
				adoc.setBodyProperties(ur.getBodyProperties());
				adoc.setControl(ur.getControl());
				
				adoc.setTags(ur.getTags());
				
				if (adoc.getOnProperty() != null) {
					adoc.setOnProperty(adoc.getOnProperty());
//					adoc.setKeysMetadata(null);
//					adoc.setElement(null);
					adoc.setStructure(null);
				} else {
					adoc.setOnClass(adoc.getOnClass());
//					adoc.setKeysMetadata(ur.getKeysMetadata());
//					adoc.setElement(ur.getIndexStructure());
					adoc.setStructure(ur.getStructure());
				}	
				
				if (adoc.getOnClass() != null && prototypeParameters != null) {
					updateRequiredParameters(prototypeParameters, adoc);
				}

				AnnotationEditGroupUpdateRequest aegr = new AnnotationEditGroupUpdateRequest();
				aegr.setAsProperty(ur.getAsProperty());
				aegr.setAutoexportable(false);
				
				if (adoc.getOnProperty() != null) {
					aegr.setOnProperty(adoc.getOnProperty());
				} else {
					aegr.setOnClass(adoc.getOnClass());
					aegr.setKeys(ur.getStructure() != null ? ur.getStructure().getKeys() : null);
//					SPARQLStructure ss = sparqlService.toSPARQL(adoc.getElement(), false);
//					aegr.setSparqlClause(ss.getWhereClause());
				}
				
				Optional<AnnotationEditGroup> aegOpt;
				
				if (ur.getAsProperty() != null) { // legacy
					AnnotationEditGroup aeg = null;
					aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
					if (!aegOpt.isPresent()) {
						aeg = aegService.create(currentUser, dataset, aegr);
					} else {
						aeg = aegOpt.get();
					}
						
					adoc.setAnnotatorEditGroupId(aeg.getId());
//					aeg.addAnnotatorId(adoc.getId());
					
				} else {
					if (ur.getTags() == null) {
						AnnotationEditGroup aeg = null;
						
						if (adoc.getOnProperty() != null) {
							aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagExistsAndUserId(dataset.getId(), adoc.getOnProperty(), false, new ObjectId(currentUser.getId()));
						} else {
//							aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagExistsAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), false, new ObjectId(currentUser.getId()));
							aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagExistsAndUserId(dataset.getId(), adoc.getOnClass(), ur.getStructure() != null ? ur.getStructure().getKeys() : null, false, new ObjectId(currentUser.getId()));
						}
						if (!aegOpt.isPresent()) {
							aeg = aegService.create(currentUser, dataset, aegr);
						} else {
							aeg = aegOpt.get();
						}
						
						AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
						aegc.update(iaegr -> {
							AnnotationEditGroup iaeg = iaegr.getObject();
							iaeg.addAnnotatorId(adoc.getId());
						});
						
//						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), adoc.getId(), new ObjectId(currentUser.getId()))) {
//						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), adoc.getId(), new ObjectId(currentUser.getId()))) {
						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), adoc.getId(), new ObjectId(currentUser.getId()))) {
							if (xaeg.getId().equals(aeg.getId())) {
								continue;
							}
							
							if (xaeg.getTag() != null) {
								AnnotationEditGroupContainer xaegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, xaeg);
								xaegc.update(iaegr -> {
									AnnotationEditGroup iaeg = iaegr.getObject();
									iaeg.removeAnnotatorId(adoc.getId());
								});	
								
								if (xaegc.getObject().getAnnotatorId() == null) {
									annotationEditGroupRepository.delete(xaegc.getObject());
								}
							}
						}	
					} else {
						Set<ObjectId> aegIds = new HashSet<>();
						for (String tag : ur.getTags()) {
							AnnotationEditGroup aeg = null;
							
							if (adoc.getOnProperty() != null) {
								aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagAndUserId(dataset.getId(), adoc.getOnProperty(), tag, new ObjectId(currentUser.getId()));
							} else {
//								aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), tag, new ObjectId(currentUser.getId()));
								aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagAndUserId(dataset.getId(), adoc.getOnClass(), ur.getStructure() != null ? ur.getStructure().getKeys() : null, tag, new ObjectId(currentUser.getId()));
							}
							if (!aegOpt.isPresent()) {
								aegr.setTag(tag);
								aeg = aegService.create(currentUser, dataset, aegr);
							} else {
								aeg = aegOpt.get();
							}
							
							aegIds.add(aeg.getId());
							
							AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
							aegc.update(iaegr -> {
								AnnotationEditGroup iaeg = iaegr.getObject();
								iaeg.addAnnotatorId(adoc.getId());
							});	
						}
						
//						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), adoc.getId(), new ObjectId(currentUser.getId()))) {
//						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), ur.getKeys(), adoc.getId(), new ObjectId(currentUser.getId()))) {
						for (AnnotationEditGroup xaeg : annotationEditGroupRepository.findByDatasetIdAndOnClassAndAnnotatorIdAndUserId(dataset.getId(), adoc.getOnClass(), adoc.getId(), new ObjectId(currentUser.getId()))) {
							if (aegIds.contains(xaeg.getId())) {
								continue;
							}
							
							if (xaeg.getTag() == null || !ur.getTags().contains(xaeg.getTag())) {
								AnnotationEditGroupContainer xaegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, xaeg);
								xaegc.update(iaegr -> {
									AnnotationEditGroup iaeg = iaegr.getObject();
									iaeg.removeAnnotatorId(adoc.getId());
								});
								
								if (xaegc.getObject().getAnnotatorId() == null) {
									annotationEditGroupRepository.delete(xaegc.getObject());
								}
							}
						}	
					}					
					
				
				}
				
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			
			synchronized (saveSyncString()) {
				clearExecution();
					
				annotatorRepository.delete(object);

				if (object.getAsProperty() != null) { // legacy
					if (annotatorRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(object.getDatasetUuid(), object.getOnProperty().toArray(new String[] {}), object.getAsProperty(), new ObjectId(currentUser.getId())).isEmpty()) {
						annotationEditRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(object.getDatasetUuid(), object.getOnProperty(), object.getAsProperty(), new ObjectId(currentUser.getId()));
						annotationEditGroupRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(object.getDatasetUuid(), object.getOnProperty().toArray(new String[] {}), object.getAsProperty(), new ObjectId(currentUser.getId()));
					}
					
				} else {
					for (AnnotationEditGroup aeg : annotationEditGroupRepository.findByAnnotatorId(object.getId())) {
						AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
						aegc.update(iaegc -> {
							AnnotationEditGroup iaeg = iaegc.getObject();
							iaeg.removeAnnotatorId(object.getId());
						});
						
						if (aegc.getObject().getAnnotatorId() == null) {
							annotationEditRepository.deleteByAnnotationEditGroupId(aeg.getId());
							annotationEditGroupRepository.delete(aegc.getObject());
						}
					}
					

//					for (AnnotatorTag tag : object.getTags()) {
//						if (annotatorRepository.findByDatasetIdAndOnPropertyAndTagsAndUserId(object.getDatasetId(), object.getOnProperty().toArray(new String[] {}), tag, new ObjectId(currentUser.getId())).isEmpty()) {
//							annotationEditRepository.deleteByDatasetUuidAndOnPropertyAndTagAndUserId(object.getDatasetUuid(), object.getOnProperty(), tag, new ObjectId(currentUser.getId()));
//							annotationEditGroupRepository.deleteByDatasetIdAndOnPropertyAndTagAndUserId(object.getDatasetId(), object.getOnProperty().toArray(new String[] {}), tag, new ObjectId(currentUser.getId()));
//						}
//					}
				}
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
		public AnnotatorDocumentResponse asResponse() {
	    	AnnotatorDocumentResponse response = new AnnotatorDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());
	    	response.setIdentifier(object.getIdentifier());
	    	if (object.getOnProperty() != null) {
	    		response.setOnProperty(PathElement.onPathElementListAsStringListInverse(object.getOnProperty(), vocc));
	    	} else if (object.getOnClass() != null) {
	    		response.setOnClass(object.getOnClass());
	    		response.setElement(mapper.indexStructure2IndexStructureResponse(object.getStructure().getElement()));
	    		response.setKeysMetadata(object.getStructure().getKeysMetadata());
	    	}
	    	response.setAsProperty(object.getAsProperty());
	    	response.setAnnotator(object.getAnnotator());
	    	response.setOrder(object.getOrder());
	    	response.setGroup(object.getGroup());
	    	response.setControl(object.getControl());
	    	
	    	if (currentUser != null) {
	    		response.setOwnedByUser(currentUser.getId().equals(object.getUserId().toString()));
	    	}
	    	
	    	List<DataServiceParameter> parameterDef = null;
	    	
	    	if (object.getAnnotatorId() != null) {
	    		response.setAnnotatorId(object.getAnnotatorId().toString());
	    		
	   			Optional<PrototypeDocument> pdoc  = prototypeRepository.findById(object.getAnnotatorId());
	   			if (pdoc.isPresent()) {
	   				response.setAnnotatorName(pdoc.get().getName());
	   				
	   				parameterDef = pdoc.get().getParameters();
	   			}
	    		
	    	} else {
	    		// TODO also for system annotators
	    	}
	    	
//	    	response.setThesaurus(object.getThesaurus());
	    	if (object.getThesaurusId() != null) {
	    		Optional<Dataset> tdoc  = datasetRepository.findById(object.getThesaurusId());
	    		if (tdoc.isPresent()) {
		    		response.setThesaurusId(object.getThesaurusId().toString());
		    		response.setThesaurusName(tdoc.get().getName());
	    		}
	    	}

	    	response.setParameters(DataServiceParameter.getShowedParameters(isCurrentUserOwner(), parameterDef, object.getParameters()));
	    	response.setPreprocess(object.getPreprocess());
	    	response.setVariant(object.getVariant());
	    	response.setDefaultTarget(vocc.arrayPrefixize(object.getDefaultTarget()));
	    	response.setBodyProperties(object.getBodyProperties());
	    	
	    	response.setTags(object.getTags());
	    	
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	response.copyStates(object, getDatasetTripleStoreVirtuosoConfiguration(), fileSystemConfiguration);
	    	
//	    	AnnotationEditGroupResponse aegr = new AnnotationEditGroupResponse();
//	    	aegr.setId(aeg.getId().toString());
//	    	aegr.setUuid(aeg.getUuid());
//	    	aegr.setDatasetUuid(aeg.getDatasetUuid());
//	    	aegr.setAsProperty(aeg.getAsProperty());
//	    	aegr.setOnProperty(PathElement.onPathElementListAsStringListInverse(aeg.getOnProperty(), null));
//	    	
//	    	aegr.setTag(aeg.getTag());
//	    	
//	    	response.setEditGroup(aegr);
	    	
	        return response;
		}
		
		@Override
		public String getDescription() {
			return object.getAnnotator();
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
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByAnnotatorIdAndFileSystemConfigurationId(getObject().getId(), getContainerFileSystemConfiguration().getId(), type).orElse(null);
		}	
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}		
		
		@Override
		public PrepareState prepare() throws Exception {
			return prepareAnnotator(false);
		}
		
		@Override
		public PrepareState isPrepared() throws Exception {
			return prepareAnnotator(true);
		}
		
		private PrepareState prepareAnnotator(boolean checkOnly) throws Exception {

			AnnotatorDocument adoc = getObject();
			String id = adoc.getId().toString();
			
			Map<String, Object> params = new HashMap<>();

			for (DataServiceParameterValue dsp : adoc.getParameters()) {
				params.put(dsp.getName(), dsp.getValue());
			}

//			if (adoc.getThesaurus() != null) {
			if (adoc.getThesaurusId() != null) {
//				GraphLocation gl = idService.getGraph(adoc.getThesaurus());
				Optional<Dataset> adocOpt = datasetRepository.findById(adoc.getThesaurusId());
				if (adocOpt.isPresent()) {
					GraphLocation gl = idService.getGraph(adocOpt.get().getIdentifier());
					if (gl != null && gl.isPublik()) {
//						params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(adoc.getThesaurus()).toString());
						params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(adocOpt.get().getIdentifier()).toString());
					}
				}
			}
			
			System.out.println(params);
					
			PrepareState res = PrepareState.UNKNOWN;
			
			try (RDFOutputHandler outhandler = new JenaStringRDFOutputHandler()) {
				
				ExecutionOptions eo = buildExecutionParameters();
				
				String str = applyPreprocessToMappingDocument(eo);

				D2RMLModel rmlMapping = D2RMLModel.readFromString(str, resourceVocabulary.getMappingAsResource(id).toString(), 
						                                               ResourceFactory.createResource(resourceVocabulary.getMappingAsResource(id).toString() + "#" + (checkOnly ? "IsPreparedSpecification" : "PrepareSpecification") ));
				
//				System.out.println(">>> " + ResourceFactory.createResource(resourceVocabulary.getMappingAsResource(id).toString() + "#" + (checkOnly ? "IsPreparedSpecification" : "PrepareSpecification") ));
//				System.out.println(">>> " + rmlMapping.getD2RMLSpecification());
				
				if (rmlMapping.getD2RMLSpecification() == null) {
					res = PrepareState.PREPARED; 
				
				} else {
					Executor exec = new Executor(outhandler, safeExecute);
					
					exec.setMonitor(new SimpleMonitor());
					exec.execute(rmlMapping, params);
					
					Model model = ModelFactory.createDefaultModel();
					
//					System.out.println(((StringOutputHandler)outhandler).getResult());
					
					try (StringReader sr = new StringReader(((StringOutputHandler)outhandler).getResult())) {
						RDFDataMgr.read(model, sr, null, Lang.TURTLE);
						
						StmtIterator stmtIter = null;
						
						stmtIter = model.listStatements(null, SEMAVocabulary.annotatorPrepared, ResourceFactory.createTypedLiteral(true));
						if (stmtIter.hasNext()) {
							res = PrepareState.PREPARED;
						} else {
							stmtIter = model.listStatements(null, SEMAVocabulary.annotatorPreparing, ResourceFactory.createTypedLiteral(true));
							if (stmtIter.hasNext()) {
								res = PrepareState.PREPARING;
							} else {
								res = PrepareState.NOT_PREPARED;
							}
						}
		
					}
				}
			}
			
			return res;
		}

		@Override
		public ExecutionOptions buildExecutionParameters() {
			
			Map<String, Object> params = new HashMap<>();
			Map<String, IndexKeyMetadata> targets = null;
			
	    	DatasetCatalog dcg = schemaService.asCatalog(getEnclosingObject());
	    	String fromClause = schemaService.buildFromClause(dcg);

	    	TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			params.put("iigraph", fromClause);
			params.put("iirdfsource", vc.getSparqlEndpoint());
			params.put("iiannotator", object.asResource(resourceVocabulary));

			if (object.getOnProperty() != null) {
				String prop = PathElement.onPathStringListAsSPARQLString(object.getOnProperty());
//				System.out.println(prop);
				params.put("iiproperty", prop);
				
				targets = new HashMap<>();
				IndexKeyMetadata entry = new IndexKeyMetadata(0,"");
				targets.put("r" + entry.getIndex(), entry);
				
			} else if (object.getOnClass() != null) {
				SPARQLStructure ss = sparqlService.toSPARQL(object.getStructure().getElement(), AnnotatorDocument.getKeyMetadataMap(object.getStructure().getKeysMetadata()), false);
				
				params.put("iivariables", ss.returnVariables(true, true));
				params.put("iifilters", ss.filterClauses(true, true, true));
				params.put("iiwhere", ss.whereClause());
				params.put("iinosourcewhere", ss.treeWhereClause());
				params.put("iivalues", ss.valueClauses());  // value binding pattern
				params.put("iifixedvalues", ss.predefinedValueClauses()); // fixed values given by the annotator should go at end of query after bind

//				targets = ss.getTargets();

				targets = new HashMap<>();
				for (IndexKeyMetadata entry : object.getStructure().getKeysMetadata()) {
					targets.put("r" + entry.getIndex(), entry);
				}
			}

			// for compatibility: add empty values to later added parameters
			applyParameters(params);

//			if (object.getThesaurus() != null) {
			if (object.getThesaurusId() != null) {			
//				GraphLocation gl = idService.getGraph(object.getThesaurus());
				Optional<Dataset> adocOpt = datasetRepository.findById(object.getThesaurusId());
				if (adocOpt.isPresent()) {
					GraphLocation gl = idService.getGraph(adocOpt.get().getIdentifier());
					if (gl != null && gl.isPublik()) {
//						params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(object.getThesaurus()).toString());
						params.put("iithesaurus_endpoint", resourceVocabulary.getContentSparqlEnpoint(adocOpt.get().getIdentifier()).toString());
					}
				}
			}
			
//			System.out.println("C");
//			for (Map.Entry<String, Object> entry : params.entrySet()) {
//				System.out.println(entry.getKey() + " > " + entry.getValue());
//			}
//			System.out.println("TARGETS " + targets);
//			System.out.println("PARAMS " + params);
			return new ExecutionOptions(params, targets);
		}
		
		@Override
		public String applyPreprocessToMappingDocument(ExecutionOptions eo) throws Exception { // targets e.g. r0 -> title
			Map<String, Object> params = eo.getParams();
			Map<String, IndexKeyMetadata> targets = eo.getTargets();
			
			DataServiceRank rank = eo.getRank();// == null ? DataServiceRank.SINGLE : DataServiceRank.MULTIPLE;
			
//			for (Map.Entry<String, Object> entry : params.entrySet()) {
//				System.out.println(entry.getKey() + " > " + entry.getValue());
//			}
//			System.out.println("T1 " + targets);
//			System.out.println("T1 " + object.getAnnotator() + " / " + params + " / " +rank);
			
			String str = null;
			if (object.getAnnotator() != null) {
				str = dataServicesService.readMappingDocument(object, params, rank) ;
			} else {
				str = prototypeRepository.findById(object.getAnnotatorId()).get().getContent() ;
			}
			
			str = str.replace("{##ppRESOURCE_PREFIX##}", resourceVocabulary.getAnnotationAsResource("").toString());
			
			Map<String, String> itargets = new HashMap<>(); //  eg. title -> r0
			Map<String, Boolean> optionalTargets = new HashMap<>();
//			if (rank == DataServiceRank.MULTIPLE) {
				for (Map.Entry<String, IndexKeyMetadata> entry : targets.entrySet()) {
					if (entry.getValue().getName() != null) {
						itargets.put(entry.getValue().getName(), entry.getKey());
						optionalTargets.put(entry.getValue().getName(), entry.getValue().getOptional() != null ? entry.getValue().getOptional() : false);
					}
				}
//			}
			
			StringBuffer definedColumns = new StringBuffer();
			StringBuffer subjectCondition = new StringBuffer();
			StringBuffer preprocess = new StringBuffer();
			StringBuffer bindings = new StringBuffer();
			StringBuffer expandBindings = new StringBuffer();
			
			if (object.getDefinedColumns() != null) {
				for (PreprocessInstruction pi : object.getDefinedColumns()) {
					definedColumns.append("   dr:definedColumn [ \n");
					definedColumns.append("      dr:name \"" + pi.getName() + "\" ; \n");
					definedColumns.append("      dr:function <" + pi.getFunction() + "> ; \n");
					if (pi.getParameters() != null) {
						for (DataServiceParameterValue pv : pi.getParameters()) {
							definedColumns.append("      dr:parameterBinding [ \n");
							definedColumns.append("         dr:parameter \"" + pv.getName() + "\" ; \n");
							if (pv.getType() == TermMapType.CONSTANT) {
								definedColumns.append("         rr:constant \"" + pv.getValue() + "\" ; \n");
							} else if (pv.getType() == TermMapType.COLUMN) {
								definedColumns.append("         rr:column \"" + pv.getValue() + "\" ; \n");
							} else if (pv.getType() == TermMapType.TEMPLATE) {
								definedColumns.append("         rr:template \"" + pv.getValue() + "\" ; \n");
							}
							definedColumns.append("      ] ; \n");
						}
					}
					definedColumns.append("   ] ;\n");
				}
			}
			
			if (object.getMapConditions() != null) {
				for (ConditionInstruction ci : object.getMapConditions()) {
					if (ci.getScope().equals("subject")) {
						
						List<PreprocessInstruction> pis = ci.getConditions();
						if (pis.size() == 1) {
							PreprocessInstruction pi = pis.get(0);
							
							subjectCondition.append("      dr:condition  [ \n");
							subjectCondition.append("         dr:function <" + pi.getFunction() + "> ; \n");
							if (pi.getParameters() != null) {
								for (DataServiceParameterValue pv : pi.getParameters()) {
									subjectCondition.append("         dr:parameterBinding [ \n");
									subjectCondition.append("            dr:parameter \"" + pv.getName() + "\" ; \n");
									if (pv.getType() == TermMapType.CONSTANT) {
										subjectCondition.append("            rr:constant \"" + pv.getValue() + "\" ; \n");
									} else if (pv.getType() == TermMapType.COLUMN) {
										subjectCondition.append("            rr:column \"" + pv.getValue() + "\" ; \n");
									} else if (pv.getType() == TermMapType.TEMPLATE) {
										subjectCondition.append("            rr:template \"" + pv.getValue() + "\" ; \n");
									}
									subjectCondition.append("         ] ; \n");
								}
							}
							subjectCondition.append("      ] ;\n");
						} else {
							// TODO COMPLEX CONDITIONS
						}
					}
				}
			}
			
			str = str.replace("{##ppPREPROCESS_SUBJECT_CONDITION##}", subjectCondition.toString());
			
			List<PreprocessInstruction> allpis = object.getPreprocess();
			if (allpis != null && allpis.size() > 0) {

				Model model = ModelFactory.createDefaultModel();
				
				Map<String, List<PreprocessInstruction>> piPerTarget = new HashMap<>();
				for (PreprocessInstruction pi : allpis) {
					
					String target = pi.getTarget();
					if (target == null) {
						target = "";
					}
					
					List<PreprocessInstruction> list = piPerTarget.get(target);
					if (list == null) {
						list = new ArrayList<>();
						piPerTarget.put(target, list);
					}
					
					list.add(pi);
				}

//				System.out.println("T2 " + itargets);   // title -> r0       '' -> r0
//				System.out.println("A " + targets );    // r0 -> title       r0 -> ''
//				System.out.println("B " + piPerTarget); // title -> [ ... ]  '' -> [ ... ]
//				System.out.println("R " + rank);
//				
				for (Map.Entry<String, List<PreprocessInstruction>> entry : piPerTarget.entrySet()) {
					
					String nameTarget = entry.getKey();
					List<PreprocessInstruction> pis = entry.getValue();

//					String _nameTarget = "_" + nameTarget;
					String effectiveNameTarget = nameTarget.length() == 0 ? "" : "_" + nameTarget;
					
//					String target = itargets.get(nameTarget);
					String _target = "_" + itargets.get(nameTarget);
					
					for (int i = 0; i < pis.size(); i++) {
						PreprocessInstruction pi = pis.get(i);
						
						preprocess.append("   dr:definedColumn [ \n");
						if (functions.keySet().contains(model.createResource(pi.getFunction()))) {
							preprocess.append("      dr:name \"PREPROCESS-LEXICALVALUE-" + (i + 1) + _target + "\" ; \n");
							preprocess.append("      dr:function <" + pi.getFunction() + "> ; \n");
							preprocess.append("      dr:parameterBinding [ \n");
							preprocess.append("         dr:parameter \"input\" ; \n");
							if (i == 0) {
								preprocess.append("         rr:column \"lexicalValue" + _target + "\" ; \n");
							} else {
								preprocess.append("         rr:column \"PREPROCESS-LEXICALVALUE-" + i + _target + "\" ; \n");
							}
							preprocess.append("      ] ; \n");
							if (pi.getParameters() != null) {
								for (DataServiceParameterValue pv : pi.getParameters()) {
									preprocess.append("      dr:parameterBinding [ \n");
									preprocess.append("         dr:parameter \"" + pv.getName() + "\" ; \n");
									preprocess.append("         rr:constant \"" + pv.getValue() + "\" ; \n");
									preprocess.append("      ] ; \n");
								}
							}
						} else {
							preprocess.append("      dr:name \"PREPROCESS-LEXICALVALUE-" + (i + 1) + _target + "\" ; \n");
							preprocess.append("      dr:function <" + D2RMLOPVocabulary.f_identity + "> ; \n");
							preprocess.append("      dr:parameterBinding [ \n");
							preprocess.append("         dr:parameter \"input\" ; \n");
							if (i == 0) {
								preprocess.append("         rr:column \"lexicalValue" + _target + "\" ; \n");
							} else {
								preprocess.append("         rr:column \"PREPROCESS-LEXICALVALUE-" + i + _target + "\" ; \n");
							}
							
							preprocess.append("         dr:condition [ \n");
	
							if (pi.getModifier() == null) {
								preprocess.append("            dr:function <" + pi.getFunction() + "> ; \n");
								preprocess.append("            dr:parameterBinding [ \n");
								preprocess.append("               dr:parameter \"input\" ; \n");
								if (i == 0) {
									preprocess.append("               rr:column \"lexicalValue" + _target + "\" ; \n");
								} else {
									preprocess.append("               rr:column \"PREPROCESS-LEXICALVALUE-" + i + _target + "\" ; \n");
								}
								preprocess.append("            ] ; \n");
								if (pi.getParameters() != null) {
									for (DataServiceParameterValue pv : pi.getParameters()) {
										preprocess.append("            dr:parameterBinding [ \n");
										preprocess.append("               dr:parameter \"" + pv.getName() + "\" ; \n");
										preprocess.append("               rr:constant \"" + pv.getValue() + "\" ; \n");
										preprocess.append("            ] ; \n");
									}
								}
								
							} else if (pi.getModifier().equals(D2RMLOPVocabulary.logicalNot.toString())){
								preprocess.append("            dr:booleanOperator <" + D2RMLOPVocabulary.logicalNot + "> ; \n");
								preprocess.append("            dr:condition [ \n");
								preprocess.append("               dr:function <" + pi.getFunction() + "> ; \n");
								preprocess.append("               dr:parameterBinding [ \n");
								preprocess.append("                  dr:parameter \"input\" ; \n");
								if (i == 0) {
									preprocess.append("                  rr:column \"lexicalValue" + _target + "\" ; \n");
								} else {
									preprocess.append("                  rr:column \"PREPROCESS-LEXICALVALUE-" + i + _target + "\" ; \n");
								}
								preprocess.append("               ] ; \n");
								if (pi.getParameters() != null) {
									for (DataServiceParameterValue pv : pi.getParameters()) {
										preprocess.append("            dr:parameterBinding [ \n");
										preprocess.append("               dr:parameter \"" + pv.getName() + "\" ; \n");
										preprocess.append("               rr:constant \"" + pv.getValue() + "\" ; \n");
										preprocess.append("            ] ; \n");
									}
									preprocess.append("         ] ; \n");
								}
							}
						
							preprocess.append("         ] ; \n");
							
							preprocess.append("      ] ; \n");
						}
						preprocess.append("   ] ; \n");
					}
						
					preprocess.append("   dr:definedColumn [ \n");
					preprocess.append("     dr:name \"PREPROCESS-LITERAL" + _target + "\" ; \n");
					preprocess.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#strlang> ; \n");
					preprocess.append("     dr:parameterBinding [ \n");
					preprocess.append("        dr:parameter \"lexicalValue\" ; \n");
					preprocess.append("        rr:column \"PREPROCESS-LEXICALVALUE-" + pis.size() + _target + "\" ; \n");
					preprocess.append("     ] ; \n");
					preprocess.append("     dr:parameterBinding [ \n");
					preprocess.append("        dr:parameter \"language\" ; \n");
					preprocess.append("        rr:column \"language" + _target + "\" ; \n");
					preprocess.append("     ] ; \n");
					preprocess.append("   ] ; \n");
					
					bindings.append("      dr:parameterBinding [ \n"); 
					bindings.append("         dr:parameter \"literal" + effectiveNameTarget + "\" ; \n");
					bindings.append("         rr:column \"literal" + effectiveNameTarget + "\" ; \n");
					bindings.append("         dr:valueType rr:Literal ; \n");
					bindings.append("      ] ; \n");
					bindings.append("      dr:parameterBinding [ \n"); 
					bindings.append("         dr:parameter \"lexicalValue" + effectiveNameTarget + "\" ; \n");
					bindings.append("         rr:column \"lexicalValue" + effectiveNameTarget + "\" ; \n");
					bindings.append("         dr:escapeType dr:EscapeSpecial ; \n");
					bindings.append("      ] ; \n");
					bindings.append("      dr:parameterBinding [ \n"); 
					bindings.append("         dr:parameter \"language" + effectiveNameTarget + "\" ; \n");
					bindings.append("         rr:column \"language" + effectiveNameTarget + "\" ; \n");
					bindings.append("      ] ; \n");
					
					definedColumns.append("   dr:definedColumn [ \n");
					definedColumns.append("     dr:name \"literal" + effectiveNameTarget + "\" ; \n");
					definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
					definedColumns.append("     dr:parameterBinding [ \n");
					definedColumns.append("        dr:parameter \"input\" ; \n");
					definedColumns.append("        rr:column \"PREPROCESS-LITERAL" + _target + "\" ; \n");
					definedColumns.append("     ] ; \n");
					definedColumns.append("   ] ; \n");
					definedColumns.append("   dr:definedColumn [ \n");
					definedColumns.append("     dr:name \"lexicalValue" + effectiveNameTarget + "\" ; \n");
					definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
					definedColumns.append("     dr:parameterBinding [ \n");
					definedColumns.append("        dr:parameter \"input\" ; \n");
					definedColumns.append("        rr:column \"PREPROCESS-LEXICALVALUE-" + pis.size() + _target + "\" ; \n");
					definedColumns.append("     ] ; \n");
					definedColumns.append("   ] ; \n");
					definedColumns.append("   dr:definedColumn [ \n");
					definedColumns.append("     dr:name \"language" + effectiveNameTarget + "\" ; \n");
					definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
					definedColumns.append("     dr:parameterBinding [ \n");
					definedColumns.append("        dr:parameter \"input\" ; \n");
					definedColumns.append("        rr:column \"language" + _target + "\" ; \n");
					definedColumns.append("     ] ; \n");
					definedColumns.append("   ] ; \n");
				}
				
				for (Map.Entry<String, String> entry : itargets.entrySet()) {
					if (!piPerTarget.keySet().contains(entry.getKey())) {
						
						String target = entry.getKey();
						if (target.length() != 0) {
							target = "_" + target;
						}
						
						bindings.append("      dr:parameterBinding [ \n"); 
						bindings.append("         dr:parameter \"literal" + target + "\" ; \n");
						bindings.append("         rr:column \"literal" + target + "\" ; \n");
						bindings.append("         dr:valueType rr:Literal ; \n");
						if (optionalTargets.get(entry.getKey())) {
							bindings.append("         dr:optional true ; \n");
						}
						bindings.append("      ] ; \n");
						bindings.append("      dr:parameterBinding [ \n"); 
						bindings.append("         dr:parameter \"lexicalValue" + target + "\" ; \n");
						bindings.append("         rr:column \"lexicalValue" + target + "\" ; \n");
						bindings.append("         dr:escapeType dr:EscapeSpecial ; \n");
						if (optionalTargets.get(entry.getKey())) {
							bindings.append("         dr:optional true ; \n");
						}
						bindings.append("      ] ; \n");
						bindings.append("      dr:parameterBinding [ \n"); 
						bindings.append("         dr:parameter \"language" + target + "\" ; \n");
						bindings.append("         rr:column \"language" + target + "\" ; \n");
						if (optionalTargets.get(entry.getKey())) {
							bindings.append("         dr:optional true ; \n");
						}
						bindings.append("      ] ; \n");
						
						definedColumns.append("   dr:definedColumn [ \n");
						definedColumns.append("     dr:name \"literal" + target + "\" ; \n");
						definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
						definedColumns.append("     dr:parameterBinding [ \n");
						definedColumns.append("        dr:parameter \"input\" ; \n");
						definedColumns.append("        rr:column \"" + entry.getValue() + "\" ; \n");
						definedColumns.append("     ] ; \n");
						definedColumns.append("   ] ; \n");						
						definedColumns.append("   dr:definedColumn [ \n");
						definedColumns.append("     dr:name \"lexicalValue" + target + "\" ; \n");
						definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
						definedColumns.append("     dr:parameterBinding [ \n");
						definedColumns.append("        dr:parameter \"input\" ; \n");
						definedColumns.append("        rr:column \"lexicalValue_" + entry.getValue() + "\" ; \n");
						definedColumns.append("     ] ; \n");
						definedColumns.append("   ] ; \n");
						definedColumns.append("   dr:definedColumn [ \n");
						definedColumns.append("     dr:name \"language" + target + "\" ; \n");
						definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
						definedColumns.append("     dr:parameterBinding [ \n");
						definedColumns.append("        dr:parameter \"input\" ; \n");
						definedColumns.append("        rr:column \"language_" + entry.getValue() + "\" ; \n");
						definedColumns.append("     ] ; \n");
						definedColumns.append("   ] ; \n");
					}
				}
				
			} else { // no preprocessing
				for (Map.Entry<String, String> entry : itargets.entrySet()) {
					
					String target = entry.getKey();
					if (target.length() != 0) {
						target = "_" + target;
					}
					
					bindings.append("      dr:parameterBinding [ \n"); 
					bindings.append("         dr:parameter \"literal" + target + "\" ; \n");
					bindings.append("         rr:column \"literal_" + entry.getKey() + "\" ; \n");
					bindings.append("         dr:valueType rr:Literal ; \n");
					if (optionalTargets.get(entry.getKey())) {
						bindings.append("         dr:optional true ; \n");
					}
					bindings.append("      ] ; \n");
					bindings.append("      dr:parameterBinding [ \n"); 
					bindings.append("         dr:parameter \"lexicalValue" + target + "\" ; \n");
					bindings.append("         rr:column \"lexicalValue_" + entry.getKey() + "\" ; \n");
					bindings.append("         dr:escapeType dr:EscapeSpecial ; \n");
					if (optionalTargets.get(entry.getKey())) {
						bindings.append("         dr:optional true ; \n");
					}
					bindings.append("      ] ; \n");
					bindings.append("      dr:parameterBinding [ \n"); 
					bindings.append("         dr:parameter \"language" + target + "\" ; \n");
					bindings.append("         rr:column \"language_" + entry.getKey() + "\" ; \n");
					if (optionalTargets.get(entry.getKey())) {
						bindings.append("         dr:optional true ; \n");
					}
					bindings.append("      ] ; \n");
				
					definedColumns.append("   dr:definedColumn [ \n");
					definedColumns.append("     dr:name \"literal" + target + "\" ; \n");
					definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
					definedColumns.append("     dr:parameterBinding [ \n");
					definedColumns.append("        dr:parameter \"input\" ; \n");
					definedColumns.append("        rr:column \"" + entry.getValue() + "\" ; \n");
					definedColumns.append("     ] ; \n");
					definedColumns.append("   ] ; \n");						
					definedColumns.append("   dr:definedColumn [ \n");
					definedColumns.append("     dr:name \"lexicalValue" + target + "\" ; \n");
					definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
					definedColumns.append("     dr:parameterBinding [ \n");
					definedColumns.append("        dr:parameter \"input\" ; \n");
					definedColumns.append("        rr:column \"lexicalValue_" + entry.getValue() + "\" ; \n");
					definedColumns.append("     ] ; \n");
					definedColumns.append("   ] ; \n");
					definedColumns.append("   dr:definedColumn [ \n");
					definedColumns.append("     dr:name \"language" + target + "\" ; \n");
					definedColumns.append("     dr:function <http://islab.ntua.gr/ns/d2rml-op#identity> ; \n");
					definedColumns.append("     dr:parameterBinding [ \n");
					definedColumns.append("        dr:parameter \"input\" ; \n");
					definedColumns.append("        rr:column \"language_" + entry.getValue() + "\" ; \n");
					definedColumns.append("     ] ; \n");
					definedColumns.append("   ] ; \n");
				}
			}
				
			str = str.replace("{##ppPREPROCESS_QUERY_BINDINGS##}", bindings.toString());
			str = str.replace("{##ppPREPROCESS##}", preprocess.toString() + definedColumns.toString());
			
			if (rank == DataServiceRank.MULTIPLE) {
				for (Map.Entry<String, IndexKeyMetadata> entry : targets.entrySet()) {
					expandBindings.append("      dr:parameterBinding [ \n"); 
					expandBindings.append("         dr:parameter \"" + entry.getKey() + "\" ; \n");
					expandBindings.append("         rr:column \"" + entry.getKey() + "\" ; \n");
					expandBindings.append("         dr:valueType rr:Literal ; \n");
					if (entry.getValue().getOptional() != null && entry.getValue().getOptional()) {
						expandBindings.append("         dr:optional true ; \n");
					}					
					expandBindings.append("      ] ; \n");
				}
				
				str = str.replace("{##ppPREPROCESS_UNGROUP_BINDINGS##}", expandBindings.toString());
				
				StringBuffer valueBindings = new StringBuffer();
				for (Map.Entry<String, IndexKeyMetadata> entry : targets.entrySet()) {
				   
				   valueBindings.append("            rr:predicateObjectMap [ \n");
				   valueBindings.append("               rr:predicateMap [\n");
				   valueBindings.append("                  rr:constant <http://sw.islab.ntua.gr/annotation/onBinding> ; \n");
				   valueBindings.append("                  dr:condition [ \n");
//				   valueBindings.append("                     dr:function drop:matches ; \n");
				   valueBindings.append("                     dr:function xpathfn:matches ; \n");
				   valueBindings.append("                     dr:parameterBinding [ \n");
				   valueBindings.append("                        dr:parameter \"input\" ; \n");
				   valueBindings.append("                        rr:column \"" + entry.getKey() + "\" ; \n");
				   valueBindings.append("                     ] ;\n");
				   valueBindings.append("                     dr:parameterBinding [ \n");
				   valueBindings.append("                        dr:parameter \"pattern\" ; \n");
				   valueBindings.append("                        rr:constant \".+\" ; \n");
				   valueBindings.append("                     ] ;\n");
//				   valueBindings.append("                     dr:function drop:isNotEmpty ; \n");
//				   valueBindings.append("                     dr:parameterBinding [ \n");
//				   valueBindings.append("                        dr:parameter \"input\" ; \n");
//				   valueBindings.append("                        rr:column \"" + entry.getKey() + "\" ; \n");
//				   valueBindings.append("                     ] ;");
				   valueBindings.append("                  ] ;\n");
				   valueBindings.append("               ] ;\n");				   
				   valueBindings.append("               rr:objectMap [ \n");
				   valueBindings.append("                  rr:parentTriplesMap [ \n");
				   valueBindings.append("                     rr:subjectMap [ \n");
				   valueBindings.append("                        rr:termType rr:BlankNode; \n");
				   valueBindings.append("                     ] ; \n");	
				   valueBindings.append("                     rr:predicateObjectMap [ \n");
				   valueBindings.append("                        rr:predicate <http://sw.islab.ntua.gr/annotation/variable> ; \n");				   
				   valueBindings.append("                        rr:objectMap [ \n");
				   valueBindings.append("                           rr:constant \"" + entry.getKey() + "\" ; \n");
				   valueBindings.append("                           rr:termType rr:Literal; \n");
				   valueBindings.append("                        ] ; \n");
				   valueBindings.append("                     ] ; \n");	
				   valueBindings.append("                     rr:predicateObjectMap [ \n");
				   valueBindings.append("                        rr:predicate <http://sw.islab.ntua.gr/annotation/value> ; \n");
				   valueBindings.append("                        rr:objectMap [ \n");
				   valueBindings.append("                           rr:column \"" + entry.getKey() + "\" ; \n");
				   valueBindings.append("                           rr:termType rr:Literal; \n");
				   valueBindings.append("                        ] ; \n");
				   valueBindings.append("                     ] ; \n");		
				   valueBindings.append("                     rr:predicateObjectMap [ \n");
				   valueBindings.append("                        rr:predicate <http://sw.islab.ntua.gr/annotation/name> ; \n");
				   valueBindings.append("                        rr:objectMap [ \n");
				   valueBindings.append("                           rr:constant \"" + entry.getValue().getName() + "\" ; \n");
				   valueBindings.append("                           rr:termType rr:Literal; \n");
				   valueBindings.append("                        ] ; \n");
				   valueBindings.append("                     ] ; \n");					   
				   valueBindings.append("                  ] ; \n");
				   valueBindings.append("               ] ; \n");                    
				   valueBindings.append("            ] ; \n");
				}
				
				str = str.replace("{##ppPREPROCESS_VALUE_BINDINGS##}", valueBindings.toString());
			}
					
//			System.out.println(str);
			return str;

		}

		@Override
		public List<AnnotatorDocument> getAnnotators() {
			List<AnnotatorDocument> res = new ArrayList<>();
			res.add(object);
			
			return res;
		}

		@Override
		public List<String> getOnProperty() {
			return object.getOnProperty();
		}

	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	private void updateRequiredParameters(List<DataServiceParameter> params, AnnotatorDocument adoc) {
		if (params == null || adoc.getStructure() == null || adoc.getStructure().getKeysMetadata() == null) {
			return;
		}
		
		Map<String, DataServiceParameter> nameMap = new HashMap<>();
		Map<Integer, DataServiceParameter> indexMap = new HashMap<>();
		for (DataServiceParameter dsp : params) {
			nameMap.put(dsp.getName(), dsp);
		}
		
		for (IndexKeyMetadata ikm : adoc.getStructure().getKeysMetadata()) {
			DataServiceParameter dsp = nameMap.get(ikm.getName());
			ikm.setOptional(!dsp.isRequired());

			indexMap.put(ikm.getIndex(), dsp);
			
		}
		
		adoc.getStructure().getElement().updateRequiredParameters(indexMap);
	}
	
	
	@Override
	public AnnotatorDocument create(UserPrincipal currentUser, Dataset dataset, AnnotatorUpdateRequest ur) throws Exception {
		ur.normalize();
		
		AnnotatorDocument adoc = new AnnotatorDocument(dataset);
		adoc.setUserId(new ObjectId(currentUser.getId()));
		adoc.setName(ur.getName());
		adoc.setIdentifier(ur.getIdentifier());

		List<String> flatOnPath = null;
		if (ur.getOnPath() != null) {
			flatOnPath = PathElement.onPathElementListAsStringList(ur.getOnPath());
		}

		if (ur.getOnPath() != null) {
			adoc.setOnProperty(flatOnPath);
//			adoc.setKeysMetadata(null);
//			adoc.setElement(null);
			adoc.setStructure(null);
			
			adoc.setGroup(String.join(",", flatOnPath).hashCode());
		} else {
			adoc.setOnClass(ur.getOnClass());
//			adoc.setKeysMetadata(ur.getKeysMetadata());
//			adoc.setElement(ur.getIndexStructure());
			adoc.setStructure(ur.getStructure());
			
			adoc.setGroup(ur.getOnClass().hashCode());
		}
		
		List<DataServiceParameter> prototypeParameters;
		
		if (ur.getAnnotatorId() != null) {
			adoc.setAnnotatorId(new ObjectId(ur.getAnnotatorId()));
			adoc.setAnnotator(null);
			
			PrototypeContainer pc = prototypeService.getContainer(currentUser, new SimpleObjectIdentifier(new ObjectId(ur.getAnnotatorId())));
			prototypeParameters = pc.getObject().getFields();
			
		} else {
			adoc.setAnnotatorId(null);
			adoc.setAnnotator(ur.getAnnotator());
			
			prototypeParameters = annotators.get(ur.getAnnotator()).getFields();
		}
		
		if (adoc.getOnClass() != null && prototypeParameters != null) {
			updateRequiredParameters(prototypeParameters, adoc);
		}
		
		adoc.setVariant(ur.getVariant());
		adoc.setAsProperty(ur.getAsProperty());
		adoc.setParameters(ur.getParameters());
//		adoc.setThesaurus(ur.getThesaurus());
		if (ur.getThesaurusId() != null) {
			adoc.setThesaurusId(new ObjectId(ur.getThesaurusId()));
		}
		adoc.setPreprocess(ur.getPreprocess());
		adoc.setDefaultTarget(ur.getDefaultTarget());
		adoc.setBodyProperties(ur.getBodyProperties());
		adoc.setControl(ur.getControl());
		
		adoc.setTags(ur.getTags());

		AnnotationEditGroupUpdateRequest aegr = new AnnotationEditGroupUpdateRequest();
		aegr.setAsProperty(ur.getAsProperty());
		aegr.setAutoexportable(false);
		if (ur.getOnPath() != null) {
			aegr.setOnProperty(flatOnPath);
		} else {
			aegr.setOnClass(ur.getOnClass());
			aegr.setKeys(ur.getStructure() != null ? ur.getStructure().getKeys() : null);
//			SPARQLStructure ss = sparqlService.toSPARQL(adoc.getElement(), false);
//			aegr.setSparqlClause(ss.getWhereClause());
		}

		AnnotationEditGroup aeg;
		Optional<AnnotationEditGroup> aegOpt;
		
		if (adoc.getAsProperty() != null) { // legacy
			if (ur.getOnPath() != null) {
				aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(dataset.getUuid(), flatOnPath, ur.getAsProperty(), new ObjectId(currentUser.getId()));
			} else {
				aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnClassAndAsPropertyAndUserId(dataset.getUuid(), ur.getOnClass(), ur.getAsProperty(), new ObjectId(currentUser.getId()));
			}
			
			if (!aegOpt.isPresent()) {
				aeg = aegService.create(currentUser, dataset, aegr);
			} else {
				aeg = aegOpt.get();
			}
			adoc.setAnnotatorEditGroupId(aeg.getId());
//			aeg.addAnnotatorId(adoc.getId());
		}
		
		create(adoc);
		
		if (adoc.getAsProperty() == null) { 
			if (ur.getTags() == null) {
				if (ur.getOnPath() != null) {
					aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagExistsAndUserId(dataset.getId(), flatOnPath, false, new ObjectId(currentUser.getId()));
				} else {
//					aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagExistsAndUserId(dataset.getId(), ur.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), false, new ObjectId(currentUser.getId()));
					aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagExistsAndUserId(dataset.getId(), ur.getOnClass(), ur.getStructure().getKeys(), false, new ObjectId(currentUser.getId()));
				}
				if (!aegOpt.isPresent()) {
					aeg = aegService.create(currentUser, dataset, aegr);
				} else {
					aeg = aegOpt.get();
				}
				
				AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
				aegc.update(iaegr -> {
					AnnotationEditGroup iaeg = iaegr.getObject();
					iaeg.addAnnotatorId(adoc.getId());
				});
			} else {
				for (String tag : ur.getTags()) {
					if (ur.getOnPath() != null) {
						aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagAndUserId(dataset.getId(), flatOnPath, tag, new ObjectId(currentUser.getId()));
					} else {
//						aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagAndUserId(dataset.getId(), ur.getOnClass(), ur.getKeys(), aegr.getSparqlClause(), tag, new ObjectId(currentUser.getId()));
						aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnClassAndKeysAndTagAndUserId(dataset.getId(), ur.getOnClass(), ur.getStructure().getKeys(), tag, new ObjectId(currentUser.getId()));
					}
					if (!aegOpt.isPresent()) {
						aegr.setTag(tag);
						aeg = aegService.create(currentUser, dataset, aegr);
					} else {
						aeg = aegOpt.get();
					}
					
					AnnotationEditGroupContainer aegc = (AnnotationEditGroupContainer)aegService.getContainer(currentUser, aeg);
					aegc.update(iaegr -> {
						AnnotationEditGroup iaeg = iaegr.getObject();
						iaeg.addAnnotatorId(adoc.getId());
					});					
				}
			}
		} 

		return adoc;
	}
	
	@Override
	public AnnotatorContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		AnnotatorContainer ac = new AnnotatorContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}
	}
	
	@Override
	public AnnotatorContainer getContainer(UserPrincipal currentUser, AnnotatorDocument adoc) {
		AnnotatorContainer ac = new AnnotatorContainer(currentUser, adoc);

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}	}

	
	@Override
	public AnnotatorContainer getContainer(UserPrincipal currentUser, AnnotatorDocument adoc, Dataset dataset) {
		AnnotatorContainer ac = new AnnotatorContainer(currentUser, adoc, dataset);

		if (ac.getObject() == null || ac.getEnclosingObject() == null) {
			return null;
		} else {
			return ac;
		}
	}
	
//	public boolean clearExecution(AnnotatorContainer ac) {
//		MappingExecuteState es = ac.getObject().checkExecuteState(fileSystemConfiguration.getId());
//		
//		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
//			return false;
//		}
//		
//		return clearExecution(ac.getCurrentUser(), ac.getEnclosingObject(), ac.getObject(), es);
//	}
//	
//	private boolean clearExecution(AnnotatorContainer ac, MappingExecuteState es) {
//		return clearExecution(ac.getCurrentUser(), ac.getEnclosingObject(), ac.getObject(), es);
//	}
//	
//	private boolean clearExecution(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, MappingExecuteState es) {
//	
////		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
////			return false;
////		}
//		
//		ProcessStateContainer psv = doc.getCurrentPublishState(virtuosoConfigurations.values());
//		
//		if (psv != null) {
//			MappingPublishState ps = (MappingPublishState)psv.getProcessState();
//			MappingExecuteState pes = ps.getExecute();
//	
//			// do not clear published execution
//			if (pes != null && pes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) == 0 && pes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {	
//				return false;
//			} 
//		}
//		
//		// trig files
//		if (es.getExecuteShards() != null) {
//			for (int i = 0; i < es.getExecuteShards(); i++) {
//				try {
//					File f = folderService.getAnnotatorExecutionTrigFile(currentUser, dataset, doc, es, i);
//					boolean ok = false;
//					if (f != null) {
//						ok = f.delete();
//						if (ok) {
//							logger.info("Deleted file " + f.getAbsolutePath());
//						}
//					}
//					if (!ok) {
//						logger.warn("Failed to delete trig execution " + i + " for annotator " + doc.getUuid());
//					}
//					
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		// catalog file
//		try {
//			File f = folderService.getAnnotatorExecutionCatalogTrigFile(currentUser, dataset, doc, es);
//			boolean ok = false;
//			if (f != null) {
//				ok = f.delete();
//				if (ok) {
//					logger.info("Deleted file " + f.getAbsolutePath());
//				}
//			}
//			if (!ok) {
//				logger.warn("Failed to delete catalog for annotator " + doc.getUuid());
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		try {
//			File f = folderService.getAnnotatorExecutionZipFile(currentUser, dataset, doc, es);
//			boolean ok = false;
//			if (f != null) {
//				ok = f.delete();
//				if (ok) {
//					logger.info("Deleted file " + f.getAbsolutePath());
//				}
//			}
//			if (!ok) {
//				logger.warn("Failed to delete zipped execution for annotator " + doc.getUuid());
//			}
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		folderService.deleteAnnotationsExecutionDatasetFolderIfEmpty(currentUser, dataset);
//		
//		if (doc.checkExecuteState(fileSystemConfiguration.getId()) == es) {
//			doc.deleteExecuteState(fileSystemConfiguration.getId());
//		
//			if (psv != null) {
//				MappingPublishState ps = (MappingPublishState)psv.getProcessState();
//				MappingExecuteState pes = ps.getExecute();
//		
//				// do not clear published execution
//				if (pes != null && pes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) != 0 && pes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {
//					MappingExecuteState nes = doc.getExecuteState(fileSystemConfiguration.getId());
//					nes.setCount(pes.getCount());
//					nes.setExecuteCompletedAt(pes.getExecuteCompletedAt());
//					nes.setExecuteStartedAt(pes.getExecuteStartedAt());
//					nes.setExecuteShards(pes.getExecuteShards());
//					nes.setExecuteState(pes.getExecuteState());
//				}
//			}
//		} 
//		
//		annotatorRepository.save(doc);
//
//		return true;
//	}	
	

	
	@Override
	@Async("mappingExecutor")
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return serviceUtils.execute(tdescr, wsService);
	}
	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();

		AnnotatorContainer ac = (AnnotatorContainer)tdescr.getContainer();
		AnnotatorDocument adoc = ac.getObject();
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
				AnnotatorPublishState ips = (AnnotatorPublishState)((PublishableContainer)iac).getPublishState();
				ips.completeDo(pm);
				ips.setExecute(((ExecutableContainer<AnnotatorDocument,AnnotatorDocumentResponse,MappingExecuteState,Dataset>)iac).getExecuteState());
//				ips.setAsProperty(adoc.getTripleStoreGraph(resourceVocabulary));
			});

			// update annotation edit group change date
			
			if (adoc.getAsProperty() != null) {
				Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
				AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
				
				aeg.setLastPublicationStateChange(new Date());
	
				annotationEditGroupRepository.save(aeg);
			} else if (adoc.getTags() != null) {
//				for (AnnotatorTag tag : adoc.getTags()) {
//					Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagAndUserId(adoc.getDatasetId(), adoc.getOnProperty(), tag, new ObjectId(currentUser.getId()));
//					AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
//					
//					aeg.setLastPublicationStateChange(new Date());
//		
//					annotationEditGroupRepository.save(aeg);
//				}
			}

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
		
		AnnotatorContainer ac = (AnnotatorContainer)tdescr.getContainer();
		AnnotatorDocument adoc = ac.getObject();
		UserPrincipal currentUser = ac.getCurrentUser();

		TripleStoreConfiguration vc = ac.getDatasetTripleStoreVirtuosoConfiguration();

		try {
			ac.update(iac -> {
				MappingPublishState ips = ((PublishableContainer<AnnotatorDocument,AnnotatorDocumentResponse,MappingExecuteState, MappingPublishState,Dataset>)iac).getPublishState();
				ips.startUndo(pm);
			});
			
			pm.sendMessage(new PublishNotificationObject(ac));

			tripleStore.unpublish(vc, ac);

			pm.complete();
			
			ac.update(iac -> {
				PublishableContainer<AnnotatorDocument, AnnotatorDocumentResponse, MappingExecuteState, MappingPublishState,Dataset> ipc = (PublishableContainer)iac;
				ExecutableContainer<AnnotatorDocument, AnnotatorDocumentResponse, MappingExecuteState,Dataset> iec = (ExecutableContainer<AnnotatorDocument,AnnotatorDocumentResponse,MappingExecuteState,Dataset>)iac;
				
				MappingPublishState ips = ipc.getPublishState();
				
				ipc.removePublishState(ips);
				
				MappingExecuteState ies = (MappingExecuteState)iec.getExecuteState();
				MappingExecuteState ipes = ips.getExecute();
				if (ies != null && ipes != null && ies.getExecuteStartedAt().compareTo(ipes.getExecuteStartedAt()) != 0) {
					iec.clearExecution(ipes);
				}
			});
			
			// update annotation edit group change date
			if (adoc.getAsProperty() != null) {
				Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
				AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
				
				aeg.setLastPublicationStateChange(new Date());
				
				annotationEditGroupRepository.save(aeg);
			} else if (adoc.getTags() != null) {
//				for (AnnotatorTag tag : adoc.getTags()) {
//					Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetIdAndOnPropertyAndTagAndUserId(adoc.getDatasetId(), adoc.getOnProperty(), tag, new ObjectId(currentUser.getId()));
//					AnnotationEditGroup aeg = aegOpt.get(); // it should exist!
//					
//					aeg.setLastPublicationStateChange(new Date());
//					
//					annotationEditGroupRepository.save(aeg);	
//				}
			}
			
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
	
	public void unload(Dataset dataset) throws Exception {

		String id = dataset.getId().toString();
		
		try (RDFOutputHandler outhandler = new JenaStringRDFOutputHandler()) {

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

	public ThesaurusLoadState isLoaded(Dataset dataset) throws Exception {

		ThesaurusLoadState res = ThesaurusLoadState.UNKNOWN;
		
		String id = dataset.getId().toString();
		
		try (RDFOutputHandler outhandler = new JenaStringRDFOutputHandler()) {

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
					res = ThesaurusLoadState.LOADED;
				} else {
					stmtIter = model.listStatements(null, SEMAVocabulary.thesaurusLoading, ResourceFactory.createTypedLiteral(true));
					if (stmtIter.hasNext()) {
						res = ThesaurusLoadState.LOADING;
					} else {
						res = ThesaurusLoadState.NOT_LOADED;
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
	
//	private File zipExecution(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, ExecuteState es, int shards) throws IOException {
//		
//		File file = folderService.createAnnotationsExecutionZipFile(currentUser, dataset, doc, es);
//		
//		try (FileOutputStream fos = new FileOutputStream(file);
//				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
//			for (int i = 0; i < shards; i++) {
//	        	File fileToZip = folderService.getAnnotatorExecutionTrigFile(currentUser, dataset, doc, es, i);
//
//	        	try (FileInputStream fis = new FileInputStream(fileToZip)) {
//		            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
//		            zipOut.putNextEntry(zipEntry);
//		 
//		            byte[] bytes = new byte[1024];
//		            int length;
//		            while((length = fis.read(bytes)) >= 0) {
//		                zipOut.write(bytes, 0, length);
//		            }
//	            }
//	        }
//        }
//		
//		return file;
//	}
	
	
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

	
//	public List<AnnotatorDocumentResponse> getAnnotators(String datasetUri) {
//
//		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);
//
//		List<AnnotatorDocument> docs = annotatorRepository.findByDatasetUuid(datasetUuid);
//
//		Optional<Dataset> datasetOpt = datasetRepository.findByUuid(datasetUuid);
//
//		if (!datasetOpt.isPresent()) {
//			return new ArrayList<>();
//		}
//		
//		Dataset dataset = datasetOpt.get();
//		
//		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());
//		
//		final TripleStoreConfiguration vc = psv != null ? psv.getTripleStoreConfiguration() : null;
//
//		List<AnnotatorDocumentResponse> res = new ArrayList<>(); 
//				
//		for (AnnotatorDocument adoc : docs) {
////			AnnotationEditGroup aeg = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsProperty(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty()).get();
////			AnnotatorDocumentResponse adr = modelMapper.annotator2AnnotatorResponse(vc, adoc, aeg);
//				
//			AnnotatorDocumentResponse adr = modelMapper.annotator2AnnotatorResponse(vc, adoc);
//			res.add(adr);
//		}
//		return res;
//	}
	
	
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

	public List<ValueCount> getValuesForPage(AnnotatorDocument adoc, String onPropertyString, org.apache.jena.query.Dataset rdfDataset, int page, String fromClause, TripleStoreConfiguration vc) {
//	public List<ValueCount> getValuesForPage(AnnotatorDocument adoc, String onPropertyString, RDFAccessWrapper rdfDataset, int page, String fromClause, TripleStoreConfiguration vc) {
		
		// should also filter out URI values here but this would spoil pagination due to previous bug.
		String sparql = 
            "SELECT ?value ?valueCount WHERE { " +
			"  SELECT ?value (count(*) AS ?valueCount)" +
	        "  WHERE { " + 
            "    ?v a <" + OAVocabulary.Annotation + "> . " + 
		    "    ?v <" + OAVocabulary.hasTarget + "> ?target . " + 
//          "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
		    "    ?target <" + SOAVocabulary.onValue + "> ?value . "  + 
//		    "    FILTER (isLiteral(?value)) " +		         
		    "  } " +
			"  GROUP BY ?value " + 
			"  ORDER BY desc(?valueCount) ?value } " + 
 		    "LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1);


    	List<List<PathElement>> cPaths = new ArrayList<>();
//		if (adoc.getControlProperties() != null) {
//			for (ControlProperty extra : adoc.getControlProperties()) {
//				cPaths.add(PathElement.onPathElementListAsStringListInverse(extra.getOnProperty(), null));
//			}
//		}
		
    	List<ValueCount> values = new ArrayList<>();
//    	System.out.println(sparql);
//	    System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
    	try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
//    	try {
    		ResultSet rs = qe.execSelect();
//    		rdfDataset.execQuery(sparql);
    		
    		while (rs.hasNext()) {
//    		while (rdfDataset.hasNext()) {
    			QuerySolution qs = rs.next();
//    			rdfDataset.next();
//				RDFAccessWrapper qs = rdfDataset;

    			RDFNode value = qs.get("value");
    			
    			int count = qs.get("valueCount").asLiteral().getInt(); //valueCount is the number a value appears (not of annotations on value)
    			
    			ValueCount vct = new ValueCount(value, count);
    			
//    			if (adoc.getControlProperties() != null) {
//	        		PropertyValue pv = new PropertyValue(PathElement.onPathElementListAsStringListInverse(adoc.getOnProperty(), null), RDFTerm.createRDFTerm(value));
//	        		ValueResponseContainer<ValueResponse> vres = schemaService.getItemsForPropertyValue(vc.getSparqlEndpoint(), fromClause, pv, cPaths, 1, 10);
//	        		
//	        		vct.setControlGraph(vres.getExtra());
//    			}
    			
    			values.add(vct);
    		}
    	}
//    	finally {
//    		try {
//				rdfDataset.close();
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    	}
    	
    	return values;
		
	}
	
	public List<ValueCount> getMultiValuesForPage(AnnotatorDocument adoc, String onGraphTreeString, Map<String, IndexKeyMetadata> targets, org.apache.jena.query.Dataset rdfDataset, int page, String fromClause, TripleStoreConfiguration vc) {
//	public List<ValueCount> getMultiValuesForPage(AnnotatorDocument adoc, String onGraphTreeString, Map<String, IndexKeyMetadata> targets, RDFAccessWrapper rdfDataset, int page, String fromClause, TripleStoreConfiguration vc) {
		
		pageSize = 5;
		// should also filter out URI values here but this would spoil pagination due to previous bug.
		String bindings = "";
		List<String> variables = new ArrayList<>();
		for (Map.Entry<String, IndexKeyMetadata> entry : targets.entrySet()) {
			String key = entry.getKey();
			Boolean optional = entry.getValue().getOptional();
			if (optional != null && optional == true) {
				bindings += " ?r <" + SOAVocabulary.onBinding + "> ?vvv_" + key + " . ?vvv_" + key + " <" + SOAVocabulary.variable + "> \"" + key + "\" . OPTIONAL { ?vvv_" + key + " <" + SOAVocabulary.value + "> ?value_" + key + " } . ";
			} else {
				bindings += " ?r <" + SOAVocabulary.onBinding + "> ?vvv_" + key + " . ?vvv_" + key + " <" + SOAVocabulary.variable + "> \"" + key + "\" . ?vvv_" + key + " <" + SOAVocabulary.value + "> ?value_" + key + " . ";
			}
		
			variables.add("?value_" + key);
		}
		
		String variablesStrings = "";
		for (String s : variables) {
			variablesStrings += s + " ";
		}
		
		String sparql = 
            "SELECT " + variablesStrings + " ?valueCount WHERE { " +
			"  SELECT " + variablesStrings + " (count(*) AS ?valueCount)" +
	        "  WHERE { " + 
            "  ?v a <" + OAVocabulary.Annotation + "> . " + 
		    "  ?v <" + OAVocabulary.hasTarget + "> ?target . " + 
//            "  ?target <" + SOAVocabulary.onGraphTree + "> \"" + onGraphTreeString + "\" . " + 
		       bindings + "  "  + 
//		    "    FILTER (isLiteral(?value)) " +		         
		    "  } " +
			"  GROUP BY " + variablesStrings +
			"  ORDER BY desc(?valueCount) " + variablesStrings + " } " + 
 		    "LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1);


    	List<ValueCount> values = new ArrayList<>();
//	    System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
    	try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
//    	try {	
    		ResultSet rs = qe.execSelect();
//    		rdfDataset.execQuery(sparql);
    		
    		while (rs.hasNext()) {
//    		while (rdfDataset.hasNext()) {
    			QuerySolution qs = rs.next();
//    			rdfDataset.next();
//				RDFAccessWrapper qs = rdfDataset;
    			
    			List<RDFNode> vvalues = new ArrayList<>();
    			for (String v : variables) {
    				vvalues.add(qs.get(v));
    			}
    			
    			int count = qs.get("valueCount").asLiteral().getInt(); //valueCount is the number a value appears (not of annotations on value)
    			
    			ValueCount vct = new ValueCount(variables, vvalues, count);
    			
//    			if (adoc.getControlProperties() != null) {
//	        		PropertyValue pv = new PropertyValue(PathElement.onPathElementListAsStringListInverse(adoc.getOnProperty(), null), RDFTerm.createRDFTerm(value));
//	        		ValueResponseContainer<ValueResponse> vres = schemaService.getItemsForPropertyValue(vc.getSparqlEndpoint(), fromClause, pv, cPaths, 1, 10);
//	        		
//	        		vct.setControlGraph(vres.getExtra());
//    			}
//    			
    			values.add(vct);
    			
//    			System.out.println(variables + " " + vvalues + " " + count);
    		}
    	}
//    	finally {
//    		try {
//				rdfDataset.close();
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    	}
    	
    	return values;
		
	}
	
//	public ValueResponseContainer<ValueAnnotation> view(AnnotatorContainer ac, int page) throws Exception {
//
//		org.apache.jena.query.Dataset rdfDataset = serviceUtils.load(ac);
////		RDFAccessWrapper rdfDataset = serviceUtils.load(ac, RDFLibrary.JENA);
//		
//		AnnotatorDocument adoc = ac.getObject();
//		
//		Dataset ds = ac.getEnclosingObject();
//		DatasetCatalog dcg = schemaService.asCatalog(ds);
//		String fromClause = schemaService.buildFromClause(dcg);
//
//		String inSelect = "";
//		String inWhere = "";
//		List<IndexKeyMetadata> keyMetadata = adoc.getKeysMetadata();
//		
//		Map<String, Integer> fieldNameToIndexMap = new HashMap<>();
//		
//		AnnotatorContext ai = new AnnotatorContext();
//
//		if (adoc.getAnnotator() != null) {
//			DataService annotatorService = annotators.get(adoc.getAnnotator());
//			ai.setName(annotatorService.getTitle());
//		} else if (adoc.getAnnotatorId() != null) {
//			ai.setName(prototypeRepository.findById(adoc.getAnnotatorId()).get().getName());
//		}
//
//		if (adoc.getThesaurus() != null) {
//			Dataset dataset = datasetRepository.findByIdentifierAndDatabaseId(adoc.getThesaurus(), database.getId()).get();
//			ai.setId(dataset.getId());												
//			ai.setVocabularyContainer(datasetService.createVocabularyContainer(dataset.getId()));
//		} else {
//			ai.setId(ac.getEnclosingObject().getId());												
//			ai.setVocabularyContainer(datasetService.createVocabularyContainer(ac.getEnclosingObject().getId()));
//		}
//		
////		System.out.println("A");
//		if (adoc.getOnProperty() != null) { 
//			inSelect = " (count(DISTINCT ?value) AS ?valueCount) " ;
//			inWhere = " ?target <" + SOAVocabulary.onValue + "> ?value . ";
//		} else {
//			for (IndexKeyMetadata ikm : keyMetadata) {
//				inSelect = " (count(DISTINCT ?value_" + ikm.getIndex() + ") AS ?value" + ikm.getIndex() + "Count) " ;
//				if (ikm.getOptional() != null && ikm.getOptional() == true) {
//					inWhere += " OPTIONAL { ?r <" +  SOAVocabulary.onBinding + "> ?vvv_" + ikm.getIndex() + " .  ?vvv_" + ikm.getIndex() + " <" +  SOAVocabulary.value + "> ?value_" + ikm.getIndex() + " . ?vvv_" + ikm.getIndex() + " <" + SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" } . " ;
//				} else {
//					inWhere += " ?r <" +  SOAVocabulary.onBinding + "> ?vvv_" + ikm.getIndex() + " . ?vvv_" + ikm.getIndex() + " <" +  SOAVocabulary.value + "> ?value_" + ikm.getIndex() + " . ?vvv_" + ikm.getIndex() + " <" + SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" . " ;
//				}
//				fieldNameToIndexMap.put(ikm.getName(), ikm.getIndex());
//			}
//		}		
//		
//		String sparql = 
//				"SELECT (count(?v) AS ?annCount) (count(DISTINCT ?source) AS ?sourceCount) " + inSelect + 
//		        "WHERE { " +  
//		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
//			    " ?v <" + OAVocabulary.hasTarget + "> ?r . " + 
//		        " ?target <" + OAVocabulary.hasSource + "> ?source . " + 
//		           inWhere + 
//			    " } ";
//	
//		int annCount = 0;
//		int sourceCount = 0;
////		int valueCount = 0;
//		List<ResultCount> rc  = new ArrayList<>();
//		
//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
//		try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
////		try {
//			ResultSet rs = qe.execSelect();
////			rdfDataset.execQuery(sparql);
//			
//			if (rs.hasNext()) {
////			if (rdfDataset.hasNext()) {
//				QuerySolution sol = rs.next();
////				rdfDataset.next();
////				RDFAccessWrapper sol = rdfDataset;
//
//				
//				annCount = sol.get("annCount").asLiteral().getInt();
//				sourceCount = sol.get("sourceCount").asLiteral().getInt();
////				valueCount = sol.get("valueCount").asLiteral().getInt();
//				
//				if (adoc.getOnProperty() != null) {
//					rc.add(new ResultCount("" + 0, sol.get("valueCount").asLiteral().getInt()));
//				} else {
//					for (IndexKeyMetadata ikm : keyMetadata) {
//						if (sol.get("value" + ikm.getIndex() + "Count") != null) {
//							rc.add(new ResultCount("" + ikm.getIndex() , sol.get("value" + ikm.getIndex() + "Count").asLiteral().getInt()));
//						}
//					}
//				}	
//			}
//		}
////		finally {
////			rdfDataset.close();
////		}
//		
//		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
//		vrc.setTotalCount(annCount);
//		vrc.setDistinctSourceTotalCount(sourceCount);
//		vrc.setDistinctValueTotalCount(rc);
//
//		String spath = null;
//		List<ValueCount> values;
//		String variablesString = "";
//		String bindString = "";
//		ExecutionOptions eo = null;
//		
////		System.out.println("D");
//		if (adoc.getOnProperty() != null) {
//			spath = PathElement.onPathStringListAsSPARQLString(adoc.getOnProperty());
//			variablesString = "?value";
//			values = getValuesForPage(adoc, spath, rdfDataset, page, fromClause, ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values()));
//	        bindString = " ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" . " + 
//			             " ?r <" + SOAVocabulary.onValue + "> ?value . ";
//		} else {
//		    
//			SPARQLStructure ss = sparqlService.toSPARQL(adoc.getElement(), false);
//			spath = ss.getTreeWhereClause();
//			
//			eo = ac.buildExecutionParameters();
//			
//			values = getMultiValuesForPage(adoc, spath, eo.getTargets(), rdfDataset, page, fromClause, ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values()));
//		
//			String bindings = "";
//			List<String> variables = new ArrayList<>();
//			for (Map.Entry<String, IndexKeyMetadata> entry : eo.getTargets().entrySet()) {
//				String key = entry.getKey();
//				Boolean optional = entry.getValue().getOptional();
//
//				if (optional != null && optional == true) {
//					bindings += " OPTIONAL { ?r <" + SOAVocabulary.onBinding + "> ?vvv_" + key + " . ?vvv_" + key + " <" + SOAVocabulary.variable + "> \"" + key + "\" .  ?vvv_" + key + " <" + SOAVocabulary.value + "> ?value_" + key + " } . ";
//				} else {
//					bindings += " ?r <" + SOAVocabulary.onBinding + "> ?vvv_" + key + " . ?vvv_" + key + " <" + SOAVocabulary.variable + "> \"" + key + "\" . ?vvv_" + key + " <" + SOAVocabulary.value + "> ?value_" + key + " . ";
//				}
//				
//				variables.add("?value_" + key);
//				
//			}
//			
//			for (String s : variables) {
//				variablesString += s + " ";
//			}
//			
//			bindString = "  ?r <" + SOAVocabulary.onGraphTree + "> \"" + spath + "\" . " + 
//		                 "     " + bindings + " "; 
//		}
//		
//		Map<RDFTermHandler, ValueAnnotation> res = new LinkedHashMap<>();
//
//		Map<String, Set<String>> sbMap = new HashMap<>();
//    	
//    	for (ValueCount vc : values) { 
//    		RDFTermHandler aev = null;
//
//    		if (adoc.getOnProperty() != null) {
//	    		RDFNode value  = vc.getValue();
//
//	    		Set<String> sb = sbMap.get("");
//	    		if (sb == null) {
//	    			sb = new LinkedHashSet<>();
//	    			sbMap.put("", sb);
//	    		}
//	    		
//	    		if (value != null) { 
//		    		if (value.isLiteral()) {
//						Literal l = value.asLiteral();
//						String lf = l.getLexicalForm();
//						
//						sb.add(RDFTerm.createLiteral(lf, l.getLanguage(), l.getDatatype().getURI().toString()).toRDFString());
//						
//						aev = new SingleRDFTerm(value.asLiteral());
//					} else {
//						//ignore URI values. They should not be returned by getValuesForPage 
//						
//					}
//	    		}
//    		} else {
//    			List<String> names = vc.getNames();
//    			List<RDFNode> vvalues = vc.getValues();
//    			
//    			List<SingleRDFTerm> terms = new ArrayList<>();
//    			
//    			for (int i = 0; i < names.size(); i++) {
//    				String name = names.get(i);
//    				RDFNode value  = vvalues.get(i);
//    				
//    				Set<String> sb = sbMap.get(name);
//    	    		if (sb == null) {
//    	    			sb = new LinkedHashSet<>();
//    	    			sbMap.put(name, sb);
//    	    		}
//    	    		
//    	    		if (value != null) { 
//	    	    		if (value.isLiteral()) {
//	    					Literal l = value.asLiteral();
//	    					String lf = l.getLexicalForm();
//	    					
//	    					sb.add(RDFTerm.createLiteral(lf, l.getLanguage(), l.getDatatype().getURI().toString()).toRDFString());
//	    					
//	    		    		SingleRDFTerm st = new SingleRDFTerm(value.asLiteral());
//	    		    		
//	    		    		st.setName(eo.getTargets().get(name.substring(7)).getName());
//	    					terms.add(st);
//	    				} else {
//	    					//ignore URI values. They should not be returned by getValuesForPage 
//	    				}
//    	    		}
//    			}
//    			
//    			aev = new MultiRDFTerm(terms);
//    		}
//    		
//    		if (aev != null) {
//				ValueAnnotation va = new ValueAnnotation();
//				va.setOnValue(aev);
//				va.setCount(vc.getCount()); // the number of appearances of the value
//				
//				res.put(aev, va);
//    		}
//    	}
//
//    	boolean hasValues = true;
//    	
//    	String valuesString = "";
//    	if (adoc.getOnProperty() != null) {
//    		String vs = "";
//    		for (String s : sbMap.get("")) {
//    			vs += s + " " ;
//    		}
//    		if (vs.length() != 0) {
//    			valuesString = " VALUES ?value { " + vs  + " } ";
//    		} else if (vs.length() == 0) {
//    			hasValues = false;
//    		}
//    	} else {
//    		for (Map.Entry<String, Set<String>> entry : sbMap.entrySet()) {
//    			String vs = "";
//        		for (String s : entry.getValue()) {
//        			vs += s + " " ;
//        		}
//        		if (vs.length() != 0) {
//        			valuesString += " VALUES " + entry.getKey() + " { " + vs  + " } ";
//        		} else if (vs.length() == 0) {
//        			Boolean optional = eo.getTargets().get(entry.getKey().substring(7)).getOptional();
//        			if (optional != null && optional == true) {
//        			} else {
//        				hasValues = false;
//        			}
//        		}
//
//    		}
//    	}
////    	System.out.println("SM " + sbMap);
////    	System.out.println("VS " + valuesString);
////    	System.out.println("VS " + hasValues);
//
//    	boolean oneByOne = false;
//    			
//    	if (!oneByOne) {
//			sparql =   //"SELECT " + variablesString + " ?t ?start ?end ?score ?count {" + 	
//					   "SELECT distinct " + variablesString + " ?t ?bp ?bv ?start ?end (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count) " + 
//				        "WHERE { " +  
//				        " ?v <" + OAVocabulary.hasTarget + "> ?r . " + 
//					    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } ." +
//				        "OPTIONAL { ?v <http://sw.islab.ntua.gr/annotation/hasBodyStatement> [ <" + RDFSVocabulary.predicate + "> ?bp ; <" + RDFSVocabulary.object + "> ?bv ] } . " +
//					    "  ?r <" + OAVocabulary.hasSource + "> ?s . " + 
//	                    bindString + " " +
//					    valuesString + 
//					    " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +
//					    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
//					    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " + " } " +
//			            "GROUP BY ?t " + variablesString + " ?bp ?bv ?start ?end " +
//	//					"} " +
//			            " ORDER BY DESC(?count) " + variablesString + " ?start ?end ?bp ?bv ";
//	    	
//	//		System.out.println(sparql);
//	    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
//			
//			if (hasValues) {
//	
//				try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
////				try {
//					ResultSet rs = qe.execSelect();
////					rdfDataset.execQuery(sparql);
//					
//					String pann = null;
//					String pie = null;
//					Integer pstart = null;
//					Integer pend = null;
//					int pcount = -1;
//					Double pscore = null;
//					ValueAnnotationDetail pvad = null;
//					
//					while (rs.hasNext()) {
////					while (rdfDataset.hasNext()) {
//						QuerySolution sol = rs.next();
////						rdfDataset.next();
////						RDFAccessWrapper sol = rdfDataset;
//						
////						System.out.println(sol);
//						RDFTermHandler aev = null;
//						if (adoc.getOnProperty() != null) {
//							RDFNode value = sol.get("value");
//							
//							if (value.isResource()) {
//								aev = new SingleRDFTerm(value.asResource());
//							} else if (value.isLiteral()) {
//								aev = new SingleRDFTerm(value.asLiteral());
//							}
//						} else {
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
//	//						System.out.println(list);
//							
//							aev = new MultiRDFTerm(list);
//						}
//						
//						String ann = sol.get("t") != null ? sol.get("t").toString() : null;
//						String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
//						Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
//						Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
//						int count = sol.get("count").asLiteral().getInt();
//						Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
//						
//						String bp = sol.get("bp") != null ? sol.get("bp").toString() : null;
//						String bv = sol.get("bv") != null ? sol.get("bv").toString() : null;
//						
//						ValueAnnotation va = res.get(aev);
//						
//						if (va != null && ann != null) {
//							va.setCount(count);
//							
//							ValueAnnotationDetail vad = null;
//							
//							if (isEqual(ann,pann) && isEqual(ie,pie) && isEqual(start,pstart) && isEqual(end,pend) && count == pcount && isEqual(score,pscore)) {
//								vad = pvad;
//							} else {
//								vad = new ValueAnnotationDetail();
//								vad.setValue(ann);
//								vad.setValue2(ie);
//								vad.setStart(start);
//								vad.setEnd(end);
//		//						vad.setCount(count); // the number of appearances of the annotation 
//		//						                     // it is different than the number of appearances of the value if multiple annotations exist on the same value
//								
//								vad.setScore(score);
//								
//								vad.setAnnotatorInfo(ai);
//								if (ai.getVocabularyContainer() != null) {
//									vad.setLabel(vocabularyService.getLabel(ann, ai.getVocabularyContainer().resolve(ann), false));
//								} else {
//									vad.setLabel(vocabularyService.getLabel(ann, null, true));
//								}
//								
//								va.getDetails().add(vad);
//								
//								pvad = vad;
//							}
//							
//							if (bp != null && bv != null) {
//								vad.addField(bp, bv);
//							}
//							
//							pann = ann;
//							pie = ie;
//							pstart = start;
//							pend = end;
//							pcount = count;
//							pscore = score;
//						} 
//					}
//					
////					System.out.println("EXEC 4");
//	
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//    	}
//    	
////		System.out.println("H");
//		
//		vrc.setValues(new ArrayList<>(res.values()));
//		
//		return vrc;
//    }

	private static int PAGE_SIZE = 50;
	
	public ListResult<ValueAnnotation> view2(AnnotatorContainer ac, int page) throws Exception {

		org.apache.jena.query.Dataset rdfDataset = serviceUtils.load(ac);
//		RDFAccessWrapper rdfDataset = serviceUtils.load(ac, RDFLibrary.JENA);
		
		AnnotatorDocument adoc = ac.getObject();
		AnnotatorContext ai = annotationUtils.createAnnotationContext(adoc, ac.getEnclosingObject());
		
		AnnotationUtilsContainer auc = annotationUtils.createAnnotationUtilsContainer(ac, null);
		
		ValueResponseContainer<ValueAnnotation> vrc = annotationUtils.countAnnotations(ac, auc.getKeyMetadata(), null, rdfDataset, auc.getInSelect(), "", "", auc.getInWhere(), ""); 
	
		String isparql = "SELECT ?t ?start ?end (AVG(?sc) AS ?score) (count(distinct ?s) AS ?count) " + auc.getGroupBodyVariables() +
				"{ " +
			    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		        "     <" + OAVocabulary.hasTarget + "> ?r . " + 
		        "  ?r <" + OAVocabulary.hasSource + "> ?s . " + 			    
			    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } " +  
//			    auc.getBindString() + 
			    " #####VALUES##### " + 
                auc.getBodyClause() + 
                " OPTIONAL { ?v <" + SOAVocabulary.score + "> ?sc } . " +					
			    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start } . " + 
			    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } . " +
			    "} " + 
			    "GROUP BY ?t ?start ?end " + 
				"ORDER BY ?start ?end" ;
			
		String vsparqlCore = 
				" WHERE { " + 
			    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		        "     <" + OAVocabulary.hasTarget + "> ?r . " + 		
		        auc.getInWhere() +
	            "}" + 
			    "GROUP BY " + auc.getVariablesString();
		
		String cvsparql = 
				"SELECT (COUNT(*) AS ?count) " +
				" { " +
				" SELECT " + auc.getVariablesString() + 
				 vsparqlCore + " " +
				"} ";
		
		
		String vsparql = "SELECT " + auc.getVariablesString() + // " ?count " +
				" { " +
//				"SELECT DISTINCT " + auc.getVariablesString() + // " (COUNT(distinct ?v) AS ?count) " +
				"SELECT " + auc.getVariablesString() + // " (COUNT(distinct ?v) AS ?count) " +
				vsparqlCore + " " +
				"ORDER BY desc(?count) " + auc.getVariablesString() +
				"} LIMIT " + PAGE_SIZE + " OFFSET " + PAGE_SIZE * (page - 1);

		
		Pagination pg = annotationUtils.createPagination(null, rdfDataset, cvsparql, page, PAGE_SIZE);
		
//    	System.out.println("VSPARQL");
//    	System.out.println(vsparql);
//    	System.out.println(QueryFactory.create(cvsparql, Syntax.syntaxARQ));
//    	System.out.println(QueryFactory.create(vsparql, Syntax.syntaxARQ));
    	
		Map<RDFTermHandler, ValueAnnotation> res = new LinkedHashMap<>();

		//grouping does not work well with paging!!! annotations of some group may be split in different pages
		//it should be fixed somehow;
		//also same blank node annotation are repeated

		try (QueryExecution vqe = QueryExecutionFactory.create(QueryFactory.create(vsparql, Syntax.syntaxSPARQL_11), rdfDataset)) { 
			
//			Map<String, AnnotatorContext> annotatorInfoMap = new HashMap<>();

			ResultSet vrs = vqe.execSelect();
			
			int index = 0;
			while (vrs.hasNext()) {
//			while (vqe.hasNext()) {
				QuerySolution vsol = vrs.next();
//				BindingSet vsol = vqe.next();
				
				index++;
				
//				int count = vsol.get("count").asLiteral().getInt();
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
				
				RDFTermHandler aev = annotationUtils.createValueRDFTermHandler(ac, vsol, auc.getrNameToEntryMap());
				
				ValueAnnotation va = new ValueAnnotation();
				va.setOnValue(aev);
				va.setIndex(PAGE_SIZE * (page - 1) + index);
//				va.setCount(count);
						
				res.put(aev, va);
					
				
				int pos = isparql.indexOf("#####VALUES#####");
				String csparql = isparql.substring(0, pos) + restr + isparql.substring(pos + "#####VALUES#####".length());
//				String csparql = isparql.replaceAll("#####VALUES#####", restr); // causes problems with special characters.
				
//				System.out.println("CSPARQL");
//				System.out.println(csparql);
//		    	System.out.println(QueryFactory.create(csparql, Syntax.syntaxSPARQL_11));
		    	
				try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(csparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
					
					ResultSet rs = qe.execSelect();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						
						String ann = sol.get("t") != null ? sol.get("t").toString() : null;
						Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
						Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
						int count = sol.get("count").asLiteral().getInt();
						Double score = sol.get("score") != null ? sol.get("score").asLiteral().getDouble() : null;
						
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

						if (auc.getBodyVariables().size() > 0) {
							Model model = ModelFactory.createDefaultModel();
							Resource r = ResourceFactory.createResource();
			    			for (int j = 0; j < auc.getBodyVariables().size(); j++) {
			    				if (sol.get(auc.getBodyVariables().get(j)) != null) {
			    					for (String v : sol.get(auc.getBodyVariables().get(j)).toString().split("\\|\\|")) {
			    						model.add(r, ResourceFactory.createProperty(adoc.getBodyProperties().get(j)), v);
			    					}
			    				}
			    			}
			    			
			    			if (model.size() > 0) {
				    			Map<String, Object> jn = apiUtils.jsonLDFrame(model, new HashMap<>());
				    			vad.setControlGraph(jn);
			    			}
						}			    			
		    			
						va.getDetails().add(vad);
						va.setCount(count); // ?correct????
						
						
					}
				}
			}
			
			pg.setCurrentElements(index);
		}

		ListResult<ValueAnnotation> list = new ListResult<>(new ArrayList<>(res.values()), pg);
		list.setMetadata(vrc);
		
		return list;
    }
	
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
	
//	public ValueResponseContainer<ValueAnnotation> clusterview(AnnotatorContainer ac, int page) throws Exception {
//
//		org.apache.jena.query.Dataset rdfDataset = serviceUtils.load(ac);
//		
//		AnnotatorDocument adoc = ac.getObject();
////		AnnotatorContext ai = annotationUtils.createAnnotationContext(adoc, ac.getEnclosingObject());
//		
//		Map<String, Set<String>> clustering = new HashMap<>();
//		
//		String sparql = 
//				"SELECT ?source ?body " + 
//		        "WHERE { " +  
//		        " ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
//			    " ?v <" + OAVocabulary.hasTarget + "> [ <" + OAVocabulary.hasSource + "> ?source ] . " + 
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
////				if (source.equals("http://data.europa.eu/s66/resource/organisations/af9c9f0c-9432-3942-84dd-91ea1576af53")) {
////					System.out.println("B " + body);
////				}
////				if (body.equals("http://data.europa.eu/s66/resource/organisations/af9c9f0c-9432-3942-84dd-91ea1576af53")) {
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
//
////		SPARQLStructure ss = sparqlService.toSPARQL(adoc.getElement(), false);
//		
//    	DatasetCatalog dcg = schemaService.asCatalog(ac.getEnclosingObject());
//    	String fromClause = schemaService.buildFromClause(dcg);
//
//    	TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
//
//		Set<String> all = new TreeSet<>();
////		int i = 1;
//		Set<Set<String>> sclusters = new HashSet<>();
//		sclusters.addAll(clustering.values());
//
//		List<Set<String>> clusters = new ArrayList<>();
//		clusters.addAll(sclusters);
//		Collections.sort(clusters, new ClusterComparator());
//
//		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
//		vrc.setTotalCount(clusters.size());
//
//		List<ValueAnnotation> vaList = new ArrayList<>();
//		
//		int cj = 1;
//		for (Set<String> values : clusters) {
////			System.out.println(i++ + " " + values);
//			all.addAll(values);
//			
//			String s = "";
//			for (String v : values) {
//				s += " <" + v + ">";
//			}
//			
//			String lsparql = 
//					"SELECT DISTINCT ?label " + 
//			        fromClause +
//			        "WHERE { " +  
//			        " ?v <http://data.europa.eu/s66#legalName> ?label . " + 
//			        " VALUES ?v { " + s + " }  } ";
//			
//			System.out.println("");
//			System.out.println("CLUSTER " + cj++ + " [" + values.size() + "]" );
//			List<String> labels = new ArrayList<>();
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
//				ResultSet rs = qe.execSelect();
//				
//				while (rs.hasNext()) {
//					QuerySolution sol = rs.next();
//						
//					String label = sol.get("label").toString();
////					labels.add(label);
//					System.out.println(label);
//				}
//			}
//			
////			System.out.println(labels);
//		}
//		vrc.setDistinctSourceTotalCount(all.size());
//		
////		Map<Integer, IndexKeyMetadata> keyMap = adoc.getKeyMetadataMap();
//		
//		String cExtra = "";
//		String wExtra = "";
//
//    	List<List<PathElement>> cPaths = new ArrayList<>();
//		if (adoc.getControlProperties() != null) {
//			for (List<String> extra : adoc.getControlProperties()) {
//				cPaths.add(PathElement.onPathElementListAsStringListInverse(extra, null));
//			}
//			
//			for (int j = 0; j < cPaths.size(); j++) {
//				List<PathElement> extraPath = cPaths.get(j);
//				String pew = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(extraPath), "?VAR_ZZZ_" + j + "_");
//				wExtra += " OPTIONAL { ?s " + pew + "?VAR_ZZZ_" + j + " } ";
//				
//				List<PathElement> extraResultPath = new ArrayList<>();
//				extraResultPath.add(extraPath.get(extraPath.size() - 1));
//				String pec = PathElement.onPathStringListAsSPARQLString(PathElement.onPathElementListAsStringList(extraPath), "?VAR_ZZZ_" + j + "_");
//				
//				cExtra += " ?s " + pec + "?VAR_ZZZ_" + j ;
//			}
//
//		}
//		
//		for (int k = (page - 1)*pageSize; k < Math.min(clusters.size(), page*pageSize) ; k++) {
//
//			ValueAnnotation va = new ValueAnnotation();
//			vaList.add(va);
//			
//			va.setCount(clusters.get(k).size());
//			
//			for (String s : clusters.get(k)) {
//				ValueAnnotationDetail vad = new ValueAnnotationDetail();
//				vad.setValue(s);
//				
//    			if (adoc.getControlProperties() != null) {
//    				
//    				String valuesSparql = 
//    						"CONSTRUCT { " +
//    							cExtra + 
//    				        "} " +
//    					    fromClause + 
//    						"WHERE { VALUES ?s { <" + s + "> } " +
//    							wExtra +
//    						"} ";
//
//    				        
////    				System.out.println(valuesSparql);
////    				System.out.println(QueryFactory.create(valuesSparql));
////    				System.out.println(QueryFactory.create(countSparql));
//    				
//    				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(valuesSparql, Syntax.syntaxSPARQL_11))) {
//    					Model model = qe.execConstruct();
//    					
//    			    	Map<String, Object> jn = apiUtils.jsonLDFrame(model, new HashMap<>());
//    			    	
//    			    	vad.setControlGraph(jn);
//    				}
//    			}
//    			
//				va.getDetails().add(vad);
//			}
//			
//		}
//		
//		vrc.setValues(vaList);
//
//		return vrc;
//    }
	
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
				int c = fields.get(i).toString().compareTo(o.fields.get(i).toString());
				
				if (c != 0) {
					return c;
				}
					
			}
			
			return 0;
		}
	}
	

	public ValueResponseContainer<ValueAnnotation> clusterview(AnnotatorContainer ac, int page) throws Exception {

		AnnotatorDocument adoc = ac.getObject();

    	DatasetCatalog dcg = schemaService.asCatalog(ac.getEnclosingObject());
    	String fromClause = schemaService.buildFromClause(dcg);
    	
    	TripleStoreConfiguration vc = dcg.getDataset().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
    	
		Map<String, Set<String>> clustering = new HashMap<>();
		Map<String, Set<Link>> links = new HashMap<>();
		
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
						
						ClustererService.buildClusters(clustering, ((JenaAccessWrapper)ds).getDataset(), fromClause,vc);
//						ClustererService.buildClusters1(links, ((JenaAccessWrapper)ds).getDataset(), fromClause,vc);
					}
				}
				
//				for (Map.Entry<String, Set<Link>> entry : links.entrySet()) {
//					System.out.println(entry);
//					
//					String source = entry.getKey();
//					
//					Double sc = null;
//					int size = entry.getValue().size();
//					int c = 0;
//					for (Link l : entry.getValue()) {
//						if (c++ < Math.ceil(size*0.5)) {
//							ClustererService.addClusterElement(clustering, source, l.getTarget());
//						} else {
//							break;
//							
//						}
//					}
//				}
			}
		}
		


		Set<String> all = new TreeSet<>();
//		int i = 1;
		
		Set<Set<String>> sclusters = new HashSet<>();
		sclusters.addAll(clustering.values());

		List<Set<String>> clusters = ClustererService.finalizeClusters(clustering);
//		ClustererService.checkClusters(clusters, fromClause, vc);

		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
		vrc.setTotalCount(clusters.size());

		List<ValueAnnotation> vaList = new ArrayList<>();
		
////		String cExtra = "";
//		String wExtra = "";
//		String selectVars = "";
//		List<String> selectVarsList = new ArrayList<>();
//
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
//		
////		System.out.println(cExtra);
////		System.out.println(wExtra);
//		
//		Map<Integer,Counter> histogram = new TreeMap<>();
//		int cj = 1;
//		for (Set<String> values : clusters) {
////			System.out.println(i++ + " " + values);
//			all.addAll(values);
//			
//			String s = "";
//			for (String v : values) {
//				s += " <" + v + ">";
//			}
//			
////			String lsparql = 
////					"SELECT DISTINCT ?label " + 
////			        fromClause +
////			        "WHERE { " +  
////			        " ?v <http://data.europa.eu/s66#legalName> ?label . " +
////			        " VALUES ?v { " + s + " }  } ";
//
////			String lsparql = 
////					"SELECT " + selectVars + " (GROUP_CONCAT(?fp;separator=\"|\") AS ?gfp) (GROUP_CONCAT(?pic;separator=\"|\") AS ?gpic) " + 
////			        fromClause +
////			        "WHERE { " +  
//////			        " ?v <http://data.europa.eu/s66#legalName> ?label . " +
////                    wExtra +
////			        " OPTIONAL { ?s <http://purl.org/dc/terms/identifier> ?ofp } . " +
////			        " OPTIONAL { ?s <http://data.europa.eu/s66#identifier> ?pic } . " +
////			        " VALUES ?s { " + s + " }  " +
////			        " BIND(if(bound(?ofp),?ofp,\"FPH\") AS ?fp) } " +
////			        " GROUP BY " + selectVars ;
////			
//////			System.out.println(lsparql);
//////			System.out.println(QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11));
////			
////			Set<String> labels = new TreeSet<>();
////			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
////				ResultSet rs = qe.execSelect();
////				
////				while (rs.hasNext()) {
////					QuerySolution sol = rs.next();
////						
////					String label = sol.get("?VAR_ZZZ_0").toString();
////					Object gfp = sol.get("gfp");
////					
////					Set<String> fpSet = new TreeSet<>();
////					if (gfp != null) {
////						for (String v : gfp.toString().split("\\|")) {
////							if (v.length() > 0) {
////								fpSet.add(v.substring(0, 3));
////							}
////						}
////					}
////					
////					Object gpic = sol.get("gpic");
////					Set<String> picSet = new TreeSet<>();
////					if (gpic != null) {
////						for (String v : gpic.toString().split("\\|")) {
////							if (v.length() > 0) {
////								picSet.add(v);
////							}
////						}
////					}
////					
////					labels.add(label);
//////					System.out.println(label + " " + (fpSet.size() > 0 ? fpSet : "") + " " + (picSet.size() > 0 ? picSet : "") );
//////					System.out.println(label);
////				}
////			}
////			
////			System.out.println("");
////			System.out.println("CLUSTER " + cj++ + " [" + values.size() + "] - [" + labels.size() + "]" );
////
////			for (String ss : labels) {
////				System.out.println(ss);
////			}
//			
////			System.out.println(labels);
//			
//			Counter cc = histogram.get(values.size());
//			if (cc == null) {
//				cc = new Counter(0);
//				histogram.put(values.size(), cc);
//			}
//			cc.increase();
//		}
//		
//		for (Map.Entry<Integer, Counter> entry : histogram.entrySet()) {
//			System.out.println(entry.getKey() + ";" + entry.getValue().getValue());
//		}
		
		SchemaSelector control = adoc.getControl();
		
		SPARQLStructure ss = sparqlService.toSPARQL(control.getElement(), AnnotatorDocument.getKeyMetadataMap(control.getKeysMetadata()), false);
		String returnVariables = ss.returnVariables(false, false);
		String filterClauses = ss.filterClauses(true, false, false);
		String whereClause = ss.whereClause();
		String predefinedValueClauses = ss.predefinedValueClauses(); // fixed values given by the annotator should go at end of query after bind
		String groupByClause = ss.getGroupByClause();

		int total = 0;
		int problematic = 0;
		
		for (int k = 0; k < clusters.size(); k++) {
			Set<String> values = clusters.get(k);

			total += values.size();
			
			String uris = "";
			for (String v : values) {
				uris += " <" + v + ">";
			}
			
			String lsparql = 
					"SELECT ?c_0 " + returnVariables + " " + 
			        fromClause + 
			        " WHERE { " + 
			        whereClause + " " + filterClauses + " " + predefinedValueClauses + 
			        " VALUES ?c_0 { " + uris + " } } " + groupByClause ;
			
//			System.out.println(lsparql);
//			System.out.println(QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11));
//			System.out.println(ss.getKeys());

			Map<Fields, List<String>> fieldsToUriMap = new TreeMap<>();
			Map<String, List<String>> uriToLabelsMap = new HashMap<>();
			Map<String, List<String>> uriToWordsMap = new HashMap<>();
			Map<String, Set<String>> wordToLabelMap = new HashMap<>();
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
						
					String uri = sol.get("c_0").toString();
					
					List<String> uriLabels = uriToLabelsMap.get(uri);
					if (uriLabels == null) {
						uriLabels = new ArrayList<>();
						uriToLabelsMap.put(uri, uriLabels);
					} 
					
					List<String> uriWords = uriToWordsMap.get(uri);
					if (uriWords == null) {
						uriWords = new ArrayList<>();
						uriToWordsMap.put(uri, uriWords);
					} 
					
					
					Fields fields = new Fields();
					for (String v : ss.getKeys()) {
						if (sol.get(v) != null) {
							RDFNode value = sol.get(v);
							
							if (ss.isGroupBy()) { // sort concatenated values
								Set<String> set = new TreeSet<>();
								for (String s : value.toString().split("\\|")) {
									set.add(s);
								}
								value = ResourceFactory.createStringLiteral(String.join("|", set));
							}
							
							fields.addValue(value);

							for (String label : value.toString().split("\\|")) {
								uriLabels.add(label);
								
								for (String vv : label.split(" ")) {
									uriWords.add(vv);
								}
							}							
						} else {
							fields.addValue(null);
						}
					}
					
					List<String> array = fieldsToUriMap.get(fields);
					if (array == null) {
						array = new ArrayList<>();
						fieldsToUriMap.put(fields, array);
					}
					array.add(uri);
					
				}
			}

    			
    		System.out.println("");
    		System.out.println("CLUSTER " + (k + 1) + " [" + values.size() + "] >> [" + fieldsToUriMap.keySet().size() + "]" );

    		for (Map.Entry<Fields, List<String>> fentry : fieldsToUriMap.entrySet()) {
//    			System.out.println((fentry.getKey().getValues().size() > 1 ? fentry.getKey().getValues() : fentry.getKey().getValues().get(0)) + " >> " + fentry.getValue().size());
    			System.out.println((fentry.getKey().getValues().size() > 1 ? fentry.getKey().getValues() : fentry.getKey().getValues().get(0)));
    		}
//    		System.out.println(values);
    		
//    		Map<String, Counter> wordMap = new HashMap<>();
    		List<Set<String>> labelWords = new ArrayList<>();
//    		Set<Set<String>> labelWords = new HashSet<>();
    		Set<Pair> allWords = new TreeSet<>();

    		
//    		System.out.println("COMPUTATION");
//    		System.out.println(uriToLabelsMap);
//    		System.out.println(wordToUrisMap);
    		int count = 0;
    		
    		List<Set<String>> rows = new ArrayList<>();
    		for (Fields fentry : fieldsToUriMap.keySet()) {
    			Set<String> row = new HashSet<>();
				for(RDFNode label : fentry.getValues()) {
					for (String s : label.toString().split("\\|")) {
						row.add(s);
					}
				}
				rows.add(row);
//				System.out.println("ROW " + row );
    		}
    		
    		//------------------------------------------------------
    		
    		
//    		{
//    		int jj = 0;
//    		List<Set<String>> rowsX = new ArrayList<>();
//    		for (Set<String> row : rows) {
//    			Set<String> rowX = new HashSet<>();
//    			for (String srow : row) {
//    				for (String s : srow.split(" ")) {
//						rowX.add(s);
//					}
//				}
//    			rowsX.add(rowX);
////    			System.out.println("ROW " + jj++ + " " + rowX);
//    		}
//    		
//    		hclustering(rowsX);
//    		}
    		
    		
    		for (int i = 0; i < rows.size(); i++) {
    			Set<String> rowi = rows.get(i);
    			for (int j = i + 1; j < rows.size(); ) {
    				Set<String> rowj = rows.get(j);
    				
    				Set<String> ns1 = new HashSet<>();
    				for (String s : rowi) {
    					ns1.add(s.replaceAll(" ",""));
    				}

    				Set<String> ns2 = new HashSet<>();
    				for (String s : rowj) {
    					ns2.add(s.replaceAll(" ",""));
    				}

//    				ns1.retainAll(ns2);
//    				
//    				if (ns1.size() > 0) {
//    					rowi.addAll(rowj);
//    					rows.remove(j);
//    				} else {
//    					j++;
//    				}

    				
    				boolean merge = false;
    				loop:
    				for (String s1 : ns1) {
    					for (String s2 : ns2) {
    						if (StringDistances.levenshteinDistance(s1, s2) < 0.1) {
    							merge = true;
    							break loop;
    						}
    					}
    				}
    				
	    			if (merge) {
						rowi.addAll(rowj);
						rows.remove(j);
					} else {
						j++;
					}
    			}
    		}

    		for (Set<String> row : rows) {
				Set<String> wordSet = new HashSet<>();
				for(String label : row) {
					for (String s : label.split("[ \\|]")) {
						wordSet.add(s);
						allWords.add(new Pair(Arrays.asList(new String [] { s }), 0));
						
						Set<String> list = wordToLabelMap.get(s);
						if (list == null) {
							list = new HashSet<>();
							wordToLabelMap.put(s, list);
						}
						
						list.add(count + "");
						
					}
				}
				
//				System.out.println(row + " >>>> " + wordSet);
				
				count++;
				
//				System.out.println(fentry.getKey() + " >>> " + wordSet);
				
				labelWords.add(wordSet);
    		}
    		
    		l1:
    		for (int i = 0; i < labelWords.size(); ) {
    			Set<String> rowi = labelWords.get(i);
    			for (int j = i + 1; j < labelWords.size(); ) {
    				Set<String> rowj = labelWords.get(j);
    				
	    			if (rowi.containsAll(rowj)) {
	    				labelWords.remove(j);
						continue;
					} else if (rowj.containsAll(rowi)) {
						labelWords.remove(i);
						continue l1;
					}
	    			
	    			j++;
    			}
    			
    			i++;
    		}
    		
    		
//    		for (Fields fentry : fieldsToUriMap.keySet()) {
//    			
//				Set<String> wordSet = new HashSet<>();
//				for(RDFNode label : fentry.getValues()) {
//					for (String s : label.toString().split("[ \\|]")) {
//						wordSet.add(s);
//						allWords.add(new Pair(Arrays.asList(new String [] { s }), 0));
//						
//						Set<String> list = wordToLabelMap.get(s);
//						if (list == null) {
//							list = new HashSet<>();
//							wordToLabelMap.put(s, list);
//						}
//						
//						list.add(count + "");
//						
//					}
//				}
//				
//				count++;
//				
////				System.out.println(fentry.getKey() + " >>> " + wordSet);
//				
//				labelWords.add(wordSet);
//    		}
    		
//    		System.out.println(uriToLabelsMap);
//    		System.out.println(wordMap);
//    		System.out.println(wordIndexMap)

    		if (labelWords.size() > 1) {
        		allWords = compute(labelWords, wordToLabelMap, allWords);

        		if (allWords.size() > 1) {
	    			problematic++;
	        		System.out.println("");
	        		System.out.println("CLUSTER " + (k + 1) + " [" + values.size() + "] >> [" + fieldsToUriMap.keySet().size() + "]" );
	    
	        		for (Map.Entry<Fields, List<String>> fentry : fieldsToUriMap.entrySet()) {
	//        			System.out.println((fentry.getKey().getValues().size() > 1 ? fentry.getKey().getValues() : fentry.getKey().getValues().get(0)) + " >> " + fentry.getValue().size());
	        			System.out.println((fentry.getKey().getValues().size() > 1 ? fentry.getKey().getValues() : fentry.getKey().getValues().get(0)));
	        		}
	        		
//	        		hclustering(labelWords);
	        		
	        		System.out.println("LABEL WORDS ");
	        		for (Set<String> p : labelWords) {
	        			Set<String> orderedDoc = new TreeSet<>(p);
	        			System.out.println(orderedDoc);
	        		}
	        		
        		}
        	}


		}	
		
		System.out.println();
		System.out.println("TOTAL " + total);
//		System.out.println("PROBLEMANTIC " + problematic);
		
		
		
		vrc.setDistinctSourceTotalCount(all.size());

//		for (Map.Entry<Integer, Counter> entry : histogram.entrySet()) {
//			System.out.println(entry.getKey() + ";" + entry.getValue().getValue());
//		}
		
		for (int k = (page - 1)*pageSize; k < Math.min(clusters.size(), page*pageSize) ; k++) {
			Set<String> values = clusters.get(k);

//			String uris = "";
//			for (String v : values) {
//				uris += " <" + v + ">";
//			}
//			
//			
//			String lsparql = 
//					"SELECT ?s " + selectVars + " " + 
//			        fromClause +
//			        "WHERE { " +  
//			        wExtra + 
//			        " VALUES ?s { " + uris + " }  } ";
//			
////			System.out.println(QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11));
//
//			Map<Fields, List<String>> labelToValuesMap = new HashMap<>();
//			
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
//				ResultSet rs = qe.execSelect();
//				
//				while (rs.hasNext()) {
//					QuerySolution sol = rs.next();
//						
//					String uri = sol.get("s").toString();
//					
//					Fields fields = new Fields();
//					for (String v : selectVarsList) {
//						if (sol.get(v) != null) {
//							fields.addValue(sol.get(v));
//						} else {
//							fields.addValue(null);
//						}
//					}
//					
//					List<String> array = labelToValuesMap.get(fields);
//					if (array == null) {
//						array = new ArrayList<>();
//						labelToValuesMap.put(fields, array);
//					}
//					array.add(uri);
//				}
//			}
			
			String uris = "";
			for (String v : values) {
				uris += " <" + v + ">";
			}
			
			String lsparql = 
					"SELECT ?c_0 " + returnVariables + " " + 
			        fromClause + 
			        " WHERE { " + 
			        whereClause + " " + filterClauses + " " + predefinedValueClauses + 
			        " VALUES ?c_0 { " + uris + " } }" ;
			
//			System.out.println(QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11));
//			System.out.println(ss.getKeys());

			Map<Fields, List<String>> labelToValuesMap = new HashMap<>();
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql, Syntax.syntaxSPARQL_11))) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
						
					String uri = sol.get("c_0").toString();
					
					Fields fields = new Fields();
					for (String v : ss.getKeys()) {
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
			
			Map<Integer, List<String>> indexToPropertiesMap = control.indexToPropertiesMap();
			
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
				
    			for (int j = 0; j < control.getKeysMetadata().size(); j++) {
    				if (entry.getKey().fields.get(j) != null) {
    					List<String> props = indexToPropertiesMap.get(control.getKeysMetadata().get(j).getIndex());
    					for (String p : props) {
    						model.add(ResourceFactory.createResource(), ResourceFactory.createProperty(p), entry.getKey().fields.get(j));
    					}
    				}
				}
    			
//    			model.write(System.out, "TTL");
    			
    			if (model.size() > 0) {
	    			Map<String, Object> jn = apiUtils.jsonLDFrame(model, new HashMap<>());
	    			vad.setControlGraph(jn);
    			}
    			
				va.getDetails().add(vad);
			}
			
		}
		
		vrc.setValues(vaList);

		return vrc;
    }
	
    public static float[] proximity(List<Set<String>> data) {
        int n = data.size();
        if (n > 65535) {
            throw new IllegalArgumentException("Data size " + n + " > 65535");
        }
        int length = (int) ((long) n * (n+1) / 2);

        float[] proximity = new float[length];
        IntStream.range(0, n).parallel().forEach(i -> {
            String si = String.join("", data.get(i));

        	for (int j = 0; j < i; j++) {
                int k = length - (n-j)*(n-j+1)/2 + i - j;
                
                String sj = String.join("", data.get(j));
                
                proximity[k] = new org.apache.commons.text.similarity.LevenshteinDistance().apply(si, sj);
            }
        });

        return proximity;
    }
	
	private void hclustering(List<Set<String>> docs) {
		List<Set<String>> documents = new ArrayList<>();
		
//		System.out.println("DOCUMENTS ");
		for (Set<String> p : docs) {
			Set<String> orderedDoc = new TreeSet<>(p);
			documents.add(orderedDoc);
//			System.out.println(orderedDoc);
		}
		
		Set<String> documentsWords = new TreeSet<>();
		for (Set<String> p : documents) {
			documentsWords.addAll(p);
		}
//		
		System.out.println("DOCUMENTS WORDS " + documentsWords);
		Map<String, Integer> wordIndexMap = new HashMap<>();
		Map<Integer, String> indexWordMap = new HashMap<>();
		
		int i = 0; 
		for (String w : documentsWords) {
			wordIndexMap.put(w, i);
			indexWordMap.put(i, w);
			i++;
		}
//		
		List<Set<String>> documentsAsArray = new ArrayList<>(documents);
		
		double[][] data = new double[documents.size()][documentsWords.size()];
		for (int j = 0 ; j < documentsAsArray.size(); j++) {
			for (String s : documentsAsArray.get(j)) {
				data[j][wordIndexMap.get(s)] = 1;
			}
			
//			System.out.println(Arrays.toString(data2[j]));
		}
		
		float[] proximity = proximity(documents);
		System.out.println("CLUSTERING 2");
//		HierarchicalClustering clusters = HierarchicalClustering.fit(UPGMALinkage.of(data, new EuclideanDistance()));
		HierarchicalClustering clusters = HierarchicalClustering.fit(new CompleteLinkage(documents.size(), proximity));
		System.out.println("H " + Arrays.toString(clusters.height()));
		
		if (clusters.height().length > 0) {
    		for (int kk = 1; kk < clusters.height()[clusters.height().length - 1]; kk++) {
//    			if (ssclusters2.height()[kk] == 0.0) {
//    				continue;
//    			}
    			
    			System.out.println("X " + kk + " " + Arrays.toString(clusters.partition((double)kk)));

    			Set<Integer> clusterNumbers = new HashSet<>();
    			for (int rr : clusters.partition((double)kk)) {
    				clusterNumbers.add(rr);
    			}
    			
    			
    			for (int clusterNumber : clusterNumbers) {
    				System.out.println("SCLUSTER " + clusterNumber);
    			
    				for (int n = 0; n < clusters.partition((double)kk).length; n++) {
    					if (clusters.partition((double)kk)[n] == clusterNumber) {
    						System.out.println("\t" + documentsAsArray.get(n));
    					}
    				}
    			}
    		}
		}

	}	
	
	private Set<Pair> compute(List<Set<String>> labelWords, Map<String, Set<String>> wordToLabelMap, Set<Pair> allWords) {
		
		Map<Pair, Integer> wordIndexMap = new HashMap<>();
		Map<Integer, Pair> indexWordMap = new HashMap<>();
		
		for (Pair w : allWords) {
			int index = wordIndexMap.size();
			wordIndexMap.put(w, index);
			indexWordMap.put(index, w);
		}
		
		int[][] matrix = new int[wordIndexMap.size()][wordIndexMap.size()];
		int[][] occmatrix = new int[wordIndexMap.size()][wordIndexMap.size()];
		
		for (Pair w1 : allWords) {
			for (Pair w2 : allWords) {
				for (Set<String> words1 : labelWords) {
					if (words1.containsAll(w1.words) && words1.containsAll(w2.words)) {
						int i1 = wordIndexMap.get(w1);
						int i2 = wordIndexMap.get(w2);
							
						matrix[i1][i2]++;
					}
				}				
			}
		}
		
		Set<Pair> res = new TreeSet<>();
		
		for (int i = 0; i < matrix.length; i++) {
			for (int j = i + 1; j < matrix.length; j++) {
				if (matrix[i][j] == 0) {
					Pair w1 = indexWordMap.get(i);
					Pair w2 = indexWordMap.get(j);

					Set<String> allUris1 = new HashSet<>();
					for (String s : w1.words) {
						allUris1.addAll(wordToLabelMap.get(s));
					}
					
					Set<String> allUris2 = new HashSet<>();
					for (String s : w2.words) {
						allUris2.addAll(wordToLabelMap.get(s));
					}
//						
					Set<String> allUris = new HashSet<>();
					allUris.addAll(allUris1);
					allUris.addAll(allUris2);
					
					occmatrix[i][j] = allUris.size();

					if (allUris1.size() > 0.1*labelWords.size() && allUris2.size() > 0.1*labelWords.size()) {
	//					System.out.println(w1 + " --- " + w2 + " " + matrix[i][j] + " | " + occmatrix[i][j]);
						
						List<String> nw = new ArrayList<>();
					    nw.addAll(w1.words);
					    nw.addAll(w2.words);
						res.add(new Pair(nw, occmatrix[i][j]));
					}
				}
				
			}
		}
		
		return res;
	}
	
	class Pair implements Comparable<Pair> {
		List<String> words;
		int size;
		
		Pair(List<String> words, int size) {
			this.words =  words;
			this.size = size;
		}
		
		public int hashCode() {
			return words.hashCode();
		}
		
		public boolean equals(Object obj) {
			if (!(obj instanceof Pair)) {
				return false;
			}
			
			Pair p = (Pair)obj;
			if (words.size() != p.words.size()) {
				return false;
			}
			
			for (int i = 0; i < words.size(); i++) {
				if (!words.get(i).equals(p.words.get(i))) {
					return false;
				}
			}
			
			return true;
		}
		
		public String toString() {
			String s = "";
			for (int i = 0; i < words.size(); i++) {
				if (i > 0) {
					s += " --- ";
				}
				s += words.get(i);
			}
			s += " > " + size;
			
			return s; 
		}

		@Override
		public int compareTo(Pair o) {
			if (size < o.size) {
				return 1;
			} else if (size > o.size) {
				return -1;
			} else {
				return words.toString().compareTo(o.words.toString());
			}
		}
	}
	
	
	class ClusterComparator implements Comparator<Set<String>> {
	    @Override
	    public int compare(Set<String> a, Set<String> b) {
	        return Integer.compare(b.size(), a.size());
	    }
	}
	
//	public AnnotatorDocument failExecution(AnnotatorContainer ac) {			
//		AnnotatorDocument adoc = ac.getObject();
//		
//		MappingExecuteState es = adoc.checkExecuteState(ac.getContainerFileSystemConfiguration().getId());
//		if (es != null) {
//			es.setExecuteState(MappingState.EXECUTION_FAILED);
//			es.setExecuteCompletedAt(new Date());
//			es.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
//			annotatorRepository.save(adoc);
//		}
//		
//		return adoc;
//	}
	
	@Override
	public ListPage<AnnotatorDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, page);
	}
	
	@Override
	public ListPage<AnnotatorDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(annotatorRepository.find(userId, dataset, null, database.getId()));
		} else {
			return ListPage.create(annotatorRepository.find(userId, dataset, null, database.getId(), page));
		}
	}
	
	@Override
	public ListPage<AnnotatorDocument> getAll(AnnotatorLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);
	}

	@Override
	public ListPage<AnnotatorDocument> getAllByUser(ObjectId userId, AnnotatorLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<AnnotatorDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, AnnotatorLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(annotatorRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(annotatorRepository.find(userId, dataset, lp, database.getId(), page));
		}	
	}
	
	@Override
	public AnnotatorLookupProperties createLookupProperties() {
		return new AnnotatorLookupProperties();
	}
	
	public void executionResultsToModel(org.apache.jena.query.Dataset ds, UserPrincipal currentUser, List<ObjectId> annotatorIds) throws IOException {
		if (ds == null) {
			return;
		}
		
		if (annotatorIds != null) {
			for (ObjectId id : annotatorIds) {
				AnnotatorContainer ac = this.getContainer(null, new SimpleObjectIdentifier(id));
				if (ac == null) {
					continue;
				}
	
				AnnotatorDocument doc = ac.getObject();
	
				if (currentUser == null) {
					currentUser = userService.getContainer(null, new SimpleObjectIdentifier(doc.getUserId())).asUserPrincipal();
				}
	
				MappingExecuteState es = doc.getExecuteState(fileSystemConfiguration.getId());
		
				if (es.getExecuteState() == MappingState.EXECUTED) {
					if (es.getExecuteShards() != null) {
				        for (int i = 0; i < es.getExecuteShards(); i++) {
				        	File file = folderService.getAnnotatorExecutionTrigFile(currentUser, ac.getEnclosingObject(), doc, es, i);
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
