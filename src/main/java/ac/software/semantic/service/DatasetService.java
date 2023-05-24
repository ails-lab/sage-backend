package ac.software.semantic.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
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
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.ExtendedParameter;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.PublishDocument;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.Template;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.DatasetType;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.constants.MappingType;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.model.constants.SerializationType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.constants.ThesaurusLoadStatus;
import ac.software.semantic.model.state.CreateDistributionState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.CreateDatasetDistributionRequest;
import ac.software.semantic.payload.CreateDistributionNotificationObject;
import ac.software.semantic.payload.DatasetResponse;
import ac.software.semantic.payload.IndexNotificationObject;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.payload.PublishNotificationObject;
import ac.software.semantic.payload.TemplateResponse;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.repository.TemplateRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.ElasticConfigurationRepository;
import ac.software.semantic.repository.FileRepository;
import ac.software.semantic.repository.IndexStructureRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.SchemaService.ClassStructure;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoConstructIterator;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.semaspace.index.Indexer;


@Service
public class DatasetService implements PublishingService {

	Logger logger = LoggerFactory.getLogger(DatasetService.class);
	
	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	TripleStore tripleStore;

	@Autowired
	IndexStructureRepository indexStructureRepository;

	@Autowired
	ElasticConfigurationRepository elasticConfigurationRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	FolderService folderService;

	@Autowired
	private MappingRepository mappingRepository;

	@Autowired
	private TemplateRepository templateRepository;
	
	@Autowired
	private MappingsService mappingsService;
	
	@Autowired
	AnnotatorService annotatorService;

	@Autowired
	private IndexService indexService;

	@Autowired
	private SchemaService schemaService;
	
	@Autowired
	private FileService fileService;

	@Autowired
	private LodViewService lodViewService;
	
	@Autowired
	FileRepository fileRepository;

    @Autowired
    @Qualifier("database")
    private Database database;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;
    
	@Override
	public Class<? extends ObjectContainer> getContainerClass() {
		return ObjectContainer.class;
	}
	
	public class DatasetContainer extends ObjectContainer implements PublishableContainer {
		private ObjectId datasetId;
		
		private TripleStoreConfiguration tripleStoreConfiguration;
		private ElasticConfiguration elasticConfiguration;
		
		private PublishState publishState;
		private IndexState indexState;
		
		private Map<String, MappingDocument> mappingsMap;
		
		public DatasetContainer(UserPrincipal currentUser, ObjectId datasetId) {
			this.datasetId = datasetId;
			this.currentUser = currentUser;
			
			load();
		}
		
		public DatasetContainer(UserPrincipal currentUser, Dataset dataset) {
			this.datasetId = dataset.getId();
			this.currentUser = currentUser;
			
			this.dataset = dataset;
			
			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
				publishState = dataset.checkPublishState(vc.getId());
				if (publishState != null) {
					tripleStoreConfiguration = vc;
					break;
				}
			}
			
			for (ElasticConfiguration ec : elasticConfigurations.values()) {
				indexState = dataset.checkIndexState(ec.getId());
				if (indexState != null) {
					elasticConfiguration = ec;
					break;
				}
			}
		}

		@Override
		protected void load() {
			Optional<Dataset> doc = datasetRepository.findByIdAndUserId(datasetId, new ObjectId(currentUser.getId()));
			
			if (!doc.isPresent()) {
				return ;
			}
			
			dataset = doc.get();

			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
				publishState = dataset.checkPublishState(vc.getId());
				if (publishState != null) {
					tripleStoreConfiguration = vc;
					break;
				}
			}
			
			for (ElasticConfiguration ec : elasticConfigurations.values()) {
				indexState = dataset.checkIndexState(ec.getId());
				if (indexState != null) {
					elasticConfiguration = ec;
					break;
				}
			}			
		}

		@Override
		protected void loadDataset() {
		}
		
		@Override
		public void save(MongoUpdateInterface mui) throws Exception {
			synchronized (saveSyncString()) { 
				load();
			
				mui.update(this);
				
				datasetRepository.save(dataset);
			}
		}
		
		@Override
		public PublishDocument<PublishState> getPublishDocument() {
			return getDataset();
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return getDatasetId();
		}

		public ObjectId getDatasetId() {
			return datasetId;
		}

		public void setDatasetId(ObjectId datasetId) {
			this.datasetId = datasetId;
		}

		public TripleStoreConfiguration getTripleStoreConfiguration() {
			return tripleStoreConfiguration;
		}
		
		public ElasticConfiguration getElasticConfiguration() {
			return elasticConfiguration;
		}
		
		public IndexState getIndexState() {
			return indexState;
		}

		public void addMapping(String name, MappingDocument mapping) {
			if (mappingsMap == null) {
				mappingsMap = new HashMap<>();
			}
			mappingsMap.put(name, mapping);
		}
		
		public MappingDocument getMapping(String name) {
			if (mappingsMap == null) {
				return null;
			}
			return mappingsMap.get(name);
		}
		
		@Override
		public String localSynchronizationString() {
			return (tripleStoreConfiguration != null ? tripleStoreConfiguration.getId() : "") + ":" + getDatasetId();
		}
		
		@Override
		public DatasetResponse asResponse() {
			
			Template template = null;
			if (getDataset().getTemplateId() != null) {
				template = templateRepository.findById(dataset.getTemplateId()).get();
			}
	
			ThesaurusLoadStatus st = null;
			if (dataset.getTypeUri().contains(SEMAVocabulary.ThesaurusCollection.toString())) {
				try {
					st = annotatorService.isLoaded(dataset);
		    	} catch (Exception ex) {
		    		ex.printStackTrace();
		    		st = ThesaurusLoadStatus.UNKNOWN;
		    	}
			}
	
			return modelMapper.dataset2DatasetResponse(dataset, template, st);
			
		}
		
		@Override 
		public void publish() throws Exception {
//			TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();
//			
//			tripleStore.publish(vc, this);
			throw new Exception("Method not implemented");
		}

		@Override 
		public void unpublish() throws Exception {
//			TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();
//			
//			tripleStore.publish(vc, this);
			throw new Exception("Method not implemented");
		}
		
		public boolean isIndexed() {
			ProcessStateContainer psc = dataset.getCurrentIndexState(elasticConfigurations.values());
			if (psc != null) {
				IndexState is = (IndexState)psc.getProcessState();
				if (is.getIndexState() == IndexingState.INDEXED) {
					return true;
				}
			} 
				
			return false;
		}
		
		@Override
		public TaskType getPublishTask() {
			return TaskType.DATASET_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return TaskType.DATASET_UNPUBLISH;
		}

		@Override
		public TaskType getRepublishTask() {
			return TaskType.DATASET_REPUBLISH;
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}	
		
		@Override
		public String syncString() {
			return ("SYNC:" + containerString() + ":" + datasetId).intern();
		}

		@Override
		public String saveSyncString() {
			return ("SYNC:SAVE:" + containerString() + ":" + datasetId).intern();
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
		return ("SYNC:" + DatasetContainer.class.getName() + ":" + id).intern();
	}


	public DatasetContainer getInDatasetContainer(ObjectContainer oc) {
		if (oc instanceof DatasetContainer) {
			return (DatasetContainer)oc;
		} else {
			return new DatasetContainer(oc.getCurrentUser(), oc.getDataset());
		}
	}
	
//   public Dataset createDataset(UserPrincipal currentUser, String name, String identifier, boolean publik, TripleStoreConfiguration vc, DatasetScope scope, DatasetType type, String typeUri, String asProperty, List<ResourceOption> links, TemplateResponse template) {
	public Dataset createDataset(UserPrincipal currentUser, String name, String identifier, boolean publik, DatasetScope scope, DatasetType type, String typeUri, String asProperty, List<ResourceOption> links, TemplateResponse template) {	
		ObjectId templateId = null; 
		List<ParameterBinding> templateBindings = null;
		
		if (template != null) {
			templateId = new ObjectId(template.getId());
			
			if (template.getParameters() != null && template.getParameters().size() > 0) {
				templateBindings = new ArrayList<>();
				for (ExtendedParameter pb : template.getParameters()) {
					templateBindings.add(new ParameterBinding(pb.getName(), pb.getValue()));
				}
			}
		}
		
//		return createDataset(currentUser, name, identifier, publik, vc, scope, type, typeUri, asProperty, links, templateId, templateBindings);
		return createDataset(currentUser, name, identifier, publik, scope, type, typeUri, asProperty, links, templateId, templateBindings);
	}
	
//	public Dataset createDataset(UserPrincipal currentUser, String name, String identifier, boolean publik, TripleStoreConfiguration vc, DatasetScope scope, DatasetType type, String typeUri, String asProperty, List<ResourceOption> links, ObjectId templateId, List<ParameterBinding> templateBindings) {
	public Dataset createDataset(UserPrincipal currentUser, String name, String identifier, boolean publik, DatasetScope scope, DatasetType type, String typeUri, String asProperty, List<ResourceOption> links, ObjectId templateId, List<ParameterBinding> templateBindings) {
		
		String uuid = UUID.randomUUID().toString();
		
		List<String> typeUris = new ArrayList<>();
		if (typeUri != null) {
			typeUris.add(typeUri);
			
			for (Resource r : SEMAVocabulary.getSuperCollections(typeUri)) {
				String tr = r.toString();
				if (!typeUris.contains(tr)) {
					typeUris.add(tr);
				}
			}
		}
//		typeUris.add(classUri);
		
		Dataset ds = new Dataset();
		ds.setUserId(new ObjectId(currentUser.getId()));
		ds.setDatabaseId(database.getId());
		ds.setUuid(uuid);

		ds.setUpdatedAt(new Date());
		ds.setName(name);
//		if (identifier != null && identifier.trim().length() > 0) {
			ds.setIdentifier(identifier);
//		}
		ds.setScope(scope);
		ds.setDatasetType(type);
		ds.setTypeUri(typeUris);
		ds.setAsProperty(asProperty);
		ds.setPublik(publik);
//		ds.setTripleStoreId(vc.getId());
		
//		ds.setImportType(importType);
		if (templateId != null) {
			ds.setTemplateId(templateId);
			ds.setBinding(templateBindings);
		}

		if (links != null && links.size() > 0 ) {
			ds.setLinks(links);
		}
		
		return datasetRepository.save(ds);
	}
	
	
	
//	public Dataset updateDataset(UserPrincipal currentUser, ObjectId id, String name, String identifier, boolean publik, TripleStoreConfiguration vc, DatasetScope scope, DatasetType type, String typeUri, String asProperty, List<ResourceOption> links) {
	public Dataset updateDataset(UserPrincipal currentUser, ObjectId id, String name, String identifier, boolean publik, DatasetScope scope, DatasetType type, String typeUri, String asProperty, List<ResourceOption> links) {
		
		Optional<Dataset> dsOpt = datasetRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId()));
		
		if (!dsOpt.isPresent()) {
			return null;
		}
		
		List<String> typeUris = new ArrayList<>();
		if (typeUri != null) {
			typeUris.add(typeUri);
			
			for (Resource r : SEMAVocabulary.getSuperCollections(typeUri)) {
				String tr = r.toString();
				if (!typeUris.contains(tr)) {
					typeUris.add(tr);
				}
			}
		}
//		typeUris.add(classUri);
		
		Dataset ds = dsOpt.get();
		ds.setUpdatedAt(new Date());
		ds.setName(name);
		ds.setIdentifier(identifier);
		ds.setScope(scope);
		ds.setDatasetType(type);
		ds.setTypeUri(typeUris);
		ds.setAsProperty(asProperty);
		ds.setPublik(publik);
//		ds.setTripleStoreId(vc.getId());
		
		ds.setLinks(links);

		return datasetRepository.save(ds);
	}
	
	public boolean deleteDataset(UserPrincipal currentUser, String id) throws IOException {

		//TODO: should delete catalog members
		List<MappingDocument> datasetMappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		for (MappingDocument map : datasetMappings) {
			mappingsService.deleteMapping(currentUser, map.getId().toString());
		}

		Long r = datasetRepository.deleteByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		for (Dataset ds : datasetRepository.findByDatasets(new ObjectId(id))) {
			ds.getDatasets().remove(new ObjectId(id));
			datasetRepository.save(ds);
		}

		if (r > 0) {
			return true;
		} else {
			return false;
		}
	}
	
	public List<DatasetResponse> getDatasets(UserPrincipal currentUser, DatasetScope scope, DatasetType type) {
		
		List<Dataset> datasets;
		if (scope != null) {
			datasets = datasetRepository.findByUserIdAndScopeAndTypeAndDatabaseId(new ObjectId(currentUser.getId()), scope, type, database.getId());
			//legacy
			datasets.addAll(datasetRepository.findByUserIdAndTypeAndDatabaseId(new ObjectId(currentUser.getId()), scope.toString().toLowerCase() + "-" + type.toString().toLowerCase(), database.getId()));
		} else {
			datasets = datasetRepository.findByUserIdAndTypeAndDatabaseId(new ObjectId(currentUser.getId()), type, database.getId());
			//legacy
			datasets.addAll(datasetRepository.findByUserIdAndTypeAndDatabaseId(new ObjectId(currentUser.getId()), "collection-" + type.toString().toLowerCase(), database.getId()));
			datasets.addAll(datasetRepository.findByUserIdAndTypeAndDatabaseId(new ObjectId(currentUser.getId()), "catalog-" + type.toString().toLowerCase(), database.getId()));
		}
		
		List<DatasetResponse> res = new ArrayList<>();
		
		for (Dataset ds : datasets) {
			Template t = null;
			if (ds.getTemplateId() != null) {
				t = templateRepository.findById(ds.getTemplateId()).get();
			}
				
			res.add(modelMapper.dataset2DatasetResponse(ds, t));
		}
        
        return res;
		
//		String sparql =
//        "CONSTRUCT { ?catalog <http://www.w3.org/2000/01/rdf-schema#label> ?name }" +
//          " WHERE { " +
//             "GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//                "?catalog a <http://www.w3.org/ns/dcat#Dataset> ; " +
//                    "<http://www.w3.org/2000/01/rdf-schema#label> ?name . } " +
//             "GRAPH <" + SEMAVocabulary.accessGraph + "> { " +                        
//                "?group <" + SACCVocabulary.dataset + "> ?catalog ; " +
//                    "<" + SACCVocabulary.member + "> <" +  SEMAVocabulary.getUser(currentUser.getVirtuosoId()) + "> . } }";
//
//		QueryExecution qe = QueryExecutionFactory.sparqlService(virtuoso.endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ));
//		Model model = qe.execConstruct();
//		
//		Writer sw = new StringWriter();
//		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//
////		System.out.println(sw);
//
//        return sw.toString();
		
		
	}
	
//	public Optional<DatasetResponse> getDataset(UserPrincipal currentUser, String id) {
//		
//		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		if (datasetOpt.isPresent()) {
//			Template template = null;
//			Dataset dataset = datasetOpt.get();
//			if (dataset.getTemplateId() != null) {
//				template = templateRepository.findById(dataset.getTemplateId()).get();
//			}
//			
//			return Optional.of(modelMapper.dataset2DatasetResponse(dataset, template));
//		} else {
//			return Optional.empty();
//		}
//        
////		String sparql =
////        "CONSTRUCT { <" + uri + "> <http://www.w3.org/2000/01/rdf-schema#label> ?name }" +
////          " WHERE { " +
////             "GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
////                "<" + uri + "> a <http://www.w3.org/ns/dcat#Dataset> ; " +
////                    "<http://www.w3.org/2000/01/rdf-schema#label> ?name . } " +
////             "GRAPH <" + SEMAVocabulary.accessGraph + "> { " +                        
////                "<" + SEMAVocabulary.getGroup(currentUser.getVirtuosoId()) + "> <" + SACCVocabulary.dataset + "> <" + uri + "> ; " +
////                    "<" + SACCVocabulary.member + "> <" +  SEMAVocabulary.getUser(currentUser.getVirtuosoId()) + "> . } }";
////
////		QueryExecution qe = QueryExecutionFactory.sparqlService(virtuoso.endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ));
////		Model model = qe.execConstruct();
////		
////		Writer sw = new StringWriter();
////		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
////
////        return sw.toString();
//	}
	
	public Optional<Dataset> getDataset(UserPrincipal currentUser, String id) {
		return datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
	}
	
	public List<MappingDocument> getMappings(UserPrincipal currentUser, ObjectId id) {
		return mappingRepository.findByDatasetIdAndUserId(id, new ObjectId(currentUser.getId()));
	}
	
	public boolean insert(UserPrincipal currentUser, String id, String toId) {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(toId), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		dataset.addDataset(new ObjectId(id));
		
		datasetRepository.save(dataset);
		
		return true;
	}
	
	public boolean remove(UserPrincipal currentUser, String id, String fromId) {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(fromId), new ObjectId(currentUser.getId()));

		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		dataset.removeDataset(new ObjectId(id));
		
		datasetRepository.save(dataset);
		
		return true;
	}
	

	@Async("indexExecutor")
	public ListenableFuture<Date> index(TaskDescription tdescr,  WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		DatasetContainer dc = (DatasetContainer)tdescr.getContainer();
		Dataset dataset = dc.getDataset();
		TripleStoreConfiguration vc = dc.getTripleStoreConfiguration();
		
		IndexStructure idxStruct = (IndexStructure)tdescr.getProperties().get(ServiceProperties.INDEX_STRUCTURE);
		ElasticConfiguration ec = idxStruct.bind(elasticConfigurations);
		
		IndexState is = dataset.getIndexState(ec.getId());

		try {
			is.startDo(pm);
			is.setIndexStructureId(idxStruct.getId());

	        datasetRepository.save(dataset);
	        
			pm.sendMessage(new IndexNotificationObject(is.getIndexState(), dc));
	        
	        logger.info("Indexing of " + dataset.getId() + " started");
	        
			idxStruct.createIndex(database, dataset); //should be removed;
			
	        indexService.index(dataset, ec, vc, idxStruct);
	        
	        logger.info("Indexing of " + dataset.getId() + " completed");
	        
	        pm.complete();

	        is.completeDo(pm);
	        is.setPublish(dc.getPublishState());
	        
	        datasetRepository.save(dataset);
			
			pm.sendMessage(new IndexNotificationObject(is.getIndexState(), dc));

			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
			is.failDo(pm);

			datasetRepository.save(dataset);
			
			pm.sendMessage(new IndexNotificationObject(is.getIndexState(), dc));
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	@Async("indexExecutor")
	public ListenableFuture<Date> unindex(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		DatasetContainer dc = (DatasetContainer)tdescr.getContainer();
		Dataset dataset = dc.getDataset();

		ElasticConfiguration ec = dc.getElasticConfiguration();
		IndexState is = dc.getIndexState();
		
		try {
			is.startUndo(pm);

			datasetRepository.save(dataset);
	
			pm.sendMessage(new IndexNotificationObject(is.getIndexState(), dc));
			
			logger.info("Unindexing of " + dataset.getId() + " started");
			
			indexService.unindex(dataset, ec, resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString());
			
			logger.info("Unindexing of " + dataset.getId() + " completed");
			
			dataset.removeIndexState(is);
			
			datasetRepository.save(dataset);
			
			pm.complete();
			pm.sendMessage(new IndexNotificationObject(IndexingState.NOT_INDEXED, dc));
			
			return new AsyncResult<>(pm.getCompletedAt());
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete();
			
			is.failUndo(pm);
			datasetRepository.save(dataset);
			
			pm.sendMessage(new IndexNotificationObject(is.getIndexState(), dc));
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
			
		}
	}

	
	public boolean indexDatasetOld(UserPrincipal currentUser, String datasetId) throws IOException {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();

		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());

		if (psv == null) {
			return false;
		}

		TripleStoreConfiguration vc = psv.getTripleStoreConfiguration();

		ProcessStateContainer ise = dataset.getCurrentIndexState(elasticConfigurations.values());

		if (ise == null) {
			return false;
		}
		
		ElasticConfiguration ec = ise.getElasticConfiguration();
		
		IndexState is = dataset.getIndexState(ec.getId());
		
//		if (dataset.getType().startsWith("vocabulary-dataset")) {
		if (dataset.getScope() == DatasetScope.VOCABULARY && dataset.getDatasetType() == DatasetType.DATASET) {
			
			is.setIndexState(IndexingState.INDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);

			Indexer ic = new Indexer(vc.getSparqlEndpoint());
			ic.indexVocabulary(ec.getIndexIp(), ec.getIndexVocabularyName(), resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString(), legacyUris);
			
			logger.info("Indexing of " + dataset.getId() + " completed");
			
			is.setIndexState(IndexingState.INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);
			
			return true;
//		} else if (dataset.getType().startsWith("collection-dataset")) {
		} else if (dataset.getScope() == DatasetScope.COLLECTION && dataset.getDatasetType() == DatasetType.DATASET) {
            
            is.setIndexState(IndexingState.INDEXING);
            is.setIndexStartedAt(new Date(System.currentTimeMillis()));
            datasetRepository.save(dataset);

            Indexer ic = new Indexer(vc.getSparqlEndpoint());
            ic.indexCollection(ec.getIndexIp(), ec.getIndexDataName(), resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString(), null);
            
            logger.info("Indexing of " + dataset.getId() + " completed");
            
            is.setIndexState(IndexingState.INDEXED);
            is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
            datasetRepository.save(dataset);
            
            return true;
            
        }
		
		return false;
	}
	
	public boolean unindexDatasetOld(UserPrincipal currentUser, String datasetId) throws IOException {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		
		ProcessStateContainer psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());

		if (psv == null) {
			return false;
		}

		TripleStoreConfiguration vc = psv.getTripleStoreConfiguration();

		ProcessStateContainer ise = dataset.getCurrentIndexState(elasticConfigurations.values());

		if (ise == null) {
			return false;
		}
		
		ElasticConfiguration ec = ise.getElasticConfiguration();
		
		IndexState is = dataset.getIndexState(ec.getId());
		
//		if (dataset.getType().startsWith("vocabulary-dataset")) {
		if (dataset.getScope() == DatasetScope.VOCABULARY && dataset.getDatasetType() == DatasetType.DATASET) {
			
			is.setIndexState(IndexingState.UNINDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);

			Indexer ic = new Indexer(vc.getSparqlEndpoint());
			ic.unindexVocabulary(ec.getIndexIp(), ec.getIndexVocabularyName(), resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString(), legacyUris);
			
			logger.info("Unindexing of " + dataset.getId() + " completed");
			
			is.setIndexState(IndexingState.NOT_INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);
			
			return true;
			
//		} else if (dataset.getType().startsWith("collection-dataset")) {
		} else if (dataset.getScope() == DatasetScope.COLLECTION && dataset.getDatasetType() == DatasetType.DATASET) {
			
			is.setIndexState(IndexingState.UNINDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);

			Indexer ic = new Indexer(vc.getSparqlEndpoint());
			ic.unindexCollection(ec.getIndexIp(), ec.getIndexDataName(), resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString());
			
			logger.info("Unindexing of " + dataset.getId() + " completed");
			
			is.setIndexState(IndexingState.NOT_INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);
			
			return true;
			
		}
		
		return false;
	}
	

//	public DatasetMessage checkPublishVocabulary(UserPrincipal currentUser, String virtuoso, String id) throws Exception {
//		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		if (!doc.isPresent()) {
//			return DatasetMessage.DATASET_DOES_NOT_EXIST;
//		}
//		
//		Dataset dataset = doc.get();
//		
//		if (!dataset.getType().equals("vocabulary-dataset")) {
//			return DatasetMessage.OK;
//		}
//		
//		List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
////		List<FileDocument> files = fileRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		String mf = getMappingsFolder(currentUser);
//    	
//		int count = 0;
//		String identifier = null;
//		
//    	try (RDFJenaSource source  = new RDFJenaSource()) {
//    	
//			for (MappingDocument map : mappings) {
//	    		boolean hi = !map.getParameters().isEmpty();
//	
//				for (MappingInstance mi : map.getInstances()) {
//	    			MappingExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());
//	
//					for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
//						String fileName = map.getUuid() + (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig";
//					
//		    			if (dataset.getType().equals("annotation-dataset")) {
//						} else {
//	    	    			
//			    			if (map.getType() == MappingType.HEADER) {
//			    				System.out.println("LOADING " + mf + fileName);
//			    				((RDFJenaConnection)source.getConnection()).load(new File(mf + File.separator + fileName), SEMAVocabulary.contentGraph, Lang.TTL);
//					    	}
//						}
//	    			}
//				}
//			}
//			
//	//		String labelSparql =  "SELECT ?label GRAPH <" + SEMAVocabulary.contentGraph + "> " + 
//	//		                 "WHERE { <" + SEMAVocabulary.getDataset(id) + "> <" + RDFSVocabulary.label + "> ?label";
//	
//			String getIdentifier = "SELECT ?identifier FROM <" + SEMAVocabulary.contentGraph + "> " + 
//								 "WHERE { <" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <http://purl.org/dc/elements/1.1/identifier> ?identifier . }";
//			
//			try (IteratorSet iter = source.getConnection().executeQuery(getIdentifier)) {
//				while (iter.hasNext()) {
//					System.out.println(count);
//					count++;
//					identifier = iter.next().get(0).toString();
//				}
//			}
//    	}		
//    	
//		if (count == 0) {
//			return DatasetMessage.NO_IDENTIFIER;
//		} else if (count > 1) {
//			return DatasetMessage.MULTIPLE_IDENTIFIERS;
//		}
//		
//		String checkIdentifier = "ASK FROM <" + SEMAVocabulary.contentGraph + "> " + 
//				 "WHERE { ?p a <http://www.w3.org/ns/dcat#Dataset> . " + 
//				        " ?p <http://purl.org/dc/elements/1.1/identifier> <" + identifier + "> . }";
//
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(checkIdentifier))) {
//			
//			if (qe.execAsk()) {
//				return DatasetMessage.IDENTIFIER_ALREADY_EXISTS;
//			}
//		}
//		
//		return DatasetMessage.OK;
//	}

	public DatasetContainer getContainer(UserPrincipal currentUser, String datasetId) {
		DatasetContainer dc = new DatasetContainer(currentUser, new ObjectId(datasetId));
		if (dc.getDataset() == null) {
			return null;
		} else {
			return dc;
		}
	}

	@Override
	public DatasetContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		DatasetContainer dc = new DatasetContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());
		if (dc.getDataset() == null) {
			return null;
		} else {
			return dc;
		}
	}
	
//	public DatasetContainer getPublishedContainer(UserPrincipal currentUser, String datasetId) {
//		DatasetContainer dc = new DatasetContainer(currentUser, new ObjectId(datasetId));
//		if (dc.getDataset() == null || dc.getTripleStore() == null) {
//			return null;
//		} else {
//			return dc;
//		}
//	}
	
//	public DatasetContainer getUnpublishedContainer(UserPrincipal currentUser, String datasetId, TripleStoreConfiguration vc) {
//		DatasetContainer dc = new DatasetContainer(currentUser, new ObjectId(datasetId), vc);
//		if (dc.getDataset() == null) {
//			return null;
//		} else {
//			return dc;
//		}
//	}
	
//	public DatasetContainer getUnpublishedContainer(UserPrincipal currentUser, String datasetId) {
//		DatasetContainer dc = new DatasetContainer(currentUser, new ObjectId(datasetId));
//		if (dc.getDataset() == null) {
//			return null;
//		} else {
//			return dc;
//		}
//	}	
	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
//		taskService.taskStarted(tdescr);
		return new AsyncResult<>(publish((DatasetContainer)tdescr.getContainer(), tdescr.getProperties(), (GenericMonitor)tdescr.getMonitor(), wsService));
	}	

	//  publicc:  0 : private
	//            1 : public
	//           -1 : current publication state
	// inconsitencies iff accessed concurrently for the same dataset
	// if applicationEventPublisher will generate sse
	private Date publish(DatasetContainer dc, Properties props, GenericMonitor pm, WebSocketService wsService) throws TaskFailureException {
		
		int publicc = (int)props.get(ServiceProperties.PUBLISH_MODE);
		boolean metadata = (boolean)props.get(ServiceProperties.PUBLISH_METADATA);
		boolean content = (boolean)props.get(ServiceProperties.PUBLISH_CONTENT);
		boolean onlyUnpublished = (boolean)props.get(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT);

		Dataset dataset = dc.getDataset();
		UserPrincipal currentUser = dc.getCurrentUser();
		ObjectId id = dc.getDatasetId();
//		TripleStoreConfiguration vc = dc.getTripleStore();
		TripleStoreConfiguration vc = (TripleStoreConfiguration)props.get(ServiceProperties.TRIPLE_STORE);
		
		PublishState ps = dataset.getPublishState(vc.getId());		
		
		boolean isPublic;
		if (publicc == ServiceProperties.PUBLISH_MODE_CURRENT) {
			isPublic = (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC)  ? true : false;
		} else {
			isPublic = publicc == ServiceProperties.PUBLISH_MODE_PUBLIC ? true : false;
		}

		List<MappingDocument> mappingsToPublish = new ArrayList<>();
		List<MappingInstance> mappingsInstancesToPublish = new ArrayList<>();
		List<FileDocument> filesToPublish = new ArrayList<>();

		try {
			clearDistribution(dc);

			ps.startDo(pm);
			
			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), dc));
			
			tripleStore.clearDistributionToMetadata(dataset, vc);

			List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(id, new ObjectId(currentUser.getId()));
			List<FileDocument> files = fileRepository.findByDatasetIdAndFileSystemConfigurationIdAndUserId(id, fileSystemConfiguration.getId(), new ObjectId(currentUser.getId()));
			
			for (MappingDocument map : mappings) {
				MappingType type = map.getType();
				
				if ((metadata && type == MappingType.HEADER) || (content && type == MappingType.CONTENT)) {

					for (MappingInstance mi : map.getInstances()) {
						PublishState pstate = mi.getPublishState(vc.getId());
						ExecuteState estate = mi.getExecuteState(fileSystemConfiguration.getId());
						
						if (estate.getExecuteState() == MappingState.EXECUTED) {
							if ((onlyUnpublished && pstate.getPublishState() != DatasetState.PUBLISHED) || !onlyUnpublished) { 
								pstate.setPublishStartedAt(new Date(System.currentTimeMillis()));
								pstate.setPublishState(DatasetState.PUBLISHING);
								
								mappingsToPublish.add(map);
								mappingsInstancesToPublish.add(mi);								
								
								mappingRepository.save(map);
							}
						}
					}
				}
			}
			
			if (content) {
				for (FileDocument fd : files) {
					PublishState pstate = fd.getPublishState(vc.getId());
					
					if ((onlyUnpublished && pstate.getPublishState() != DatasetState.PUBLISHED) || !onlyUnpublished) {
						pstate.setPublishStartedAt(new Date(System.currentTimeMillis()));
						pstate.setPublishState(DatasetState.PUBLISHING);
	
						filesToPublish.add(fd);
						
						fileRepository.save(fd);
					}
				}
			}
			
			datasetRepository.save(dataset);
			
			// Allow successful publication even with schema computation failure.
			// TODO: Think further how to handle this 
			boolean schemaComputed = tripleStore.publish(currentUser, vc, dataset, mappingsToPublish, filesToPublish, metadata);
			
//			if (!dataset.getType().equals("annotation-dataset")) {
			if (!(dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET)) {				
				tripleStore.addDatasetToAccessGraph(currentUser, vc, dataset.getUuid(), isPublic);
			}
			
			// remove from inknowledge
			if (dataset.getTypeUri().contains(SEMAVocabulary.ThesaurusCollection.toString())) {
				annotatorService.unload(dataset);
			}
			
			ps = dataset.getPublishState(vc.getId());
	    	
			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			if (isPublic) {
				ps.setPublishState(DatasetState.PUBLISHED_PUBLIC);
			} else {
				ps.setPublishState(DatasetState.PUBLISHED_PRIVATE);
			}
			
			for (MappingDocument map : mappingsToPublish) {
				for (MappingInstance mi : map.getInstances()) {
					if (mappingsInstancesToPublish.contains(mi)) {
						MappingExecuteState mies = mi.getExecuteState(fileSystemConfiguration.getId());
						MappingPublishState mips = mi.getPublishState(vc.getId());
						
						mips.setPublishCompletedAt(new Date(System.currentTimeMillis()));
						mips.setPublishState(DatasetState.PUBLISHED);
						mips.setExecute(mies);
					}
				}
				
				mappingRepository.save(map);
			}
			
			for (FileDocument fd : filesToPublish) {
				FilePublishState fdps = fd.getPublishState(vc.getId());
				
				fdps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
				fdps.setPublishState(DatasetState.PUBLISHED);
				fdps.setExecute(fd.getExecute());

				fileRepository.save(fd);
			}

			// get last update date
//			Date date = null;
//			for (MappingDocument map : mappingsToPublish) {
//				for (MappingInstance mi : map.getInstances()) {
//					if (mappingsInstancesToPublish.contains(mi)) {
//						MappingPublishState pstate = mi.checkPublishState(virtuosoId);
//	
//						if (pstate != null && pstate.getPublishState() == DatasetState.PUBLISHED) {
//							Date newDate = pstate.getExecute().getExecuteStartedAt();
//							if (date == null || newDate.after(date)) {
//								date = newDate;
//							}
//						}
//					}
//				}
//			}
//			
//			for (FileDocument fd : filesToPublish) {
//				FilePublishState pstate = fd.checkPublishState(virtuosoId);
//
//				if (pstate != null && pstate.getPublishState() == DatasetState.PUBLISHED) {
//					Date newDate = pstate.getExecute().getExecuteStartedAt();
//					if (date == null || newDate.after(date)) {
//						date = newDate;
//					}
//				}
//			}
//			
//			tripleStore.setPublishLastModified(currentUser, virtuoso, dataset, date);
			tripleStore.setPublishLastModified(currentUser, vc, dataset, ps.getPublishCompletedAt());
			
			if (dataset.checkFirstPublishState(vc.getId()) == null) {
				dataset.setFirstPublishState(ps, vc.getId());
			}
			
			datasetRepository.save(dataset);
			
			logger.info("Publication of " + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + " completed.");
			
			//success
//			if (applicationEventPublisher != null) {
//				SSEController.send("dataset", applicationEventPublisher, this, new NotificationObject("publish", ps.getPublishState().toString(), dc, ps.getPublishStartedAt(), ps.getPublishCompletedAt()));
//			}
			
			pm.complete();
			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), dc));

			lodViewService.updateLodView();

			return pm.getCompletedAt();
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
			ps.failDo(pm);
			
			for (MappingDocument map : mappingsToPublish) {
				for (MappingInstance mi : map.getInstances()) {
					PublishState state = mi.getPublishState(vc.getId());

					mi.removePublishState(state);
				}
				
				mappingRepository.save(map);
			}
			
			for (FileDocument fd : filesToPublish) {
				PublishState state = fd.getPublishState(vc.getId());

				fd.removePublishState(state);
				
				fileRepository.save(fd);
			}

			datasetRepository.save(dataset);
			
			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), dc));

			throw new TaskFailureException(ex, pm.getCompletedAt());
		}				
	}

	
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return new AsyncResult<>(unpublish((DatasetContainer)tdescr.getContainer(), tdescr.getProperties(), (GenericMonitor)tdescr.getMonitor(), wsService));
	}

	// Metadata but not content should be unpublished only when republishing metadata
	private Date unpublish(DatasetContainer dc, Properties props, GenericMonitor pm, WebSocketService wsService) throws TaskFailureException {

		boolean metadata = (boolean)props.get(ServiceProperties.PUBLISH_METADATA);
		boolean content = (boolean)props.get(ServiceProperties.PUBLISH_CONTENT);

		UserPrincipal currentUser = dc.getCurrentUser();
		Dataset dataset = dc.getDataset();
		ObjectId id = dc.getDatasetId();
		TripleStoreConfiguration vc = dc.getTripleStoreConfiguration();

		List<MappingDocument> mappingsToUnpublish = new ArrayList<>();
		List<FileDocument> filesToUnpublish = new ArrayList<>();

		PublishState ps = null;
		try {
			ps = dc.getPublishState();

			clearDistribution(dc); // this should be before startUndo

			ps.startUndo(pm);
			
			pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), dc));
	
			tripleStore.clearDistributionToMetadata(dataset, vc);

			List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(id, new ObjectId(currentUser.getId()));
			List<FileDocument> files = fileRepository.findByDatasetIdAndFileSystemConfigurationIdAndUserId(id, fileSystemConfiguration.getId(), new ObjectId(currentUser.getId()));
			
			for (MappingDocument map : mappings) {
				MappingType type = map.getType();
				
				if ((metadata && type == MappingType.HEADER) || (content && type == MappingType.CONTENT)) {
					for (MappingInstance mi : map.getInstances()) {
						PublishState state = mi.getPublishState(vc.getId());
	
						state.setPublishState(DatasetState.UNPUBLISHING);
						
						mappingsToUnpublish.add(map);
					}
					mappingRepository.save(map);
				}
			}
			
			if (content) {
				for (FileDocument fd : files) {
					PublishState state = fd.getPublishState(vc.getId());
	
					state.setPublishState(DatasetState.UNPUBLISHING);
					
					filesToUnpublish.add(fd);
	
					fileRepository.save(fd);
				}
			}
			
			datasetRepository.save(dataset);
        
			tripleStore.unpublish(currentUser, vc, dataset, mappingsToUnpublish, filesToUnpublish, metadata, content);
			
			if (content && metadata) {
//				if (!dataset.getType().equals("annotation-dataset")) {
				if (!(dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET)) {
					tripleStore.removeDatasetFromAccessGraph(vc, dataset.getUuid().toString());
				}
			}
			
			// remove from inknowledge
			if (dataset.getTypeUri().contains(SEMAVocabulary.ThesaurusCollection.toString())) {
				annotatorService.unload(dataset);
			}
			
			if (content && metadata && ps != null) {
				dataset.removePublishState(ps);
			}
			
			for (MappingDocument map : mappingsToUnpublish) {
				for (MappingInstance mi : map.getInstances()) {
					MappingPublishState state = mi.getPublishState(vc.getId());
	
					mi.removePublishState(state);
					
					// remove published execution if newer execution exists. problem if unpuplishing from different location than publishing: execution is not available
					MappingExecuteState es = mi.getExecuteState(fileSystemConfiguration.getId());
					MappingExecuteState pes = state.getExecute();
					if (es != null && pes != null && es.getExecuteStartedAt().compareTo(pes.getExecuteStartedAt()) != 0) {
						mappingsService.clearExecution(currentUser, dataset, map, mi, pes);
					}
				}
				mappingRepository.save(map);
			}
			
			for (FileDocument fd : filesToUnpublish) {
				FilePublishState state = fd.getPublishState(vc.getId());
	
				fd.removePublishState(state);
				
				// remove published execution if newer execution exists.
				FileExecuteState es = fd.getExecute();
				FileExecuteState pes = state.getExecute();
				if (es != null && pes != null && es.getExecuteStartedAt().compareTo(pes.getExecuteStartedAt()) != 0) {
					fileService.clearFile(currentUser, dataset, fd, pes);
				}
				
				fileRepository.save(fd);
			}
			
			datasetRepository.save(dataset);
			
			logger.info("Unpublication of " + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + " completed.");
			
			pm.complete();
			pm.sendMessage(new PublishNotificationObject(DatasetState.UNPUBLISHED, dc));

			lodViewService.updateLodView();
			
			return pm.getCompletedAt();
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
			if (ps != null) {
				ps.failUndo(pm);
			}
			
			for (MappingDocument map : mappingsToUnpublish) {
				for (MappingInstance mi : map.getInstances()) {
					PublishState state = mi.getPublishState(vc.getId());

					mi.removePublishState(state);
				}
				
				mappingRepository.save(map);
			}
			
			for (FileDocument fd : filesToUnpublish) {
				PublishState state = fd.getPublishState(vc.getId());

				fd.removePublishState(state);
				
				fileRepository.save(fd);
			}

			datasetRepository.save(dataset);
			
			if (ps != null) {
				pm.sendMessage(new PublishNotificationObject(ps.getPublishState(), dc));
			}
			
//			return false;
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	
//	@Async("publishExecutor")
//	public ListenableFuture<Boolean> republishMetadata(TaskDescription tdescr, DatasetContainer dc, ApplicationEventPublisher applicationEventPublisher) {
////		taskService.taskStarted(tdescr);
//		
//		PublishState ps = dc.publishState;
//		
//		Properties props = new Properties();
//		props.put(ServiceProperties.PUBLISH_METADATA, true);
//		props.put(ServiceProperties.PUBLISH_CONTENT, false);
//		
//		boolean failed = false;
//		if (!unpublishF(dc, props, null)) {
//			failed = true;
//		}
//		if (!failed) {
//			
//			props = new Properties();
//			
//			int isPublic = ServiceProperties.PUBLISH_MODE_CURRENT;
//			if (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC) {
//				isPublic = ServiceProperties.PUBLISH_MODE_PUBLIC;
//			} else if (ps.getPublishState() == DatasetState.PUBLISHED_PRIVATE) {
//				isPublic = ServiceProperties.PUBLISH_MODE_PRIVATE;
//			}
//
//			props.put(ServiceProperties.PUBLISH_MODE, isPublic);
//			props.put(ServiceProperties.PUBLISH_METADATA, true);
//			props.put(ServiceProperties.PUBLISH_CONTENT, false);
//			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);			
//			
//			if (!publishF(dc, props, null)) {
//				failed = true;
//			} 
//		}
//		
//		if (!failed) {
//			SseApplicationEvent.sse(applicationEventPublisher, this, "dataset", new NotificationObject("publish", ps.getPublishState().toString(), dc, ps.getPublishStartedAt(), ps.getPublishCompletedAt()));
//			return new AsyncResult<>(true);
//		} else {
//			SseApplicationEvent.sse(applicationEventPublisher, this, "dataset", new NotificationObject("publish", DatasetState.PUBLISHING_FAILED.toString(), dc, null, null));
//		   return new AsyncResult<>(false);
//		}
//	}
	
	@Async("publishExecutor")
	public ListenableFuture<Date> flipVisibility(TaskDescription tdescr, DatasetContainer dc, WebSocketService wsService) throws TaskFailureException {
//		taskService.taskStarted(tdescr);
		
		UserPrincipal currentUser = dc.getCurrentUser();
		Dataset dataset = dc.getDataset();
		TripleStoreConfiguration vc = dc.getTripleStoreConfiguration();

		PublishState ps = null;

		try {
			ps = dc.getPublishState();

//			if (!dataset.getType().equals("annotation-dataset")) {
			if (!(dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET)) {
				
				if (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC) {
					tripleStore.addDatasetToAccessGraph(currentUser, vc, dataset.getUuid().toString(), false);
					ps.setPublishState(DatasetState.PUBLISHED_PRIVATE);
					
					logger.info("Visibility of " + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + " changed to PRIVATE.");
					
				} else if (ps.getPublishState() == DatasetState.PUBLISHED_PRIVATE) {
					tripleStore.addDatasetToAccessGraph(currentUser, vc, dataset.getUuid().toString(), true);
					ps.setPublishState(DatasetState.PUBLISHED_PUBLIC);
					
					logger.info("Visibility of " + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + " changed to PUBLIC.");
				} else {
					throw new Exception("Dataset visibility error");
				}
				
				datasetRepository.save(dataset);
			}
			
			wsService.send(NotificationChannel.dataset, currentUser, new NotificationObject(NotificationType.publish, ps.getPublishState().toString(), dc, ps.getPublishStartedAt(), ps.getPublishCompletedAt()));
			
			return new AsyncResult<>(new Date());
		} catch (Exception ex) {
			wsService.send(NotificationChannel.dataset, currentUser, new NotificationObject(NotificationType.publish, DatasetState.PUBLISHING_FAILED.toString(), dc, null, null));
//			return new AsyncResult<>(false);
			throw new TaskFailureException(ex, new Date());
		}
		
	}	 
	
	@Async("createDistributionExecutor")
	public ListenableFuture<Date> createDistribution(TaskDescription tdescr, DatasetContainer dc, CreateDatasetDistributionRequest options, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
    	Dataset dataset = dc.getDataset();
		TripleStoreConfiguration vc = dc.getTripleStoreConfiguration();
		
		DatasetCatalog dcg = schemaService.asCatalog(dataset.getUuid());
		String fromClause = schemaService.buildFromClause(dcg);

		Date createStart = new Date(System.currentTimeMillis());

		CreateDistributionState cds = dataset.getCreateDistributionState(fileSystemConfiguration.getId(), vc.getId());

		try {

			logger.info("Creating distribution for " + resourceVocabulary.getDatasetAsResource(dataset.getUuid()));

	    	cds.setCreateDistributionState(MappingState.PREPARING_EXECUTION);
	    	cds.setCreateDistributionStartedAt(createStart);
	    	cds.setCreateDistributionCompletedAt(null);
	    	cds.setSerialization(options.getSerializations());
	    	cds.setCompression(options.getCompress());
	    	cds.clearMessages();
	    	
	    	datasetRepository.save(dataset);

			pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Clearing previous distribution")));

//			Thread.sleep(1000);
			
			clearDistribution(dc.getCurrentUser(), dataset, (PublishState)dataset.getCurrentPublishState(virtuosoConfigurations.values()).getProcessState());

			pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Clearing relevant metadata")));
			
//			Thread.sleep(1000);

			tripleStore.clearDistributionToMetadata(dataset, vc);
			
	    	cds.setCreateDistributionState(MappingState.EXECUTING);
	    	
	    	datasetRepository.save(dataset);
	    	
	    	pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Reading top classes")));
	    	
//	    	Thread.sleep(1000);
	    	
	    	Map<String, ClassStructure> structMap = new HashMap<>();
	    	for (ClassStructure cs : schemaService.readTopClasses(dc.getDataset())) {
	    		structMap.put(cs.getClazz().toString(), cs);
	    	}
	    	
	    	List<ClassStructure> structs = new ArrayList<>();
	    	for (String clazz : options.getClasses()) {
	    		ClassStructure cs = structMap.get(clazz);
	    		if (cs != null) {
	    			structs.add(cs);
	    		}
	    	}
	    	
			File ttlFile = null;
			File ntFile= null;
			FileWriter ttlWriter = null;
			FileWriter ntWriter = null;
	    	if (options.getSerializations().contains(SerializationType.TTL)) {
	    		ttlFile = folderService.createDatasetDistributionFile(dc.getCurrentUser(), dataset, (PublishState)dataset.getCurrentPublishState(virtuosoConfigurations.values()).getProcessState(), SerializationType.TTL);
	    		ttlWriter = new FileWriter(ttlFile);
	    	}
	    	
	    	if (options.getSerializations().contains(SerializationType.NT)) {
	    		ntFile = folderService.createDatasetDistributionFile(dc.getCurrentUser(), dataset, (PublishState)dataset.getCurrentPublishState(virtuosoConfigurations.values()).getProcessState(), SerializationType.NT);
	    		ntWriter = new FileWriter(ntFile);
	    	}
	    	
//		    sorted version but very slow (one request for each resource) and triplestore may fails due to many requests 			
//			try {
//	
//				int csCount = 0;
//		    	for (ClassStructureResponse cs : structs) {
//		    	
//					StringBuffer db = new StringBuffer();
//					
//					db.append("CONSTRUCT { <{@@RESOURCE@@}> ?p1 ?o1 . ");
//					for (int j = 2; j <= cs.getDepth(); j++) {
//						db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
//					}
//					db.append("} " + fromClause + " WHERE { <{@@RESOURCE@@}>  ?p1 ?o1 . ");
//		    		for (int j = 2; j <= cs.getDepth(); j++) {
//						db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
//					}			
//		    		for (int j = 2; j <= cs.getDepth(); j++) {
//						db.append(" } ");
//					}
//					db.append("}");
//					
//					String itemSparql = db.toString();
//		
//					String sparql =
//							"SELECT ?item " + fromClause + " WHERE { " + 
//							" SELECT ?item WHERE { " +
//							"    ?item a <" + cs.getClazz() + "> . " + 
//							" } ORDER BY ?item }";
//					
//	
//					if (ttlWriter != null&& csCount > 0) {
//						ttlWriter.append("\n");
//					}
//					
//					csCount++;
//	
//					int count = 0;
//					
//					System.out.println(QueryFactory.create(sparql));
//					
//					try (VirtuosoSelectIterator qe = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {
//						System.out.println("XX");
//
//						while (qe.hasNext()) {
//							QuerySolution sol = qe.next();
//							
//							Resource item = sol.getResource("item");
//		
//							String iSparql = itemSparql.replaceAll("\\{@@RESOURCE@@\\}", item.toString());
//		
//							if (ttlWriter != null&& count > 0) {
//								ttlWriter.append("\n");
//							}
//								
//							try (QueryExecution iqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), iSparql)) {
//								System.out.println(count);
//								Model model = iqe.execConstruct();
//								model.clearNsPrefixMap();
//							
//								if (ttlWriter != null) {
//									RDFDataMgr.write(ttlWriter, model, Lang.TURTLE);
//								}
//								
//								if (ntWriter != null) {
//									RDFDataMgr.write(ntWriter, model, Lang.NT);
//								}
//								
//								count++;
//							}
//						}
//					}
//				}
//		    	
//		    	if (ttlWriter != null) {
//		    		ttlWriter.flush();
//		    	}
//		    	
//		    	if (ntWriter != null) {
//		    		ntWriter.flush();
//		    	}
//	    	} finally {
//	    		if (ttlWriter != null) {
//	    			ttlWriter.close();
//	    		}
//
//	    		if (ntWriter != null) {
//	    			ntWriter.close();
//	    		}
//	    	}
	    	
			try {
	
				int csCount = 0;
		    	for (ClassStructure cs : structs) {
//		    		logger.info("Exporting " + cs.getClazz());
		    		
		    		pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Reading " + cs.getClazz() + " instances")));
		    		
//		    		Thread.sleep(1000);
		    		
					StringBuffer db = new StringBuffer();
					
					db.append("CONSTRUCT { ?resource ?p1 ?o1 . ");
					for (int j = 2; j <= cs.getDepth(); j++) {
						db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
					}
					db.append("} " + fromClause + " WHERE { ?resource  ?p1 ?o1 . ");
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
					}			
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db.append(" } ");
					}
		    		db.append("VALUES ?resource { {@@RESOURCE@@} }");
					db.append("}");
					
					StringBuffer db2 = new StringBuffer();
					
					db2.append("CONSTRUCT { {@@RESOURCE@@} ?p1 ?o1 . ");
					for (int j = 2; j <= cs.getDepth(); j++) {
						db2.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
					}
					db2.append("} WHERE { {@@RESOURCE@@}  ?p1 ?o1 . ");
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db2.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
					}			
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db2.append(" } ");
					}
					db2.append("}");
					
					String itemsSparql = db.toString();
					String resourceSparql = db2.toString();

					String countSparql =
							" SELECT (COUNT(?item) AS ?count) " + fromClause + " WHERE { " +
							"    ?item a <" + cs.getClazz() + "> . " + 
							" } ";
					
					int totalCount = 0;
					try (VirtuosoSelectIterator qe = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), countSparql)) {
						while (qe.hasNext()) {
							QuerySolution sol = qe.next();
							
							totalCount = sol.get("count").asLiteral().getInt();
						}
					}
					
					String sparql =
							"SELECT ?item " + fromClause + " WHERE { " + 
							" SELECT ?item WHERE { " +
							"    ?item a <" + cs.getClazz() + "> . " + 
							" } ORDER BY ?item }";
					
	
					if (ttlWriter != null && csCount > 0) {
						ttlWriter.append("\n");
					}
					
					csCount++;
	
					int count = 0;
					
//					System.out.println(QueryFactory.create(sparql));
					
					int pageSize = Math.min(1000, VirtuosoSelectIterator.VIRTUOSO_LIMIT / cs.getSize());
					
//					System.out.println("page size " + pageSize);
					
					try (VirtuosoSelectIterator qe = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {

						pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Exporting " + cs.getClazz() + " (" + count + "/" + totalCount + ")")));
						
//						Thread.sleep(1000);

						List<Resource> buffer = new ArrayList<>();
//						int buffCount = 0;
						
						while (qe.hasNext()) {
							if (Thread.currentThread().isInterrupted()) {
								Exception ex = new InterruptedException("The task was interrupted.");
								throw ex;
							}
							
							QuerySolution sol = qe.next();
							
							Resource item = sol.getResource("item");
		
							if (buffer.size() < pageSize) {
								buffer.add(item);
							} else {
								pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Exporting " + cs.getClazz() + " (" + count + "/" + totalCount + ")")));
								
//								Thread.sleep(1000);

//								System.out.println(buffCount++ + " " + count);
								
								StringBuffer stringBuffer = new StringBuffer();
								for (Resource r : buffer) {
									stringBuffer.append("<" + r.toString() + ">" );
								}
								
								String iSparql = itemsSparql.replaceAll("\\{@@RESOURCE@@\\}", stringBuffer.toString());
		
//								if (ttlWriter != null && count > 0) {
//									ttlWriter.append("\n");
//								}
								
								try (QueryExecution iqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), iSparql)) {
									
									Model model = VirtuosoConstructIterator.tryQuery(iqe, iSparql, logger);
//									model.clearNsPrefixMap();
									
									for (Resource r : buffer) {
										String iiSparql = resourceSparql.replaceAll("\\{@@RESOURCE@@\\}", "<" + r.toString() + ">");
										
										try (QueryExecution iiqe = QueryExecutionFactory.create(iiSparql, model)) {
											
											Model imodel = iiqe.execConstruct();
											imodel.clearNsPrefixMap();
											
											if (ttlWriter != null && count > 0) {
												ttlWriter.append("\n");
											}
											
											if (ttlWriter != null) {
												RDFDataMgr.write(ttlWriter, imodel, Lang.TURTLE);
											}
											
											if (ntWriter != null) {
												RDFDataMgr.write(ntWriter, imodel, Lang.NT);
											}
											
											count++;

										}
									}
									
//									if (ttlWriter != null) {
//										RDFDataMgr.write(ttlWriter, model, Lang.TURTLE);
//									}
//									
//									if (ntWriter != null) {
//										RDFDataMgr.write(ntWriter, model, Lang.NT);
//									}
//									
//									count++;
								}
								
								buffer = new ArrayList<>();
								buffer.add(item);
							}
						}
						
						if (buffer.size() > 0) {
//							System.out.println(buffCount++ + " " + count);
							
							StringBuffer stringBuffer = new StringBuffer();
							for (Resource r : buffer) {
								stringBuffer.append("<" + r.toString() + ">" );
							}
							
							String iSparql = itemsSparql.replaceAll("\\{@@RESOURCE@@\\}", stringBuffer.toString());
	
//							if (ttlWriter != null && count > 0) {
//								ttlWriter.append("\n");
//							}
							
							try (QueryExecution iqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), iSparql)) {
								
								Model model = VirtuosoConstructIterator.tryQuery(iqe, iSparql, logger);
//								model.clearNsPrefixMap();

								for (Resource r : buffer) {
									String iiSparql = resourceSparql.replaceAll("\\{@@RESOURCE@@\\}", "<" + r.toString() + ">");
									
									try (QueryExecution iiqe = QueryExecutionFactory.create(iiSparql, model)) {
										
										Model imodel = iiqe.execConstruct();
										imodel.clearNsPrefixMap();
										
										if (ttlWriter != null && count > 0) {
											ttlWriter.append("\n");
										}
										
										if (ttlWriter != null) {
											RDFDataMgr.write(ttlWriter, imodel, Lang.TURTLE);
										}
										
										if (ntWriter != null) {
											RDFDataMgr.write(ntWriter, imodel, Lang.NT);
										}
										
										count++;
									}
								}
								
//								if (ttlWriter != null) {
//									RDFDataMgr.write(ttlWriter, model, Lang.TURTLE);
//								}
//								
//								if (ntWriter != null) {
//									RDFDataMgr.write(ntWriter, model, Lang.NT);
//								}
//								
//								count++;
							}
						}
					}
					
//					System.out.println(cs.getClazz() + " : " + count);
					logger.info("Exported " + count + " items for " + cs.getClazz());
				}
		    	
		    	
		    	if (ttlWriter != null) {
		    		ttlWriter.flush();
		    	}
		    	
		    	if (ntWriter != null) {
		    		ntWriter.flush();
		    	}
	    	} finally {
	    		if (ttlWriter != null) {
	    			ttlWriter.close();
	    		}

	    		if (ntWriter != null) {
	    			ntWriter.close();
	    		}
	    	}
		    
//          not working properly	    	
//			try {
//				
//				int csCount = 0;
//		    	for (ClassStructureResponse cs : structs) {
//		    	
//					StringBuffer db = new StringBuffer();
//					
//					db.append("CONSTRUCT { ?resource a <" + cs.getClazz() + "> . ?resource ?p1 ?o1 . ");
//					for (int j = 2; j <= cs.getDepth(); j++) {
//						db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
//					}
//					db.append("} " + fromClause + " WHERE { ?resource a <" + cs.getClazz() + "> . ?resource  ?p1 ?o1 . ");
//		    		for (int j = 2; j <= cs.getDepth(); j++) {
//						db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
//					}			
//		    		for (int j = 2; j <= cs.getDepth(); j++) {
//						db.append(" } ");
//					}
//					db.append("}");
//					
//					String itemSparql = db.toString();
//		
//					if (ttlWriter != null && csCount > 0) {
//						ttlWriter.append("\n");
//					}
//					
//					csCount++;
//	
//					System.out.println(QueryFactory.create(itemSparql));
//					
//					try (VirtuosoConstructIterator vs = new VirtuosoConstructIterator(vc.getSparqlEndpoint(), itemSparql, 500)) {
//						
//						int count = 0;
//						while (vs.hasNext()) {
//							System.out.println(count++);							
//	//						Model model1 = vs.next();
//							
//							Model model = vs.next();
//							model.clearNsPrefixMap();
//							
//							if (ttlWriter != null) {
//								RDFDataMgr.write(ttlWriter, model, Lang.TURTLE);
//							}
//								
//							if (ntWriter != null) {
//								RDFDataMgr.write(ntWriter, model, Lang.NT);
//							}
//								
//						}
//					}
//				}
//		    	
//		    	if (ttlWriter != null) {
//		    		ttlWriter.flush();
//		    	}
//		    	
//		    	if (ntWriter != null) {
//		    		ntWriter.flush();
//		    	}
//	    	} finally {
//	    		if (ttlWriter != null) {
//	    			ttlWriter.close();
//	    		}
//
//	    		if (ntWriter != null) {
//	    			ntWriter.close();
//	    		}
//	    	}	    	
			
			if (options.getCompress().equals("ZIP")) {
				if (ttlFile != null) {
					zipDistribution(dc.getCurrentUser(), dataset, SerializationType.TTL, ttlFile);
				}

				if (ntFile != null) {
					zipDistribution(dc.getCurrentUser(), dataset, SerializationType.NT, ntFile);
				}

			}
			
			Date completedAt = new Date(System.currentTimeMillis()); 
	    	
	    	cds = dataset.checkCreateDistributionState(fileSystemConfiguration.getId(), vc.getId());
	    	cds.setCreateDistributionState(MappingState.EXECUTED);
	    	cds.setCreateDistributionCompletedAt(completedAt);
	    	
	    	datasetRepository.save(dataset);
	    	
	    	tripleStore.addDistributionToMetadata(dataset, vc);

	    	logger.info("Distribution for " + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + " created");
	    	
	    	pm.complete();
	    	
	    	pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc));

			return new AsyncResult<>(completedAt);

		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
	    	cds.setCreateDistributionState(MappingState.EXECUTION_FAILED);
	    	cds.setCreateDistributionCompletedAt(new Date(System.currentTimeMillis()));
	    	cds.setMessage(pm.getFailureMessage());
	    	
//			if (!(ex instanceof InterruptedException)) {
//				try {
			    	datasetRepository.save(dataset); 
//				} catch (Exception ex2) {
//					ex2.printStackTrace();
//				}
//			}

	    	
	    	pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc));
	    	
//			return new AsyncResult<>(false);
//	    	throw ex;
	    	throw new TaskFailureException(ex, pm.getCompletedAt());
		}
    }
	
	private File zipDistribution(UserPrincipal currentUser, Dataset dataset, SerializationType serialization, File fileToZip) throws IOException {
		
		File file = folderService.createDatasetDistributionZipFile(currentUser, dataset, (PublishState)dataset.getCurrentPublishState(virtuosoConfigurations.values()).getProcessState(), serialization);
		
		try (FileOutputStream fos = new FileOutputStream(file);
			ZipOutputStream zipOut = new ZipOutputStream(fos)) {
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
    	
		return file;
	}
	
	public boolean clearDistribution(DatasetContainer dc) throws Exception {

		if (dc.getTripleStoreConfiguration() == null) {
			return false;
		}

		Dataset dataset = dc.getDataset();
		
		CreateDistributionState cds = dataset.checkCreateDistributionState(fileSystemConfiguration.getId(), dc.getTripleStoreConfiguration().getId());
		
		if (cds != null) {
			
			if (cds.getCreateDistributionState() == MappingState.EXECUTED) {
				clearDistribution(dc.getCurrentUser(), dc.getDataset(), dc.getPublishState());
			}
		
			dataset.removeCreateDistributionState(cds);
			datasetRepository.save(dataset);
			
			return true;
		} else {
			return false;
		}
	}
	
	public boolean clearDistribution(UserPrincipal currentUser, Dataset dataset, PublishState cds) {
		
		try {
			File f = folderService.getDatasetDistributionFile(currentUser, dataset, cds, SerializationType.TTL);
			boolean ok = false;
			if (f != null) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
				
			}
			if (!ok) {
				logger.warn("Failed to delete ttl distribution for dataset " + dataset.getUuid());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}			

		try {
			File f = folderService.getDatasetDistributionZipFile(currentUser, dataset, cds, SerializationType.TTL);
			boolean ok = false;
			if (f != null) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
				
			}
			if (!ok) {
				logger.warn("Failed to delete ttl_zip distribution for dataset " + dataset.getUuid());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}			
		
		try {
			File f = folderService.getDatasetDistributionFile(currentUser, dataset, cds, SerializationType.NT);
			boolean ok = false;
			if (f != null) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
				
			}
			if (!ok) {
				logger.warn("Failed to delete nt distribution for dataset " + dataset.getUuid());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}			

		try {
			File f = folderService.getDatasetDistributionZipFile(currentUser, dataset, cds, SerializationType.NT);
			boolean ok = false;
			if (f != null) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
				
			}
			if (!ok) {
				logger.warn("Failed to delete nt distribution for dataset " + dataset.getUuid());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		

		
		folderService.deleteDatasetsDistributionFolderIfEmpty(currentUser, dataset);

		return true;
	}	
	
	public void failCreateDistribution(DatasetContainer dc) {			
		Dataset dataset = dc.getDataset();
		
		CreateDistributionState cds = dataset.checkCreateDistributionState(fileSystemConfiguration.getId(), dc.getTripleStoreConfiguration().getId());
		if (cds != null) {
			cds.setCreateDistributionState(MappingState.EXECUTION_FAILED);
			cds.setCreateDistributionCompletedAt(new Date());
			cds.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
			datasetRepository.save(dataset);
		}
	}

//	public void failCreateDistribution(String id, Throwable ex, Date date) {
//		Dataset dataset = datasetRepository.findById(id).get();
//		
//		TripleStoreConfiguration tripleStore = null;
//		
//		if (dataset.getTripleStoreId() != null) {
//			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
//				if (dataset.getTripleStoreId().equals(vc.getId())) {
//					tripleStore = vc;
//					break;
//				}
//			}
//		} else {
//			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
//				PublishState publishState = dataset.checkPublishState(vc.getId());
//				if (publishState != null) {
//					tripleStore = vc;
//					break;
//				}
//			}
//		}
//		
//		CreateDistributionState cds = dataset.checkCreateDistributionState(fileSystemConfiguration.getId(), tripleStore.getId());
//		
//		if (cds != null) {
//			cds.setCreateDistributionState(MappingState.EXECUTION_FAILED);
//			cds.setCreateDistributionCompletedAt(date);
//			if (ex != null) {
//				cds.setMessage(new NotificationMessage(MessageType.ERROR, ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage()));
//			}
//	
//			datasetRepository.save(dataset);
//		}
//	}	

}
