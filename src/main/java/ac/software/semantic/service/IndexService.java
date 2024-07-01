package ac.software.semantic.service;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.base.CreatableDocument;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.CreatableContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.StateConflictException;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.lookup.IndexLookupProperties;
import ac.software.semantic.service.monitor.GenericMonitor;
import ac.software.semantic.service.monitor.IndexMonitor;
import ac.software.semantic.payload.notification.CreateNotificationObject;
import ac.software.semantic.payload.request.IndexUpdateRequest;
import ac.software.semantic.payload.response.IndexDocumentResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.IndexDocumentRepository;
import ac.software.semantic.repository.core.IndexStructureRepository;
import ac.software.semantic.repository.core.TaskRepository;

@Service
public class IndexService implements EnclosingService<IndexDocument, IndexDocumentResponse, Dataset>,
                                     EnclosedCreatableService<IndexDocument, IndexDocumentResponse, IndexUpdateRequest, Dataset>,
                                     CreatingService<IndexDocument, IndexDocumentResponse> {

	private Logger logger = LoggerFactory.getLogger(IndexService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private ElasticSearch elasticSearch;

	@Autowired
	@Qualifier("triplestore-configurations")
	private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;
    
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
    
	@Autowired
	private IndexDocumentRepository indexRepository;

	@Autowired
	private IndexStructureRepository indexStructureRepository;

	@Autowired
	private DatasetRepository datasetRepository;
	
    @Autowired
    private IndexDocumentRepository indexDocumentRepository;
	
	@Autowired
	private TaskRepository taskRepository;
	
	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends EnclosedObjectContainer<IndexDocument,IndexDocumentResponse,Dataset>> getContainerClass() {
		return IndexContainer.class;
	}
	
	@Override
	public DocumentRepository<IndexDocument> getRepository() {
		return indexDocumentRepository;
	}
	
	public class IndexContainer extends EnclosedObjectContainer<IndexDocument,IndexDocumentResponse,Dataset> 
	                            implements UpdatableContainer<IndexDocument, IndexDocumentResponse,IndexUpdateRequest>, 
	                                       CreatableContainer<IndexDocument, IndexDocumentResponse,IndexState,Dataset> {
		
		private ObjectId indexId;
		private IndexStructure indexStructure;
		
		private ElasticConfiguration elasticConfiguration;
		
		private IndexContainer(UserPrincipal currentUser, ObjectId indexId) {
			this.currentUser = currentUser;
			
			this.indexId = indexId;
		
			load();
			
			this.elasticConfiguration = elasticConfigurations.getById(object.getElasticConfigurationId());
		}
		
		private IndexContainer(UserPrincipal currentUser, IndexDocument idoc) {
			this(currentUser, idoc, null);
		}
		
		private IndexContainer(UserPrincipal currentUser, IndexDocument idoc, Dataset dataset) {
			this.currentUser = currentUser;

			this.indexId = idoc.getId();
			this.object = idoc;
			
			this.dataset = dataset;
			
			this.elasticConfiguration = elasticConfigurations.getById(object.getElasticConfigurationId());
		}

		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return indexId;
		}
		
		@Override
		public DocumentRepository<IndexDocument> getRepository() {
			return indexDocumentRepository;
		}
		
		@Override
		public IndexService getService() {
			return IndexService.this;
		}
		
		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}
		
//		@Override
//		public void load() {
//			Optional<IndexDocument> indexOpt = indexRepository.findById(indexId);
//
//			if (!indexOpt.isPresent()) {
//				return;
//			}
//
//			object = indexOpt.get();
//		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(object.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			setEnclosingObject(datasetOpt.get());
		}
		
		protected void loadIndexStructure() {
			Optional<IndexStructure> indexStructureOpt = indexStructureRepository.findById(object.getIndexStructureId());

			if (!indexStructureOpt.isPresent()) {
				return;
			}
		
			indexStructure = indexStructureOpt.get();
		}
		
		public IndexStructure getIndexStructure() {
			if (indexStructure == null) {
				loadIndexStructure();
			}
		
			return indexStructure;
		}

		@Override
		public CreatableDocument<IndexState> getCreateDocument() {
			return getObject();
		}
		
//		@Override 
//		public Dataset getEnclosingObject() {
//			return getEnclosingObject();
//		}
		
//		@Override
//		public IndexDocument update(IndexUpdateRequest ur) throws Exception {
//
//			boolean idefault = ur.isIdefault();
//			IndexDocument idoc = getObject();
//			
//			if (idoc.getIdefault() != idefault) {
//				if (idefault) {
//					List<IndexDocument> idocs = indexRepository.findByDatasetIdAndIndexStructureId(idoc.getDatasetId(), idoc.getIndexStructureId());
//					if (idocs.size() > 1) {
//								
//						for (IndexDocument iidoc : idocs) {
//							if (!iidoc.getId().equals(idoc.getId())) {
//								((IndexContainer)getContainer(getCurrentUser(), iidoc)).updateI(false);
//							}
//						}
//					}
//				}
//				
//				((IndexContainer)getContainer(getCurrentUser(), idoc)).updateI(idefault);
//			}
//			
//			return getObject();
//		}
//		
//		private IndexDocument updateI(boolean idefault) throws Exception {
//
//			return save(iic -> {
//				IndexDocument idoc = iic.getObject();
//
//				idoc.setIdefault(idefault);
//			});
//		}

		@Override
		public IndexDocument update(IndexUpdateRequest ur) throws Exception {

			return update(iic -> {
				IndexDocument idoc = iic.getObject();
				
				idoc.setName(ur.getName());
				idoc.setIndexStructureId(new ObjectId(ur.getIndexStructureId()));
				idoc.setElasticConfigurationId(ur.getElasticConfiguration().getId());
			});
		}

		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
					
				indexRepository.delete(object);
	
				return true;
			}
		}
		
		public void setIndexId(ObjectId indexId) {
			this.indexId = indexId;
		}

		@Override
		public String localSynchronizationString() {
			return getObject().getId().toString();
		}

		@Override
		public IndexDocumentResponse asResponse() {
			
	    	IndexDocumentResponse response = new IndexDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());

	    	response.setIndexStructureId(getIndexStructure().getId().toString());
	    	response.setIndexStructureIdentifier(getIndexStructure().getIdentifier());
	    	response.setElasticConfiguration(elasticConfigurations.getById(object.getElasticConfigurationId()).getName());
//	    	response.setIdefault(idoc.getIdefault());
	    	response.setOrder(object.getOrder());
	    	
	    	if (currentUser != null) {
	    		response.setOwnedByUser(currentUser.getId().equals(object.getUserId().toString()));
	    	}
	    	
	    	response.copyStates(object, null, fileSystemConfiguration);
	    	
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	return response;
		}
		
		@Override
		public String getDescription() {
			return getIndexStructure().getIdentifier();
		}

		@Override
		public TaskType getCreateTask() {
			return TaskType.INDEX_CREATE;
		}

		@Override
		public TaskType getDestroyTask() {
			return TaskType.INDEX_DESTROY;
		}

		@Override
		public TaskType getRecreateTask() {
			return TaskType.INDEX_RECREATE;
		}
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByIndexIdAndElasticConfigurationId(getObject().getId(), getContainerElasticConfiguration().getId(), type).orElse(null);
		}
		
		@Override
		public ElasticConfiguration getContainerElasticConfiguration() {
			return elasticConfiguration;
		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}

	
	@Override
	public IndexContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		IndexContainer ec = new IndexContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public IndexContainer getContainer(UserPrincipal currentUser, IndexDocument idoc, Dataset dataset) {
		IndexContainer ec = new IndexContainer(currentUser, idoc, dataset);

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
    
//	public IndexDocument create(UserPrincipal currentUser, ObjectId datasetId, IndexStructure indexStructure, ElasticConfiguration ec) throws StateConflictException {
//
//		Optional<Dataset> datasetOpt = datasetRepository.findById(datasetId);
//		if (!datasetOpt.isPresent()) {
//			throw new StateConflictException("The dataset does not exists.");
//		}
//
//    	Optional<IndexDocument> idocOpt = indexDocumentRepository.findByDatasetIdAndIndexStructureIdAndElasticConfigurationId(datasetId, indexStructure.getId(), ec.getId());
//    	
//    	if (idocOpt.isPresent()) {
//    		throw new StateConflictException("An index for the particular dataset with the same index structure already exists.");
//    	}
//    	
//		Dataset dataset = datasetOpt.get();
//
//		String uuid = UUID.randomUUID().toString();
//
//		IndexDocument idoc = new IndexDocument();
//		idoc.setUuid(uuid);
//		idoc.setDatabaseId(database.getId());
//		idoc.setUserId(new ObjectId(currentUser.getId()));
//
//		idoc.setDatasetId(dataset.getId());
//		idoc.setDatasetUuid(dataset.getUuid());
//		idoc.setIndexStructureId(indexStructure.getId());
//		idoc.setElasticConfigurationId(ec.getId());
//		
//		idoc.setUpdatedAt(new Date());
//
//		return indexRepository.save(idoc);
//	}

	@Override
	public IndexDocument create(UserPrincipal currentUser, Dataset dataset, IndexUpdateRequest ur) throws Exception {

    	Optional<IndexDocument> idocOpt = indexDocumentRepository.findByDatasetIdAndIndexStructureIdAndElasticConfigurationId(dataset.getId(), new ObjectId(ur.getIndexStructureId()), ur.getElasticConfiguration().getId());
    	
    	if (idocOpt.isPresent()) {
    		throw new StateConflictException("An index for the particular dataset with the same index structure already exists.");
    	}
    	
		IndexDocument idoc = new IndexDocument(dataset);
		idoc.setUserId(new ObjectId(currentUser.getId()));
		idoc.setIndexStructureId(new ObjectId(ur.getIndexStructureId()));
		idoc.setElasticConfigurationId(ur.getElasticConfiguration().getId());
		idoc.setName(ur.getName());
		
		return create(idoc);
	}

//	public IndexDocument updateCheck(IndexContainer ic, boolean idefault) throws Exception {
//
//		IndexDocument idoc = ic.getObject();
//		
//		if (idoc.getIdefault() != idefault) {
//			if (idefault) {
//				List<IndexDocument> idocs = indexRepository.findByDatasetIdAndIndexStructureId(idoc.getDatasetId(), idoc.getIndexStructureId());
//				if (idocs.size() > 1) {
//							
//					for (IndexDocument iidoc : idocs) {
//						if (!iidoc.getId().equals(idoc.getId())) {
//							update(getContainer(ic.getCurrentUser(), iidoc), false);
//						}
//					}
//				}
//			}
//			
//			update(getContainer(ic.getCurrentUser(), idoc), idefault);
//		}
//		
//		return ic.getObject();
//		
//	}
//	
//	public IndexDocument update(ObjectContainer<IndexDocument> ic, boolean idefault) throws Exception {
//
//		ic.save(iic -> {
//			IndexDocument idoc = ((IndexContainer)iic).getObject();
//
//			idoc.setIdefault(idefault);
//		});
//			
//		return ic.getObject();
//	}

//	public IndexStructure createIndexStructure(UserPrincipal currentUser, String indexIdentifier, ElasticConfiguration ec, List<ClassIndexElement> elements, List<IndexKeyMetadata> keysMetadata) {
//
//		Optional<IndexStructure> isOpt = indexStructureRepository.findByIdentifierAndDatabaseId(indexIdentifier, database.getId());
//		if (isOpt.isPresent()) {
//			return null;
//		}
//
//		String uuid = UUID.randomUUID().toString();
//
//		IndexStructure is = new IndexStructure();
//		is.setUserId(new ObjectId(currentUser.getId()));
//		is.setDatabaseId(database.getId());
//		is.setUuid(uuid);
//		is.setIdentifier(indexIdentifier);
//		is.setElements(elements);
//		is.setKeysMetadata(keysMetadata);
//		is.setUpdatedAt(new Date());
////		is.setElasticConfigurationId(ec.getId());
//
//		is = indexStructureRepository.save(is);
//
//		return is;
//	}
	
	@Async("indexExecutor")
	public ListenableFuture<Date> create(TaskDescription tdescr,  WebSocketService wsService) throws TaskFailureException {
		IndexMonitor pm = (IndexMonitor)tdescr.getMonitor();
		
		IndexContainer ic = (IndexContainer)tdescr.getContainer();
		IndexDocument idoc = ic.getObject();
		
		try {
			Dataset dataset = ic.getEnclosingObject();
			IndexStructure idxStruct = ic.getIndexStructure();
			
//			ElasticConfiguration ec = idxStruct.bind(elasticConfigurations);
			ElasticConfiguration ec = elasticConfigurations.getById(idoc.getElasticConfigurationId());

			ic.update(ioc -> {
				IndexState iis = ((CreatableContainer<IndexDocument,IndexDocumentResponse,IndexState,Dataset>)ioc).getCreateState();
				iis.startDo(pm);
			});

			pm.sendMessage(new CreateNotificationObject(ic));
	        
			logger.info("Indexing of " + ic.getClass().getName() + ":"  + ic.getPrimaryId() + " started.");
			
			if (elasticSearch.existsIndex(database, dataset, ec, idxStruct)) {
				pm.sendMessage(new CreateNotificationObject(ic), "Deleting existing index");
				
				logger.info("Deleting index for " + ic.getClass().getName() + ":"  + ic.getPrimaryId());
				
				elasticSearch.deleteIndex(database, dataset, ec, idxStruct); 
			}
			
			pm.sendMessage(new CreateNotificationObject(ic), "Creating index");
			
			logger.info("Creating index for " + ic.getClass().getName() + ":"  + ic.getPrimaryId());
			
			elasticSearch.createIndex(database, dataset, ec, idxStruct);
			
			pm.createStructure(idxStruct);
			
			pm.startIndexing(idoc);

			elasticSearch.index(dataset, ec, idxStruct, pm);
	        
	        pm.complete();

			ic.update(ioc -> {
				CreatableContainer<IndexDocument,IndexDocumentResponse,IndexState,Dataset> coc = (CreatableContainer<IndexDocument,IndexDocumentResponse,IndexState,Dataset>)ioc;
				
				IndexState iis = coc.getCreateState();
				iis.completeDo(pm);
				iis.setPublish((PublishState)(coc.getEnclosingObject().getCurrentPublishState(virtuosoConfigurations.values()).getProcessState()));
			});

			logger.info("Indexing of " + ic.getClass().getName() + ":"  + ic.getPrimaryId() + " completed.");

			pm.sendMessage(new CreateNotificationObject(ic));

			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			logger.info("Indexing of " + ic.getClass().getName() + ":"  + ic.getPrimaryId() + " failed.");
			
			ex.printStackTrace();
			
			pm.complete(ex);
			
			try {
				ic.update(ioc -> {
					CreateState ics = ((CreatableContainer)ioc).checkCreateState();
					if (ics != null) {
						ics.failDo(pm);
					}
				});
				
				if (ic.checkCreateState() != null) {
					pm.sendMessage(new CreateNotificationObject(ic));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		
		} finally {
			try {
				if (pm != null) {
					pm.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Async("indexExecutor")
	public ListenableFuture<Date> destroy(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		IndexContainer ic = (IndexContainer)tdescr.getContainer();
		IndexDocument idoc = ic.getObject();
		
		try {
			Dataset dataset = ic.getEnclosingObject();
			IndexStructure idxStruct = ic.getIndexStructure();
			
//			ElasticConfiguration ec = idxStruct.bind(elasticConfigurations);
			ElasticConfiguration ec = elasticConfigurations.getById(idoc.getElasticConfigurationId());

			ic.update(ioc -> {
				IndexState iis = ((CreatableContainer<IndexDocument,IndexDocumentResponse,IndexState,Dataset>)ioc).getCreateState();
				iis.startUndo(pm);
			});
			
			pm.sendMessage(new CreateNotificationObject(ic));
			
			logger.info("Unindexing of " + ic.getClass().getName() + ":"  + ic.getPrimaryId() + " started.");
			
//			indexService.unindex(dataset, ec, resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString());
			if (elasticSearch.existsIndex(database, dataset, ec, idxStruct)) {
				logger.info("Deleting index for " + ic.getClass().getName() + ":"  + ic.getPrimaryId());
				
				elasticSearch.deleteIndex(database, dataset, ec, idxStruct); 
			}
			
			pm.complete();
			
			ic.update(ioc -> {
				((CreatableContainer<IndexDocument,IndexDocumentResponse,IndexState,Dataset>)ioc).deleteCreateState();
			});
			
			logger.info("Unindexing of " + ic.getClass().getName() + ":"  + ic.getPrimaryId() + " completed.");
			
			
			pm.sendMessage(new CreateNotificationObject(ic));
			
			return new AsyncResult<>(pm.getCompletedAt());
		
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);
			
			try {
				ic.update(ioc -> {
					IndexState iis = ((CreatableContainer<IndexDocument,IndexDocumentResponse,IndexState,Dataset>)ioc).checkCreateState();
					if (iis != null) {
						iis.failUndo(pm);
					}
				});
				
				if (ic.checkCreateState() != null) {
					pm.sendMessage(new CreateNotificationObject(ic));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}

	@Override
	public ListPage<IndexDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, page);
//		if (page == null) {
//			return ListPage.create(indexRepository.findByDatabaseIdAndUserIdAndElasticConfigurationIds(database.getId(), userId, elasticConfigurations.getIdMap().keySet()));
//		} else {
//			return ListPage.create(indexRepository.findByDatabaseIdAndUserIdAndElasticConfigurationIds(database.getId(), userId, elasticConfigurations.getIdMap().keySet(), page));
//		}
	}

	@Override
	public ListPage<IndexDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		IndexLookupProperties lp = new IndexLookupProperties();
		lp.setElasticConfigurationId(elasticConfigurations.getIdMap().keySet());

		if (page == null) {
			return ListPage.create(indexRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(indexRepository.find(userId, dataset, lp, database.getId(), page));
		}
//		if (page == null) {
//			return ListPage.create(indexRepository.findByDatasetIdAndUserIdAndElasticConfigurationIds(dataset.getId(), userId, elasticConfigurations.getIdMap().keySet()));
//		} else {
//			return ListPage.create(indexRepository.findByDatasetIdAndUserIdAndElasticConfigurationIds(dataset.getId(), userId, elasticConfigurations.getIdMap().keySet(), page));
//		}
	}

}