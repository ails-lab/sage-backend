package ac.software.semantic.service;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.request.IndexStructureUpdateRequest;
import ac.software.semantic.payload.response.IndexStructureResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.IndexDocumentRepository;
import ac.software.semantic.repository.core.IndexStructureRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskConflictException;
import ac.software.semantic.service.lookup.IndexStructureLookupProperties;

@Service
public class IndexStructureService implements EnclosedCreatableLookupService<IndexStructure, IndexStructureResponse, IndexStructureUpdateRequest, Dataset, IndexStructureLookupProperties>,
                                         EnclosingLookupService<IndexStructure, IndexStructureResponse, Dataset, IndexStructureLookupProperties>,
                                         IdentifiableDocumentService<IndexStructure, IndexStructureResponse> {

	private Logger logger = LoggerFactory.getLogger(IndexStructureService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private ModelMapper mapper;
	
	@Autowired
	private IndexStructureRepository indexStructureRepository;

	@Autowired
	private IndexDocumentRepository indexRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	
	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends EnclosedObjectContainer<IndexStructure, IndexStructureResponse, Dataset>> getContainerClass() {
		return IndexStructureContainer.class;
	}
	
	@Override 
	public DocumentRepository<IndexStructure> getRepository() {
		return indexStructureRepository;
	}
	
	public class IndexStructureContainer extends EnclosedObjectContainer<IndexStructure, IndexStructureResponse, Dataset>
	                                implements UpdatableContainer<IndexStructure, IndexStructureResponse, IndexStructureUpdateRequest>
//	                                TypeLookupContainer<ComparatorDocument, ComparatorDocumentResponse, ComparatorLookupProperties>
	                                {
		
		private ObjectId indexStructureId;
		
		private IndexStructureContainer(UserPrincipal currentUser, ObjectId indexStructureId) {
			this.currentUser = currentUser;
			this.indexStructureId = indexStructureId;

			load();
		}
		
		private IndexStructureContainer(UserPrincipal currentUser, IndexStructure idoc) {
			this.currentUser = currentUser;
			this.indexStructureId = idoc.getId();
			this.object = idoc;
		}
		
		private IndexStructureContainer(UserPrincipal currentUser, IndexStructure idoc, Dataset dataset) {
			this.currentUser = currentUser;

			this.indexStructureId = idoc.getId();
			this.object = idoc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return indexStructureId;
		}
		
		@Override 
		public DocumentRepository<IndexStructure> getRepository() {
			return indexStructureRepository;
		}
		
		@Override
		public IndexStructureService getService() {
			return IndexStructureService.this;
		}

		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}

//		@Override
//		public ComparatorLookupProperties buildTypeLookupPropetries() {
//			ComparatorLookupProperties lp = new ComparatorLookupProperties();
//			lp.setPrototypeType(object.getType());
//			
//			return lp;
//		}

		@Override
		public IndexStructure update(IndexStructureUpdateRequest ur) throws Exception {

			return update(iec -> {
				IndexStructure cdoc = iec.getObject();
			
				cdoc.setName(ur.getName());
				cdoc.setIdentifier(ur.getIdentifier());
				cdoc.setDescription(ur.getDescription());
				cdoc.setSchemaDatasetId(new ObjectId(ur.getSchemaDatasetId()));
				cdoc.setSchema(ur.getSchema());
				cdoc.setElements(ur.getElements());
				cdoc.setKeysMetadata(ur.getKeysMetadata());
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				List<IndexDocument> list = indexRepository.findByIndexStructureId(getPrimaryId());
				if (list.size() > 0) {
					throw new TaskConflictException("Cannot delete index definition because it is being used");
				}
				
				indexStructureRepository.delete(object);
	
				return true;
			}
		}
		
		@Override
		public String localSynchronizationString() {
			return ":" + getObject().getId().toString();
		}

		@Override
		public IndexStructureResponse asResponse() {
			IndexStructureResponse response = new IndexStructureResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());
	    	response.setDescription(object.getDescription());
	    	response.setIdentifier(object.getIdentifier());
	    	response.setSchemaDatasetId(object.getSchemaDatasetId().toString());
	    	response.setSchema(object.getSchema());
	    	response.setElements(mapper.indexStructure2IndexStructureResponse(object.getElements()));
	    	response.setKeysMetadata(object.getKeysMetadata());

	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	if (currentUser != null) {
	    		response.setOwnedByUser(currentUser.getId().equals(object.getUserId().toString()));
	    	}

	    	return response;
		}
		
		@Override
		public String getDescription() {
			return object.getName();
		}
		
		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(object.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			setEnclosingObject(datasetOpt.get());
		}

		@Override
		public TaskDescription getActiveTask(TaskType type) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	@Override
	public IndexStructureContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		IndexStructureContainer ec = new IndexStructureContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public IndexStructureContainer getContainer(UserPrincipal currentUser, IndexStructure idoc) {
		IndexStructureContainer pc = new IndexStructureContainer(currentUser, idoc);

		if (pc.getObject() == null) {
			return null;
		} else {
			return pc;
		}
	}

	@Override
	public EnclosedObjectContainer<IndexStructure, IndexStructureResponse, Dataset> getContainer(UserPrincipal currentUser, IndexStructure idoc, Dataset dataset) {
		IndexStructureContainer pc = new IndexStructureContainer(currentUser, idoc, dataset);
		
		if (pc.getObject() == null) {
			return null;
		} else {
			return pc;
		}
	}
	
	@Override	
	public IndexStructure create(UserPrincipal currentUser, Dataset dataset, IndexStructureUpdateRequest ur) throws Exception {
		
		IndexStructure idoc = new IndexStructure(dataset);
		idoc.setUserId(new ObjectId(currentUser.getId()));
		idoc.setName(ur.getName());
		idoc.setIdentifier(ur.getIdentifier());
		idoc.setDescription(ur.getDescription());
		idoc.setSchemaDatasetId(new ObjectId(ur.getSchemaDatasetId()));
		idoc.setSchema(ur.getSchema());
		idoc.setElements(ur.getElements());
		idoc.setKeysMetadata(ur.getKeysMetadata());
		
		return create(idoc);
	}

	@Override
	public ListPage<IndexStructure> getAllByUser(ObjectId userId, IndexStructureLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<IndexStructure> getAll(IndexStructureLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);
	}

	@Override
	public ListPage<IndexStructure> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		return getAllByUser(dataset, userId, null, page);
	}

	@Override
	public ListPage<IndexStructure> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, null, page);
	}

	@Override
	public ListPage<IndexStructure> getAll(List<Dataset> dataset, IndexStructureLookupProperties lp, Pageable page) {
		return getAllByUser(dataset, null, lp, page);
	}
	
	@Override
	public ListPage<IndexStructure> getAllByUser(List<Dataset> dataset, ObjectId userId, IndexStructureLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(indexStructureRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(indexStructureRepository.find(userId, dataset, lp, database.getId(), page));
		}
	}

	@Override
	public IndexStructureLookupProperties createLookupProperties() {
		return new IndexStructureLookupProperties();
	}
	
	
}
