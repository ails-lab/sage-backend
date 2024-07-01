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

import ac.software.semantic.model.ComparatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.request.ComparatorUpdateRequest;
import ac.software.semantic.payload.response.ComparatorDocumentResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.ComparatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.lookup.ComparatorLookupProperties;

@Service
public class ComparatorService implements EnclosedCreatableLookupService<ComparatorDocument, ComparatorDocumentResponse, ComparatorUpdateRequest, Dataset, ComparatorLookupProperties>,
                                         EnclosingLookupService<ComparatorDocument, ComparatorDocumentResponse, Dataset, ComparatorLookupProperties>,
                                         IdentifiableDocumentService<ComparatorDocument, ComparatorDocumentResponse> {

	private Logger logger = LoggerFactory.getLogger(ComparatorService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private ModelMapper mapper;
	
	@Autowired
	private ComparatorDocumentRepository comparatorRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	
	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends EnclosedObjectContainer<ComparatorDocument,ComparatorDocumentResponse,Dataset>> getContainerClass() {
		return ComparatorContainer.class;
	}
	
	@Override 
	public DocumentRepository<ComparatorDocument> getRepository() {
		return comparatorRepository;
	}
	
	public class ComparatorContainer extends EnclosedObjectContainer<ComparatorDocument, ComparatorDocumentResponse, Dataset>
	                                implements UpdatableContainer<ComparatorDocument, ComparatorDocumentResponse, ComparatorUpdateRequest>
//	                                TypeLookupContainer<ComparatorDocument, ComparatorDocumentResponse, ComparatorLookupProperties>
	                                {
		
		private ObjectId comparatorId;
		
		private ComparatorContainer(UserPrincipal currentUser, ObjectId comparatorId) {
			this.currentUser = currentUser;
			this.comparatorId = comparatorId;

			load();
		}
		
		private ComparatorContainer(UserPrincipal currentUser, ComparatorDocument cdoc) {
			this.currentUser = currentUser;
			this.comparatorId = cdoc.getId();
			this.object = cdoc;
		}
		
		private ComparatorContainer(UserPrincipal currentUser, ComparatorDocument cdoc, Dataset dataset) {
			this.currentUser = currentUser;

			this.comparatorId = cdoc.getId();
			this.object = cdoc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return comparatorId;
		}
		
		@Override 
		public DocumentRepository<ComparatorDocument> getRepository() {
			return comparatorRepository;
		}
		
		@Override
		public ComparatorService getService() {
			return ComparatorService.this;
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
		public ComparatorDocument update(ComparatorUpdateRequest ur) throws Exception {

			return update(iec -> {
				ComparatorDocument cdoc = iec.getObject();
			
				cdoc.setName(ur.getName());
				cdoc.setIdentifier(ur.getIdentifier());
				cdoc.setDescription(ur.getDescription());
				cdoc.setSchemaDatasetId(new ObjectId(ur.getSchemaDatasetId()));
//				cdoc.setElement(ur.getElement());
//				cdoc.setKeysMetadata(ur.getKeysMetadata());
				cdoc.setStructure(ur.getStructure());
				cdoc.setComputation(ur.getComputation());
				
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				comparatorRepository.delete(object);
	
				return true;
			}
		}
		
		@Override
		public String localSynchronizationString() {
			return ":" + getObject().getId().toString();
		}

		@Override
		public ComparatorDocumentResponse asResponse() {
			ComparatorDocumentResponse response = new ComparatorDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());
	    	response.setIdentifier(object.getIdentifier());
	    	response.setSchemaDatasetId(object.getSchemaDatasetId().toString());
	    	response.setDescription(object.getDescription());
	    	response.setComputation(object.getComputation());
	    	response.setElement(mapper.indexStructure2IndexStructureResponse(object.getStructure().getElement()));
	    	response.setKeysMetadata(object.getStructure().getKeysMetadata());

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
	public ComparatorContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		ComparatorContainer ec = new ComparatorContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public ComparatorContainer getContainer(UserPrincipal currentUser, ComparatorDocument cdoc) {
		ComparatorContainer pc = new ComparatorContainer(currentUser, cdoc);

		if (pc.getObject() == null) {
			return null;
		} else {
			return pc;
		}
	}

	@Override
	public EnclosedObjectContainer<ComparatorDocument, ComparatorDocumentResponse, Dataset> getContainer(UserPrincipal currentUser, ComparatorDocument cdoc, Dataset dataset) {
		ComparatorContainer pc = new ComparatorContainer(currentUser, cdoc, dataset);
		
		if (pc.getObject() == null) {
			return null;
		} else {
			return pc;
		}
	}
	
	@Override	
	public ComparatorDocument create(UserPrincipal currentUser, Dataset dataset, ComparatorUpdateRequest ur) throws Exception {
		
		ComparatorDocument cdoc = new ComparatorDocument(dataset);
		cdoc.setUserId(new ObjectId(currentUser.getId()));
		cdoc.setName(ur.getName());
		cdoc.setIdentifier(ur.getIdentifier());
		cdoc.setDescription(ur.getDescription());
		cdoc.setSchemaDatasetId(new ObjectId(ur.getSchemaDatasetId()));
//		cdoc.setElement(ur.getElement());
//		cdoc.setKeysMetadata(ur.getKeysMetadata());
		cdoc.setStructure(ur.getStructure());
		cdoc.setComputation(ur.getComputation());
		
		return create(cdoc);
	}

	@Override
	public ListPage<ComparatorDocument> getAllByUser(ObjectId userId, ComparatorLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<ComparatorDocument> getAll(ComparatorLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);
	}

	@Override
	public ListPage<ComparatorDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		return getAllByUser(dataset, userId, null, page);
	}

	@Override
	public ListPage<ComparatorDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, null, page);
	}

	@Override
	public ListPage<ComparatorDocument> getAll(List<Dataset> dataset, ComparatorLookupProperties lp, Pageable page) {
		return getAllByUser(dataset, null, lp, page);
	}
	
	@Override
	public ListPage<ComparatorDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, ComparatorLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(comparatorRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(comparatorRepository.find(userId, dataset, lp, database.getId(), page));
		}
	}

	@Override
	public ComparatorLookupProperties createLookupProperties() {
		return new ComparatorLookupProperties();
	}
	
	
}
