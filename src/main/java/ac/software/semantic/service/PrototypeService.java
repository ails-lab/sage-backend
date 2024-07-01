package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DependencyBinding;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.request.PrototypeUpdateRequest;
import ac.software.semantic.payload.response.PrototypeDocumentResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.MappingDocumentRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.TypeLookupContainer;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskConflictException;
import ac.software.semantic.service.lookup.PrototypeLookupProperties;

@Service
public class PrototypeService implements EnclosedCreatableLookupService<PrototypeDocument, PrototypeDocumentResponse, PrototypeUpdateRequest, Dataset ,PrototypeLookupProperties>,
                                         EnclosingLookupService<PrototypeDocument, PrototypeDocumentResponse,Dataset, PrototypeLookupProperties> {

	private Logger logger = LoggerFactory.getLogger(PrototypeService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	private PrototypeDocumentRepository prototypeRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;

	@Autowired
	private MappingDocumentRepository mappingRepository;
	
	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends EnclosedObjectContainer<PrototypeDocument,PrototypeDocumentResponse,Dataset>> getContainerClass() {
		return PrototypeContainer.class;
	}
	
	@Override 
	public DocumentRepository<PrototypeDocument> getRepository() {
		return prototypeRepository;
	}
	
	public class PrototypeContainer extends EnclosedObjectContainer<PrototypeDocument,PrototypeDocumentResponse,Dataset>
	                                implements UpdatableContainer<PrototypeDocument, PrototypeDocumentResponse,PrototypeUpdateRequest>,
	                                TypeLookupContainer<PrototypeDocument, PrototypeDocumentResponse, PrototypeLookupProperties>
	                                {
		
		private ObjectId prototypeId;
		
		private PrototypeContainer(UserPrincipal currentUser, ObjectId prototypeId) {
			this.currentUser = currentUser;
			this.prototypeId = prototypeId;

			load();
		}
		
		private PrototypeContainer(UserPrincipal currentUser, PrototypeDocument pdoc) {
			this.currentUser = currentUser;
			this.prototypeId = pdoc.getId();
			this.object = pdoc;
		}
		
		private PrototypeContainer(UserPrincipal currentUser, PrototypeDocument pdoc, Dataset dataset) {
			this.currentUser = currentUser;

			this.prototypeId = pdoc.getId();
			this.object = pdoc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return prototypeId;
		}
		
		@Override 
		public DocumentRepository<PrototypeDocument> getRepository() {
			return prototypeRepository;
		}
		
		@Override
		public PrototypeService getService() {
			return PrototypeService.this;
		}

		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}

		@Override
		public PrototypeLookupProperties buildTypeLookupPropetries() {
			PrototypeLookupProperties lp = new PrototypeLookupProperties();
			lp.setPrototypeType(object.getType());
			
			return lp;
		}

		@Override
		public PrototypeDocument update(PrototypeUpdateRequest ur) throws Exception {

			return update(iec -> {
				PrototypeDocument pdoc = iec.getObject();
			
				pdoc.setName(ur.getName());
				pdoc.setDescription(ur.getDescription());

				pdoc.setFields(ur.getFields());
				
				MultipartFile file = null;
				if (ur.getFile() != null) {
					file = ur.getFile();
				} else if (ur.getUrl() != null) {
					file = serviceUtils.download(ur.getUrl());
				}
				
				// should validate file to get parameters !!!!
				
				if (file != null) {
					pdoc.setContent(new String(file.getBytes()));
					
					if (ur.getParameters() != null && ur.getParameters().size() > 0) {
						List<DataServiceParameter> params = new ArrayList<>();
						loop:
						for (DataServiceParameter p : ur.getParameters()) {
							if (object.getParameters() != null) {
								for (DataServiceParameter currentParam : object.getParameters()) {
									if (currentParam.getName().equals(p.getName())) { // why ???
										params.add(p);
										continue loop;
									}
								}
							}
							params.add(p);
						}
						
						pdoc.setParameters(params);	
					} else {
						pdoc.setParameters(null);
					}
					
					List<String> dependencies = ur.getDependencies();
					if (dependencies != null && dependencies.size() > 0) {
						List<DependencyBinding> dps = new ArrayList<>(); 
						for (String s : dependencies) {
							DependencyBinding dp = new DependencyBinding();
							dp.setName(s);
							dps.add(dp);
						}
						pdoc.setDependencies(dps);
					}

				} else {
//					pdoc.setParameters(ur.getParameters()); // this overwrites parameters
				}
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
//				clearExecution();
				
				if (object.getType() == PrototypeType.ANNOTATOR) {
					List<AnnotatorDocument> list = annotatorRepository.findByAnnotatorId(getPrimaryId());
					if (list.size() > 0) {
						throw new TaskConflictException("Cannot delete annotator because it is being used");
					}
				} else if (object.getType() == PrototypeType.D2RML) {
					List<MappingDocument> list = mappingRepository.findByD2rmlId(getPrimaryId());
					if (list.size() > 0) {
						throw new TaskConflictException("Cannot delete D2RML mapping because it is being used");
					}
				}
					
				prototypeRepository.delete(object);
	
				return true;
			}
		}
		
		@Override
		public String localSynchronizationString() {
			return ":" + getObject().getId().toString();
		}

		@Override
		public PrototypeDocumentResponse asResponse() {
	    	PrototypeDocumentResponse response = new PrototypeDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());
	    	response.setDescription(object.getDescription());
	    	response.setType(object.getType());
	    	response.setUrl(object.getUrl());
	    	response.setParameters(object.getParameters());
//	    	response.setContent(pdoc.getContent());
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	response.setFields(object.getFields());
	    	
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
	public PrototypeContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		PrototypeContainer ec = new PrototypeContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public PrototypeContainer getContainer(UserPrincipal currentUser, PrototypeDocument pdoc) {
		PrototypeContainer pc = new PrototypeContainer(currentUser, pdoc);

		if (pc.getObject() == null) {
			return null;
		} else {
			return pc;
		}
	}

	@Override
	public EnclosedObjectContainer<PrototypeDocument,PrototypeDocumentResponse,Dataset> getContainer(UserPrincipal currentUser, PrototypeDocument pdoc, Dataset dataset) {
		PrototypeContainer pc = new PrototypeContainer(currentUser, pdoc, dataset);
		
		if (pc.getObject() == null) {
			return null;
		} else {
			return pc;
		}
	}
	
	@Override	
	public PrototypeDocument create(UserPrincipal currentUser, Dataset dataset, PrototypeUpdateRequest ur) throws Exception {
		
		PrototypeDocument pd = new PrototypeDocument(dataset);
		pd.setUserId(new ObjectId(currentUser.getId()));
		pd.setName(ur.getName());
		pd.setDescription(ur.getDescription());
		pd.setType(ur.getType());
		pd.setUrl(ur.getUrl());
		
		
		MultipartFile file = null;
		if (ur.getFile() != null) {
			file = ur.getFile();
		} else if (ur.getUrl() != null) {
			file = serviceUtils.download(ur.getUrl());
		}
		
		if (file != null) {
			pd.setContent(new String(file.getBytes()));
		} else {
			pd.setContent("");
		}
		

		if (ur.getParameters() != null && ur.getParameters().size() > 0) {
			pd.setParameters(ur.getParameters());	
		}
		
		if (ur.getFields() != null && ur.getFields().size() > 0) {
			pd.setFields(ur.getFields());
		}
		
		List<String> dependencies = ur.getDependencies();
		if (dependencies != null && dependencies.size() > 0) {
			List<DependencyBinding> dps = new ArrayList<>(); 
			for (String s : dependencies) {
				DependencyBinding dp = new DependencyBinding();
				dp.setName(s);
				dps.add(dp);
			}
			pd.setDependencies(dps);
		}

		return create(pd);
	}

	@Override
	public ListPage<PrototypeDocument> getAllByUser(ObjectId userId, PrototypeLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<PrototypeDocument> getAll(PrototypeLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);
	}

	@Override
	public ListPage<PrototypeDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		return getAllByUser(dataset, userId, null, page);
	}

	@Override
	public ListPage<PrototypeDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, null, page);
	}

	@Override
	public ListPage<PrototypeDocument> getAll(List<Dataset> dataset, PrototypeLookupProperties lp, Pageable page) {
		return getAllByUser(dataset, null, lp, page);
	}
	
	@Override
	public ListPage<PrototypeDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, PrototypeLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(prototypeRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(prototypeRepository.find(userId, dataset, lp, database.getId(), page));
		}
	}

	@Override
	public PrototypeLookupProperties createLookupProperties() {
		return new PrototypeLookupProperties();
	}
}
