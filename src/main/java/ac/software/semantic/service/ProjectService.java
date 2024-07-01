package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Date;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.request.DatasetUpdateRequest;
import ac.software.semantic.payload.request.ProjectUpdateRequest;
import ac.software.semantic.payload.response.ProjectDocumentResponse;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.UserResponse;
import ac.software.semantic.payload.response.modifier.ProjectResponseModifier;
import ac.software.semantic.payload.response.modifier.UserResponseModifier;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.ProjectDocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.MemberContainer;
import ac.software.semantic.service.container.MultipleResponseContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.lookup.ProjectLookupProperties;

@Service
public class ProjectService implements CreatableService<ProjectDocument, ProjectDocumentResponse,ProjectUpdateRequest>,
                                       LookupService<ProjectDocument, ProjectDocumentResponse,ProjectLookupProperties>,
                                       IdentifiableDocumentService<ProjectDocument, ProjectDocumentResponse> {

	private Logger logger = LoggerFactory.getLogger(ProjectService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private ProjectDocumentRepository projectRepository;

	@Autowired
	private UserService userService;

	@Lazy
	@Autowired
	private APIUtils apiUtils;

	@Autowired
	private ServiceUtils serviceUtils;

	@Override
	public Class<? extends ObjectContainer<ProjectDocument,ProjectDocumentResponse>> getContainerClass() {
		return ProjectContainer.class;
	}
	
	@Override 
	public DocumentRepository<ProjectDocument> getRepository() {
		return projectRepository;
	}
	
	public class ProjectContainer extends ObjectContainer<ProjectDocument,ProjectDocumentResponse> 
	                              implements UpdatableContainer<ProjectDocument,ProjectDocumentResponse,ProjectUpdateRequest>,
	                                         MemberContainer<ProjectDocument, ProjectDocumentResponse, User>,
	                                         MultipleResponseContainer<ProjectDocument, ProjectDocumentResponse, ProjectResponseModifier> {
		
		private ObjectId projectId;
		
		private ProjectContainer(UserPrincipal currentUser, ObjectId projectId) {
			this.currentUser = currentUser;
			this.projectId = projectId;
		
			load();
		}
		
		private ProjectContainer(UserPrincipal currentUser, ProjectDocument pdoc) {
			this.currentUser = currentUser;
			this.projectId = pdoc.getId();
			this.object = pdoc;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return projectId;
		}
		
		@Override 
		public DocumentRepository<ProjectDocument> getRepository() {
			return projectRepository;
		}
		
		@Override
		public ProjectService getService() {
			return ProjectService.this;
		}
		
		@Override
		public ProjectDocument update(ProjectUpdateRequest ur) throws Exception {

			return update(iec -> {
				ProjectDocument edoc = iec.getObject();
				edoc.setName(ur.getName());
				edoc.setPublik(ur.isPublik());
				edoc.setIdentifier(ur.getIdentifier());
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
					
				projectRepository.delete(object);
	
				return true;
			}
		}
		
		@Override
		public String localSynchronizationString() {
			return getObject().getId().toString();
		}

		@Override
		public ProjectDocumentResponse asResponse(ProjectResponseModifier rm) {
			
			ProjectDocumentResponse response = new ProjectDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setUuid(object.getUuid());
	    	response.setName(object.getName());
	    	response.setIdentifier(object.getIdentifier());
	    	response.setPublik(object.isPublik());
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	if (rm != null) {
	    		if (rm.getJoinedUsers() == ResponseFieldType.EXPAND) {
	    			
					UserResponseModifier urm = new UserResponseModifier();
					urm.setRoles(ResponseFieldType.IGNORE);
					
	    			response.setJoinedUsers(((ArrayList<UserResponse>)apiUtils.getMembers(currentUser, new SimpleObjectIdentifier(projectId), ProjectService.this, userService, urm).getData()));
	    		}
	    	}
	    	
	        return response;
		}
		
		@Override
		public String getDescription() {
			return object.getName();
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
	public ProjectContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		ProjectContainer ec = new ProjectContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public ProjectContainer getContainer(UserPrincipal currentUser, ProjectDocument edoc) {
		ProjectContainer ec = new ProjectContainer(currentUser, edoc);

		if (ec.getObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public ProjectDocument create(UserPrincipal currentUser, ProjectUpdateRequest ur) throws Exception {
		ProjectDocument edoc = new ProjectDocument(database);
		
		edoc.setUserId(new ObjectId(currentUser.getId()));
		edoc.setName(ur.getName());
		edoc.setIdentifier(ur.getIdentifier());
		edoc.setPublik(ur.isPublik());

		return create(edoc);
	}


	@Override
	public ListPage<ProjectDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(userId, null, page);
	}

	@Override
	public ListPage<ProjectDocument> getAll(ProjectLookupProperties lp, Pageable page) {
		return getAllByUser(null, lp, page);
	}

	@Override
	public ListPage<ProjectDocument> getAllByUser(ObjectId userId, ProjectLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(projectRepository.find(userId, lp, database.getId()));
		} else {
			return ListPage.create(projectRepository.find(userId, lp, database.getId(), page));
		}
	}

	@Override
	public ProjectLookupProperties createLookupProperties() {
		return new ProjectLookupProperties();
	}


}
