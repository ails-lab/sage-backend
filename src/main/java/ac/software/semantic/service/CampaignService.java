package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Access;
import ac.software.semantic.model.AssigningContainer;
import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.DataDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.User;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.constants.state.PagedAnnotationValidationState;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.constants.type.UserRoleType;
import ac.software.semantic.payload.request.CampaignUpdateRequest;
import ac.software.semantic.payload.response.CampaignResponse;
import ac.software.semantic.payload.response.DatasetProgressResponse;
import ac.software.semantic.payload.response.DatasetResponse;
import ac.software.semantic.payload.response.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.UserResponse;
import ac.software.semantic.payload.response.modifier.CampaignResponseModifier;
import ac.software.semantic.payload.response.modifier.DatasetResponseModifier;
import ac.software.semantic.payload.response.modifier.PagedAnnotationValidationResponseModifier;
import ac.software.semantic.payload.response.modifier.ResponseModifier;
import ac.software.semantic.payload.response.modifier.UserResponseModifier;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.AccessRepository;
import ac.software.semantic.repository.core.CampaignRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationPageLocksRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.container.InverseMemberContainer;
import ac.software.semantic.service.container.MemberContainer;
import ac.software.semantic.service.container.MultipleResponseContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.lookup.CampaignLookupProperties;

@Service
public class CampaignService implements EnclosedCreatableService<Campaign, CampaignResponse, CampaignUpdateRequest, ProjectDocument>, 
                                        LookupService<Campaign, CampaignResponse, CampaignLookupProperties>,
 									   EnclosingLookupService<Campaign, CampaignResponse, ProjectDocument, CampaignLookupProperties> {

	Logger logger = LoggerFactory.getLogger(CampaignService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private AccessRepository accessRepository;
	
	@Autowired
	private CampaignRepository campaignRepository;

	@Autowired
	private PagedAnnotationValidationRepository pavRepository;

	@Autowired
	private PagedAnnotationValidationPageLocksRepository pavplRepository;

	@Autowired
	private PagedAnnotationValidationService pavService;

	@Autowired
	private VocabularyService vocabularyService;

	@Autowired
	private UserService userService;

	@Autowired
	private DatasetService datasetService;

	@Autowired
	private APIUtils apiUtils;
	
	@Autowired
	private ServiceUtils serviceUtils;

	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer vocc;
	
	@Override
	public Class<? extends ObjectContainer<Campaign, CampaignResponse>> getContainerClass() {
		return CampaignContainer.class;
	}
	
	@Override 
	public DocumentRepository<Campaign> getRepository() {
		return campaignRepository;
	}
	
	public class CampaignContainer extends ObjectContainer<Campaign, CampaignResponse> 
	                               implements UpdatableContainer<Campaign, CampaignResponse, CampaignUpdateRequest>,
	                                          MemberContainer<Campaign, CampaignResponse, DataDocument>,
	                                          InverseMemberContainer<Campaign, CampaignResponse, ProjectDocument>,
	                                          MultipleResponseContainer<Campaign, CampaignResponse, CampaignResponseModifier>,
											  AssigningContainer<Campaign, CampaignResponse, Dataset>
	                                          {

		private ObjectId campaignId;
		
		private CampaignContainer(UserPrincipal currentUser, ObjectId campaignId) {
			this.currentUser = currentUser;
			this.campaignId = campaignId;

			load();
		}
		
		private CampaignContainer(UserPrincipal currentUser, Campaign cdoc) {
			this.currentUser = currentUser;
			this.campaignId = cdoc.getId();
			this.object = cdoc;
		}

		private CampaignContainer(UserPrincipal currentUser, Campaign cdoc, ProjectDocument project) { // ???
			this.currentUser = currentUser;
			this.campaignId = cdoc.getId();
			this.object = cdoc;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return campaignId;
		}
		
		@Override 
		public DocumentRepository<Campaign> getRepository() {
			return campaignRepository;
		}
		
		@Override
		public CampaignService getService() {
			return CampaignService.this;
		}
		
		@Override
		public Campaign update(CampaignUpdateRequest ur) throws Exception {
			return update(icc -> {
				Campaign camp = icc.getObject();
				
				camp.setName(ur.getName());
				camp.setState(ur.getState());
			});
		}

		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
				accessRepository.deleteByCampaignId(object.getId());

				campaignRepository.delete(object);
				
				return true;
			}
		}

		@Override
		public CampaignResponse asResponse(CampaignResponseModifier rm) {
	    	CampaignResponse response = new CampaignResponse();
	    	response.setId(object.getId().toString());
	    	response.setName(object.getName());
	    	response.setType(object.getType());
	    	response.setState(object.getState());
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	        
	    	if (rm != null) {
	    		if (rm.getValidators() == ResponseFieldType.EXPAND) {
	    			
					UserResponseModifier irm = new UserResponseModifier();
					irm.setRoles(ResponseFieldType.IGNORE);
					
	    			response.setValidators(((ArrayList<UserResponse>)apiUtils.getMembers(currentUser, new SimpleObjectIdentifier(campaignId), CampaignService.this, userService, irm).getData()));
	    		}
	    		
	    		if (rm.getDatasets() == ResponseFieldType.EXPAND) {
	    			DatasetResponseModifier irm = DatasetResponseModifier.baseModifier();
					
	    			response.setDatasets(((ArrayList<DatasetResponse>)apiUtils.getMembers(currentUser, new SimpleObjectIdentifier(campaignId), CampaignService.this, datasetService, irm).getData()));
//	    		} else if (rm.getAssignedDatasets() == ResponseFieldType.EXPAND) {
//	    			DatasetResponseModifier irm = DatasetResponseModifier.baseModifier();
//	    			
//	    			Set<ObjectId> datasetIds = new HashSet<>();
//	    			
//		    		for (Access acc : accessRepository.findByCampaignIdAndUserIdAndAccessType(campaignId, new ObjectId(currentUser.getId()), UserRoleType.VALIDATOR)) {
//		    			datasetIds.add(acc.getCollectionId());
//		    		}
//
//		    		List<DatasetResponse> datasets = new ArrayList<>();
//		    		for (ObjectId datasetId : datasetIds) {
//		    			DatasetContainer dc = datasetService.getContainer(currentUser, new SimpleObjectIdentifier(datasetId));
//		    			datasets.add(dc.asResponse(irm));
//	    			}
//		    		
//		    		response.setDatasets(datasets);
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

		@Override
		public void removeMember(DataDocument member) throws Exception {
			if (member instanceof User) {
				update(idc -> {
					Campaign camp = idc.getObject();
	
					accessRepository.deleteByCampaignIdAndUserIdAndAccessType(camp.getId(), member.getId(), UserRoleType.VALIDATOR);
	
					camp.removeMember(member);
				});
			} else if (member instanceof Dataset) {
				update(idc -> {
					Campaign camp = idc.getObject();
					camp.removeMember(member);
				});
				
			}
		}
		
		@Override
		public boolean isAssigned(Dataset dataset, User user) {
			return accessRepository.findByCampaignIdAndUserIdAndCollectionId(object.getId(), user.getId(), dataset.getId()).isPresent();
		}
		
		@Override
		public boolean assign(Dataset dataset, User user) {
			Access newEntry = new Access();
            newEntry.setUserId(user.getId());
            newEntry.setDatabaseId(database.getId());
            newEntry.setCampaignId(object.getId());
            newEntry.setCreatorId(new ObjectId(currentUser.getId()));
            newEntry.setCollectionId(dataset.getId());
            newEntry.setCollectionUuid(dataset.getUuid());
            newEntry.setAccessType(UserRoleType.VALIDATOR);
            
            accessRepository.save(newEntry);
            
            return true;
		}
		
		@Override
		public boolean unassign(Dataset dataset, User user) {
	    	Long res = accessRepository.deleteByCampaignIdAndCreatorIdAndUserIdAndCollectionIdAndAccessType(object.getId(), object.getUserId(), user.getId(), dataset.getId(), UserRoleType.VALIDATOR);

		    if (res > 0 ) {
		    	return true;
		    } else {
		    	return false;
		    }
		}
		
		@Override
		public int unassignAll(User user) {
	    	Long res = accessRepository.deleteByCampaignIdAndCreatorIdAndUserIdAndAccessType(object.getId(), object.getUserId(), user.getId(), UserRoleType.VALIDATOR);

	    	return res.intValue();
		}
		
		@Override
		public List<Dataset> getAssigned(User user) {
			List<Dataset> res = new ArrayList<>();

			for (Access acc : accessRepository.findByCampaignIdAndUserIdAndAccessType(object.getId(), user.getId(), UserRoleType.VALIDATOR)) {
				res.add(datasetService.getContainer(currentUser, new SimpleObjectIdentifier(acc.getCollectionId())).getObject());
			}
			
			return res;
		}
		
		public List<PagedAnnotationValidationResponse> getDatasetProgress(ObjectId datasetId) throws Exception {
			
			if (!object.getDatasetId().contains(datasetId)) {
				throw new Exception("Dataset is not member of campaign");
			}
			
			List<PagedAnnotationValidationResponse> res = new ArrayList<>();

			for (PagedAnnotationValidation pav : pavRepository.findByDatasetId(datasetId)) {
				PagedAnnotationValidationContainer pavc = pavService.getContainer(null, pav);
				
				PagedAnnotationValidationResponse pavResponse = pavc.asResponse(ResponseModifier.createModifier(PagedAnnotationValidationResponseModifier.class, Arrays.asList(new String[] { "base", "vocabularies", "progress" } )));
				
				pavResponse.setPropertyName(vocabularyService.onPathStringListAsPrettyString(pav.getOnProperty()));
				pavResponse.setPropertyPath(PathElement.onPathElementListAsStringListInverse(pav.getOnProperty(), vocc));
				
				pavResponse.setLocked(pavplRepository.findByAnnotationEditGroupId(pav.getAnnotationEditGroupId()).size() > 0);
	        	pavResponse.setActive(pav.getLifecycle() == PagedAnnotationValidationState.STARTED && !pavc.isPublished());
	        	
				res.add(pavResponse);
			}

			return res;
		}
	}
	
	@Override
	public Campaign create(UserPrincipal currentUser, ProjectDocument project, CampaignUpdateRequest ur) throws Exception {
		Campaign camp = new Campaign(database);
		camp.setUserId(new ObjectId(currentUser.getId()));
		camp.setName(ur.getName());
		camp.setType(ur.getType());
		camp.setState(ur.getState());
		
		if (project != null) {
			camp.setProjectId(Arrays.asList(new ObjectId[] { project.getId() }));
		}
		
		return create(camp);
	}
	
	@Override
	public CampaignContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		CampaignContainer cc = new CampaignContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (cc.getObject() == null) {
			return null;
		} else {
			return cc;
		}
	}
	
	@Override
	public CampaignContainer getContainer(UserPrincipal currentUser, Campaign object, ProjectDocument project) {
		CampaignContainer cc = new CampaignContainer(currentUser, object, project);
		
		if (cc.getObject() == null) {
			return null;
		} else {
			return cc;
		}
	}

	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}

	@Override
	public ListPage<Campaign> getAll(CampaignLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);
	}

	@Override
	public ListPage<Campaign> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, null, page);
	}

	@Override
	public ListPage<Campaign> getAllByUser(ObjectId userId, CampaignLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<Campaign> getAll(List<ProjectDocument> project, CampaignLookupProperties lp, Pageable page) {
		return getAllByUser(project, null, lp, page);
	}

	@Override
	public ListPage<Campaign> getAllByUser(List<ProjectDocument> project, ObjectId userId, Pageable page) {
		return getAllByUser(project, userId, null, page);
	}

	@Override
	public ListPage<Campaign> getAllByUser(List<ProjectDocument> project, ObjectId userId, CampaignLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(campaignRepository.find(userId, project, lp, database.getId()));
		} else {
			return ListPage.create(campaignRepository.find(userId, project, lp, database.getId(), page));
		}
	}

	@Override
	public CampaignLookupProperties createLookupProperties() {
		return new CampaignLookupProperties();
	}	

}
