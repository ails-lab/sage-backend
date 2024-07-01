package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import ac.software.semantic.config.AppConfiguration.UserTaskScheduler;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.User;
import ac.software.semantic.model.UserTaskDescription;
import ac.software.semantic.model.UserTaskDocument;
import ac.software.semantic.model.base.RunnableDocument;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.request.UserTaskUpdateRequest;
import ac.software.semantic.payload.response.UserTaskResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.core.UserRepository;
import ac.software.semantic.repository.core.UserTaskDocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.RunnableContainer;
import ac.software.semantic.service.container.SchedulableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.lookup.UserTaskLookupProperties;

@Service
public class UserTaskService implements ContainerService<UserTaskDocument,UserTaskResponse>, 
                                        EnclosedCreatableLookupService<UserTaskDocument, UserTaskResponse,UserTaskUpdateRequest, Dataset, UserTaskLookupProperties>,
                                        EnclosingService<UserTaskDocument, UserTaskResponse,Dataset>,
                                        RunningService<UserTaskDocument,UserTaskResponse> {

	private Logger logger = LoggerFactory.getLogger(UserTaskService.class);

    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private UserRepository userRepository;

	@Autowired
	private UserTaskDocumentRepository userTaskRepository;
	
	@Autowired
	private DatasetRepository datasetRepository;
	
	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;
	
    @Autowired
    @Qualifier("user-task-scheduler")
    private UserTaskScheduler userTaskScheduler;

	@Autowired
	private TaskService taskService;

	@Autowired
	private DatasetService datasetService;

	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private ServiceUtils serviceUtils;

	@Override
	public Class<? extends EnclosedObjectContainer<UserTaskDocument,UserTaskResponse,Dataset>> getContainerClass() {
		return UserTaskContainer.class;
	}
	
	@Override
	public DocumentRepository<UserTaskDocument> getRepository() {
		return userTaskRepository;
	}

	public class UserTaskContainer extends EnclosedObjectContainer<UserTaskDocument,UserTaskResponse,Dataset> 
	                               implements UpdatableContainer<UserTaskDocument,UserTaskResponse, UserTaskUpdateRequest>, 
	                                          RunnableContainer<UserTaskDocument,UserTaskResponse>, 
	                                          SchedulableContainer<UserTaskDocument,UserTaskResponse>  {
		private ObjectId userTaskId;
	
		private UserTaskContainer(UserPrincipal currentUser, ObjectId userTaskId) {
			this.currentUser = currentUser;
			
			this.userTaskId = userTaskId;
		
			load();
		}
		
		private UserTaskContainer(UserPrincipal currentUser, UserTaskDocument udoc) {
			this(currentUser, udoc, null);
		}
		
		private UserTaskContainer(UserPrincipal currentUser, UserTaskDocument udoc, Dataset dataset) {
			this.currentUser = currentUser;

			this.userTaskId = udoc.getId();
			this.object = udoc;
			
			this.dataset = dataset;
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return userTaskId;
		}
		
		@Override
		public DocumentRepository<UserTaskDocument> getRepository() {
			return userTaskRepository;
		}

		@Override
		public UserTaskService getService() {
			return UserTaskService.this;
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
		
			setEnclosingObject(datasetOpt.get());
		}

		@Override
		public RunnableDocument getRunDocument() {
			return getObject();
		}
		
		@Override
	    public UserTaskDocument update(UserTaskUpdateRequest ur) throws Exception {

	    	return update(iuc -> {
	    		UserTaskDocument ut = iuc.getObject();
	    		
	   			ut.setName(ur.getName());

	   	    	List<UserTaskDescription> ttasks = new ArrayList<>();
	   	    	for (UserTaskDescription t : ur.getTasks()) {
	   	    		UserTaskDescription utd = new UserTaskDescription();
	   	    		utd.setDatasetId(getEnclosingObject().getId());
	   	    		utd.setType(t.getType());
	   	    		if (t.getGroup() != -1) {
	   	    			utd.setGroup(t.getGroup());
	   	    		}
	   	    		
	   	    		ttasks.add(utd);
	   	    	}
	   	    	ut.setTasks(ttasks);
	   	    	
	   	    	ut.setCronExpression(ur.getCronExpression());
	   	    	ut.setFreshRunOnly(ur.isFreshRunOnly());
	    	});
	    }
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
					
				userTaskRepository.delete(object);
	
				return true;
			}
		}
		
		@Override
		public String localSynchronizationString() {
			return getObject().getId().toString(); // ????
		}

		@Override
		public UserTaskResponse asResponse() {
	    	UserTaskResponse response = new UserTaskResponse();
	    	response.setId(object.getId().toString());
	    	response.setName(object.getName());
	    	
	    	List<UserTaskDescription> list = new ArrayList<>();
	    	if (object.getTasks() != null) {
		    	for (UserTaskDescription utd : object.getTasks()) {
		    		UserTaskDescription d = new UserTaskDescription();
		    		d.setType(utd.getType());
	    			d.setGroup(utd.getGroup() == null ? - 1: utd.getGroup());
		    		list.add(d);
		    	}
	    	}
	    	response.setTasks(list);
	    	response.setCronExpression(object.getCronExpression());
	    	response.setScheduled(object.isScheduled());
	    	response.setFreshRunOnly(object.isFreshRunOnly());

	    	response.copyStates(object, null, fileSystemConfiguration);
	    	
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());

	    	return response;
		}
		
		@Override
		public String getDescription() {
			return object.getName();
		}
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByUserTaskId(getObject().getId(), type).orElse(null);
		}	

		@Override
		public FileSystemConfiguration getContainerFileSystemConfiguration() {
			return fileSystemConfiguration;
		}
		
		public Properties buildPossiblyRelevantProperties() {
			DatasetContainer dc = datasetService.getInDatasetContainer(this);
			
			Properties props = new Properties();
//			props.put(ServiceProperties.PUBLISH_MODE, dc.getEnclosingObject().isPublik() ? ServiceProperties.PUBLISH_MODE_PUBLIC : ServiceProperties.PUBLISH_MODE_PRIVATE);
			props.put(ServiceProperties.METADATA, ServiceProperties.ALL);
			props.put(ServiceProperties.CONTENT, ServiceProperties.ALL);
//			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
			if (dc.getDatasetTripleStoreVirtuosoConfiguration() != null) {
				props.put(ServiceProperties.TRIPLE_STORE, dc.getDatasetTripleStoreVirtuosoConfiguration());
			}
			
			return props;
		}
		
		@Override
		public void schedule() throws Exception {
			UserTaskDocument ut = getObject();
			
			userTaskScheduler.schedule(new ScheduledUserTask(this), ut.getCronExpression());
			
			update(iuc -> {
				UserTaskDocument iut = iuc.getObject();
				iut.setScheduled(true);
			});
		}
		
		@Override
		public void unschedule() throws Exception {
			userTaskScheduler.unschedule(getPrimaryId());

			update(iuc -> {
				UserTaskDocument iut = iuc.getObject();
				iut.setScheduled(false);
			});
		}
		
		@Override
		public boolean isScheduled() {
			return object.isScheduled();
		}

	}
	
	public class ScheduledUserTask implements Runnable {
		private UserTaskContainer uc;
		
		public ScheduledUserTask(UserTaskContainer uc) {
			this.uc = uc;
		}
		
		public UserTaskContainer getUserTaskContainer() {
			return uc;
		}
		
		public void run() {
			try {
				TaskDescription tdescr = TaskSpecification.getTaskSpecification(TaskType.USER_TASK_DATASET_RUN).createTask(uc, uc.buildPossiblyRelevantProperties());
				
				if (tdescr != null) {
		    		taskService.call(tdescr);
		    	}
				
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}

//	@Override
	public UserTaskContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		UserTaskContainer ec = new UserTaskContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public ObjectContainer<UserTaskDocument,UserTaskResponse> getContainer(UserPrincipal currentUser, UserTaskDocument udoc) {
		UserTaskContainer ec = new UserTaskContainer(currentUser, udoc);
		
		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public UserTaskContainer getContainer(UserPrincipal currentUser, UserTaskDocument udoc, Dataset dataset) {
		UserTaskContainer ec = new UserTaskContainer(currentUser, udoc, dataset);

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
    public UserTaskDocument create(UserPrincipal currentUser, Dataset dataset, UserTaskUpdateRequest ur) throws Exception {
		
    	UserTaskDocument ut = new UserTaskDocument(dataset);
    	ut.setUserId(new ObjectId(currentUser.getId()));
    	ut.setName(ur.getName());
    	ut.setFileSystemConfigurationId(fileSystemConfiguration.getId());
    	
    	List<UserTaskDescription> ttasks = new ArrayList<>();
    	for (UserTaskDescription t : ur.getTasks()) {
    		UserTaskDescription utd = new UserTaskDescription();
    		utd.setDatasetId(dataset.getId());
    		utd.setType(t.getType());
    		if (t.getGroup() != -1) {
    			utd.setGroup(t.getGroup());
    		}
    		
    		ttasks.add(utd);
    	}
    	ut.setTasks(ttasks);
    	ut.setCronExpression(ur.getCronExpression());
    	ut.setFreshRunOnly(ur.isFreshRunOnly());
    	
		return create(ut);
    }

//    @Override
// 	public ListPage<UserTaskDocument> getAllByUser(ObjectId userId, Pageable page) {
//    	if (page == null) {
//    		return ListPage.create(userTaskRepository.findByDatabaseIdAndUserIdAndFileSystemConfigurationId(database.getId(), userId, fileSystemConfiguration.getId()));
//    	} else {
//    		return ListPage.create(userTaskRepository.findByDatabaseIdAndUserIdAndFileSystemConfigurationId(database.getId(), userId, fileSystemConfiguration.getId(), page));
//    	}
//	}
//
//    
//    @Override
// 	public ListPage<UserTaskDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
//    	
//    	if (page == null) {
//    		return ListPage.create(userTaskRepository.findByDatasetIdInAndUserIdAndFileSystemConfigurationId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId, fileSystemConfiguration.getId()));
//    	} else {
//    		return ListPage.create(userTaskRepository.findByDatasetIdInAndUserIdAndFileSystemConfigurationId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId, fileSystemConfiguration.getId(), page));
//    	}
//	}
    
	@Override
	public ListPage<UserTaskDocument> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, null, page);
	}

	@Override
	public ListPage<UserTaskDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		return getAllByUser(dataset, userId, null, page);
	}

	@Override
	public ListPage<UserTaskDocument> getAll(UserTaskLookupProperties lp, Pageable page) {
		return getAllByUser(null, null, lp, page);
	}

	@Override
	public ListPage<UserTaskDocument> getAllByUser(ObjectId userId, UserTaskLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<UserTaskDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, UserTaskLookupProperties lp, Pageable page) {
		if (lp == null) {
			lp = new UserTaskLookupProperties();
		}
		
		lp.setFileSystemConfigurationId(fileSystemConfiguration.getId());
		
		if (page == null) {
			return ListPage.create(userTaskRepository.find(userId, dataset, lp, database.getId()));
		} else {
			return ListPage.create(userTaskRepository.find(userId, dataset, lp, database.getId(), page));
		}	
	}
	
 	public void activateScheduled() {
 		UserTaskLookupProperties lp = new UserTaskLookupProperties();
 		lp.setFileSystemConfigurationId(fileSystemConfiguration.getId());
 		
// 		for (UserTaskDocument ut : userTaskRepository.findByDatabaseIdAndFileSystemConfigurationId(database.getId(), fileSystemConfiguration.getId())) {
 		for (UserTaskDocument ut : userTaskRepository.find(null, null, lp, database.getId())) {
 			if (ut.isScheduled()) {
 				try {
 					Optional<User> userOpt = userRepository.findById(ut.getUserId());
 					
					((UserTaskContainer)getContainer(new UserPrincipal(userOpt.get(), null), ut)).schedule();
					logger.error("Autoscheduled " + ut.getName() + ".");
				} catch (Exception e) {
					logger.error("Autoscheduling for " + ut.getName() + " failed.");
					e.printStackTrace();
				}
 			}
 		}
 		
 	}
 	

	@Override
	public Date preRun(TaskDescription tdescr, WebSocketService wsService) {
		return serviceUtils.preRun(tdescr, wsService);
	}

	@Override
	public Date postRunSuccess(TaskDescription tdescr, WebSocketService wsService) {
		return serviceUtils.postRunSuccess(tdescr, wsService);
	}
	
	@Override
	public Date postRunFail(TaskDescription tdescr, WebSocketService wsService) {
		return serviceUtils.postRunFail(tdescr, wsService);
	}

	@Override
	public UserTaskLookupProperties createLookupProperties() {
		return new UserTaskLookupProperties();
	}

}
