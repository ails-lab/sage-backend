package ac.software.semantic.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import javax.annotation.PostConstruct;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.CatalogTemplateResult;
import ac.software.semantic.model.MappingTemplateResult;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.DatasetType;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.constants.MappingType;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.model.constants.TaskState;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.payload.CreateDatasetDistributionRequest;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.Template;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.EmbedderDocumentRepository;
import ac.software.semantic.repository.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.TaskRepository;
import ac.software.semantic.repository.TemplateRepository;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;

@Service
public class TaskService {

	Logger logger = LoggerFactory.getLogger(TaskService.class);
	
	private Map<ObjectId, TaskDescription> runningTasks;

	@Autowired
	private Environment environment;

    @Autowired
    @Qualifier("system-mac-address")
    private String mac;
    
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private MappingRepository mappingRepository;

	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;

	@Autowired
	private EmbedderDocumentRepository embedderRepository;

	@Autowired
	private PagedAnnotationValidationRepository pavRepository;

	@Autowired
	private FilterAnnotationValidationRepository favRepository;

	@Autowired
    private AnnotatorService annotatorService;

	@Autowired
    private EmbedderService embedderService;

	@Autowired
    private PagedAnnotationValidationService pavService;

	@Autowired
    private FilterAnnotationValidationService favService;

	@Lazy
	@Autowired
	private DatasetService datasetService;

	@Lazy
	@Autowired
	private MappingsService mappingsService;
	
	@Autowired
	private TemplateService templateService;

	@Autowired
	private TemplateRepository templateRepository;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private WebSocketService wsService;
	
	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;
	
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> tsConfigurations;
    
    private Map<TripleStoreConfiguration, DataQueue> tripleStoreTaskQueue;
    
	@Autowired
	private FolderService folderService;
	
	private class TaskQueueObject {
		private TaskDescription tdescr;
		private List<TaskDescription> rest;
		
		TaskQueueObject(TaskDescription tdescr, List<TaskDescription> rest) {
			this.tdescr = tdescr;
			this.rest = rest;
		}
		
		public TaskDescription getTaskDescription() {
			return tdescr;
		}

		public List<TaskDescription> getNextTaskDescriptions() {
			return rest;
		}

	}
	
	public TaskService() {
		runningTasks = new LinkedHashMap<>();
	}
	
    @PostConstruct
    private void postConstruct() {
		tripleStoreTaskQueue = new HashMap<>();
		for (TripleStoreConfiguration ts : tsConfigurations.values()) {
			DataQueue queue = new DataQueue(1);
			tripleStoreTaskQueue.put(ts, queue);
			
		    Thread thread = new Thread(new Consumer(queue));
		    logger.info("Created publication worker thread for " + ts.getName());
		    thread.start();
		}
	}
	
	private void cascadeFailUnfinishedTask(TaskDescription t) {
		if (t.getType() == TaskType.ANNOTATOR_EXECUTE) {
			Optional<AnnotatorDocument> adocOpt = annotatorRepository.findById(t.getAnnotatorId());
			if (adocOpt.isPresent()) {
				AnnotatorDocument adoc = adocOpt.get();
				MappingExecuteState es = adoc.checkExecuteState(t.getFileSystemConfigurationId());
				if (es != null) {
					es.fail(t.getEndTime(), "System has been restarted.");
					annotatorRepository.save(adoc);
				}
			}
		} else if (t.getType() == TaskType.EMBEDDER_EXECUTE) {
			Optional<EmbedderDocument> edocOpt = embedderRepository.findById(t.getEmbedderId());
			if (edocOpt.isPresent()) {
				EmbedderDocument edoc = edocOpt.get();
				MappingExecuteState es = edoc.checkExecuteState(t.getFileSystemConfigurationId());
				if (es != null) {
					es.fail(t.getEndTime(), "System has been restarted.");
					embedderRepository.save(edoc);
				}
			}
		} else if (t.getType() == TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE) {
			Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(t.getPagedAnnotationValidationId());
			if (pavOpt.isPresent()) {
				PagedAnnotationValidation pav = pavOpt.get();
				MappingExecuteState es = pav.checkExecuteState(t.getFileSystemConfigurationId());
				if (es != null) {
					es.fail(t.getEndTime(), "System has been restarted.");
					pavRepository.save(pav);
				}
			}			
		} else if (t.getType() == TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE) {
			Optional<FilterAnnotationValidation> favOpt = favRepository.findById(t.getFilterAnnotationValidationId());
			if (favOpt.isPresent()) {
				FilterAnnotationValidation fav = favOpt.get();
				MappingExecuteState es = fav.checkExecuteState(t.getFileSystemConfigurationId());
				if (es != null) {
					es.fail(t.getEndTime(), "System has been restarted.");
					favRepository.save(fav);
				}
			}				
		} else if (t.getType() == TaskType.MAPPING_EXECUTE) {
			Optional<MappingDocument> mdocOpt = mappingRepository.findById(t.getMappingId());
			if (mdocOpt.isPresent()) {
				MappingDocument mdoc = mdocOpt.get();
				MappingInstance mi = mappingsService.findMappingInstance(mdoc, t.getMappingInstanceId() != null ? t.getMappingInstanceId().toString() : null);
				if (mi != null) {
					MappingExecuteState es = mi.checkExecuteState(t.getFileSystemConfigurationId());
					if (es != null) {
						es.fail(t.getEndTime(), "System has been restarted.");
						mappingRepository.save(mdoc);
					}
				}
			}
		} else if (t.getType() == TaskType.DATASET_INDEX || t.getType() == TaskType.DATASET_UNINDEX) {
			Optional<Dataset> datasetOpt = datasetRepository.findById(t.getDatasetId());
			if (datasetOpt.isPresent()) {
				Dataset dataset = datasetOpt.get();
				IndexState is = dataset.checkIndexState(t.getElasticConfigurationId());
				if (is != null) {
					is.fail(t.getEndTime(), "System has been restarted.");
					datasetRepository.save(dataset);
				}
			}	
		}
	}
	
	
	public void failUnfinishedTasks() {
		String system = mac + ":" + environment.getProperty("local.server.port");
		
		for (TaskDescription t : taskRepository.findByDatabaseIdAndStateAndSystem(database.getId(), TaskState.STARTED, system)) {
			t.setState(TaskState.FAILED);
			t.setEndTime(new Date());
			taskRepository.save(t);
			
			cascadeFailUnfinishedTask(t);
		}
		
		for (TaskDescription t : taskRepository.findByDatabaseIdAndStateAndSystem(database.getId(), TaskState.STOPPING, system)) {
			t.setState(TaskState.FAILED);
			t.setEndTime(new Date());
			taskRepository.save(t);
			
			cascadeFailUnfinishedTask(t);
			
		}
		for (TaskDescription t : taskRepository.findByDatabaseIdAndStateAndSystem(database.getId(), TaskState.QUEUED, system)) {
			t.setState(TaskState.CANCELED);
			t.setEndTime(new Date());
			taskRepository.save(t);
			
			cascadeFailUnfinishedTask(t);
		}
	}
	
	public List<TaskDescription> getTasks(String userId, boolean parent) {
		return taskRepository.findByUserIdAndDatabaseIdAndParentIdExistsOrderByCreateTimeDesc(new ObjectId(userId), database.getId(), parent);
	}	

	public List<TaskDescription> getTasks(String userId, Date from, boolean parent) {
		if (from == null) {
			return taskRepository.findByUserIdAndDatabaseIdAndParentIdExistsOrderByCreateTimeDesc(new ObjectId(userId), database.getId(), parent);
		} else {
			return taskRepository.findByUserIdAndDatabaseIdAndParentIdExistsAndCreateTimeGreaterThanOrderByCreateTimeDesc(new ObjectId(userId), database.getId(), parent, from);
		}
	}	

	private TaskDescription newTask(TaskDescription tdescr) {
		return newTask(tdescr, null);
	}

	private TaskDescription newTask(TaskDescription tdescr, TaskDescription parent) {
//		System.out.println("NEW TASK " + tdescr.getType() + " " + tdescr.getDescription() + " " + parent);
		tdescr.setDatabaseId(database.getId());
		tdescr.setSystem(mac + ":" + environment.getProperty("local.server.port"));
		tdescr.setUuid(UUID.randomUUID().toString());
    	tdescr.setCreateTime(new Date(System.currentTimeMillis()));
		tdescr.setState(TaskState.QUEUED);

		tdescr.createMonitor(wsService);
		
		if (parent != null) {
			tdescr.setParentId(parent.getId());
		}
    	
    	tdescr = taskRepository.save(tdescr);
    	
		runningTasks.put(tdescr.getId(), tdescr);
		
		if (parent != null) {
			parent.addChild(tdescr);
		}
		
		return tdescr;
	}
	
	private void cancelNewTask(TaskDescription tdescr) {
		runningTasks.remove(tdescr.getId());
		taskRepository.deleteById(tdescr.getId());
		
		if (tdescr.getChildrenIds() != null) {
			for (ObjectId childId : tdescr.getChildrenIds()) {
				runningTasks.remove(childId);
				taskRepository.deleteById(childId);
			}
		}
	}
	
	public void taskTerminated(TaskDescription tdescr, Date date) {
		tdescr.setEndTime(date);
		taskRepository.save(tdescr);
		
		runningTasks.remove(tdescr.getId());
	}

	public void taskFailed(TaskDescription tdescr, Date date) {
		tdescr.setState(TaskState.FAILED);
		tdescr.setEndTime(date);
		taskRepository.save(tdescr);
		
		runningTasks.remove(tdescr.getId());
	}

	public void taskCanceled(TaskDescription tdescr, Date date) {
		tdescr.setState(TaskState.CANCELED);
		tdescr.setEndTime(date);
		taskRepository.save(tdescr);
		
		runningTasks.remove(tdescr.getId());
	}

	public void taskStarted(TaskDescription tdescr) {
		taskStarted(tdescr, new Date());
	}

	public void taskStarted(TaskDescription tdescr, Date date) {
		tdescr.setState(TaskState.STARTED);
		tdescr.setStartTime(date);
		
		taskRepository.save(tdescr);
	}
	
	public void setTask(TaskDescription tdescr, ListenableFuture<?> task) {
		setTask(tdescr, task, null);
	}
	
	public void setTask(TaskDescription tdescr, ListenableFuture<?> task, List<TaskDescription> rest) {
		tdescr.setTask(task);
		
		task.addCallback(new TaskListenableFutureCallback(tdescr, rest));
	}

	public boolean requestStop(ObjectId taskId) {
		TaskDescription tdescr = runningTasks.get(taskId);
		if (tdescr != null) {
			return tdescr.requestStop();
		} else {
			return false;
		}
	}

	private class DataQueue {
	    private final Queue<TaskQueueObject> queue = new LinkedList<>();
	    private final int maxSize;
	    private final Object EMPTY_QUEUE = new Object();

	    DataQueue(int maxSize) {
	        this.maxSize = maxSize;
	    }

	    public boolean isEmpty() {
    		return queue.isEmpty();
	    }

	    public void waitOnEmpty() throws InterruptedException {
        	synchronized (EMPTY_QUEUE) {
        		while (isEmpty()) {
                    EMPTY_QUEUE.wait();
        		}
            }
        }

	    public void notifyAllForEmpty() {
	        synchronized (EMPTY_QUEUE) {
	            EMPTY_QUEUE.notifyAll();
	        }
	    }

	    public boolean add(TaskQueueObject message) {
	        synchronized (queue) {
	        	if (queue.size() == maxSize) {
	        		return false;
	        	} else {
	        		return queue.add(message);
	        	}
	        }
	    }

	    public TaskQueueObject poll() {
	        synchronized (queue) {
	            return queue.poll();
	        }
	    }
	    
	    public TaskQueueObject peek() {
	        synchronized (queue) {
	            return queue.peek();
	        }
	    }

	}

	public class Consumer implements Runnable {
	    private final DataQueue dataQueue;
	    private volatile boolean runFlag;

	    public Consumer(DataQueue dataQueue) {
	        this.dataQueue = dataQueue;
	        runFlag = true;
	    }

	    @Override
	    public void run() {

	    	while (runFlag) {
				try {	  
					dataQueue.waitOnEmpty();
	            } catch (InterruptedException ex) {
	            	ex.printStackTrace();
	            	
	            	// ????	            	
	            	break;
	            }
				
	            if (!runFlag) {
	                break;
	            }
	            
	            TaskQueueObject taskDescObj = dataQueue.peek();
	            TaskDescription tdescr = taskDescObj.getTaskDescription();
	            
				try {
//					System.out.println("STARTING TASK QUEUE " + tdescr.getId() + " " + tdescr.toString());
					
					TaskType type = tdescr.getType();
					
					if (type == TaskType.DATASET_PUBLISH) {
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						ListenableFuture<Date> task = datasetService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.ANNOTATOR_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						ListenableFuture<Date> task = annotatorService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.EMBEDDER_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						ListenableFuture<Date> task = embedderService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						ListenableFuture<Date> task = pavService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						ListenableFuture<Date> task = favService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					}
				
				} catch (Exception ex) {
					// should never be here
					logger.error("Error creating postponed task. Should never happen!");
					ex.printStackTrace();
					
					dataQueue.poll();
					continue;
				}				
				
				try {
					synchronized (tdescr) {
						while (tdescr.getState() != TaskState.COMPLETED && tdescr.getState() != TaskState.FAILED) {
							tdescr.wait();
						}
					
//	            		System.out.println("FINISHED AND REMOVING FROM QUEUE " + tdescr.getId() + " " + tdescr.toString());
	            		
		            	dataQueue.poll();
	            	}

	            } catch (InterruptedException ex) {
	            	ex.printStackTrace();
	            	
	            	// ????
	            	break;
	            }
	        }
	    }


	    public void stop() {
	        runFlag = false;
	        dataQueue.notifyAllForEmpty();
	    }
	}
	
	private class TaskListenableFutureCallback<V> implements ListenableFutureCallback<Date> {
		TaskDescription tdescr;
		List<TaskDescription> rest;
		
		public TaskListenableFutureCallback(TaskDescription tdescr, List<TaskDescription> rest) {
			this.tdescr = tdescr;
			this.rest = rest;
		}
		
		@Override
		public void onSuccess(Date comletedAt) {
//			System.out.println("C >>>  COMPETED " + " " + tdescr.getType() + " " + tdescr.getDescription() + " " +tdescr.getParentId());
				
			tdescr.setState(TaskState.COMPLETED);
			taskTerminated(tdescr, comletedAt);

			if (tdescr.getMonitor() != null) {
				try {
					tdescr.getMonitor().close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
//			System.out.println("SUCCESS NOTIFYING " + tdescr.getId() + " " + tdescr.toString());
			synchronized (tdescr) {
				tdescr.notifyAll();
			}
				
			if (tdescr.getPostTaskExecution() != null) {
				try {
					tdescr.getPostTaskExecution().onComplete();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (rest != null && rest.size() > 0) {
				TaskDescription next = rest.remove(0);
				call(next, rest);
			} else {
				
				ObjectId parent = tdescr.getParentId();
				if (parent != null && (rest == null || rest.size() == 0)) {
					TaskDescription parentDescr = runningTasks.get(parent);
					
					parentDescr.setState(TaskState.COMPLETED);
					taskTerminated(parentDescr, tdescr.getEndTime());
				}
			}
		}
	
		@Override // does not allow throw the exception to be catched by execution monitor 
		public void onFailure(Throwable ex) {
			try {
	//			System.out.println("C >>>  FAILED" + ex);
				
	//			System.out.println("FAILURE NOTIFYING " + tdescr.getId() + " " + tdescr.toString());
				
				Date failureTime = new Date(System.currentTimeMillis());
				
				if (ex instanceof CancellationException) {
					tdescr.setState(TaskState.STOPPED);
				} else if (ex instanceof TaskFailureException) {
					tdescr.setState(TaskState.FAILED);
					failureTime = ((TaskFailureException)ex).getFailureTime();
				} else {
					ex.printStackTrace();
					tdescr.setState(TaskState.FAILED);
				}
				
				taskTerminated(tdescr, failureTime);
				abandonTask(tdescr, rest, failureTime);
			
			} finally {
				if (tdescr.getMonitor() != null) {
					try {
						tdescr.getMonitor().close();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}				
			}
		}
	}
	
	private void abandonTask(TaskDescription tdescr, List<TaskDescription> rest, Date completedAt) { // assumes one level children only ? 
		if (rest != null) {
			for (TaskDescription next : rest) {
				taskCanceled(next, completedAt);
			}
		}
		
		// fail parent
		ObjectId parent = tdescr.getParentId();
		if (parent != null) { 
			TaskDescription parentDescr = runningTasks.get(parent);
			parentDescr.setState(tdescr.getState());
			
			taskTerminated(parentDescr, tdescr.getEndTime());
		}
	}
	
	
	private void queueTripleStoreTask(TaskDescription tdescr, List<TaskDescription> rest) {
		DatasetContainer dc = datasetService.getInDatasetContainer(tdescr.getContainer());
		
		TripleStoreConfiguration ts = dc.getTripleStoreConfiguration();
		if (ts == null) { // this shoule happen only in dataset publish 
			ts = (TripleStoreConfiguration)tdescr.getProperties().get(ServiceProperties.TRIPLE_STORE);
		}
		
		DataQueue list = tripleStoreTaskQueue.get(ts);

		TaskMonitor tm = tdescr.getMonitor();
		NotificationType type = tdescr.getTaskSpecification().getNotificationType();

		if (list.add(new TaskQueueObject(tdescr, rest))) {
			String state = tdescr.getTaskSpecification().getQueuedStateAsString();

			if (tm != null && type != null && state != null) {
				NotificationObject no = NotificationObject.createNotificationObject(type, state, tdescr.getContainer());
				if (no != null) {
					tdescr.getMonitor().sendMessage(no);
				}
			}
			
//			System.out.println("ADDED TO QUEUE" + tdescr.getId() + " " + tdescr.toString());
			
			list.notifyAllForEmpty();
		} else {
			String state = tdescr.getTaskSpecification().getCancelledStateAsString();
			Date completedAt = new Date();
			
			if (tm != null) {
				try {
					tm.complete();
					completedAt = tm.getCompletedAt();
					tm.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
					
			if (tm != null && type != null && state != null) {
				NotificationObject no = NotificationObject.createNotificationObject(type, state, tdescr.getContainer());
				if (no != null) {
					tdescr.getMonitor().sendMessage(no);
				}
			}

			
			// here should cancel task because queue limit exceeded
			taskCanceled(tdescr, completedAt);
			abandonTask(tdescr, rest, completedAt);
		}
	}
	
	private void callSingleTask(TaskDescription tdescr, List<TaskDescription> rest) {
		
//		System.out.println("CALLING SINGLE " + tdescr.getType() + " " + tdescr.getDescription());
		TaskType type = tdescr.getType();
		
		if (type == TaskType.DATASET_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.DATASET_UNPUBLISH) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = datasetService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating dataset unpublish task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.DATASET_INDEX) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = datasetService.index(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating dataset index task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.DATASET_UNINDEX) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = datasetService.unindex(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating dataset unindex task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.DATASET_FLIP_VISIBILITY) {
			taskStarted(tdescr);
			try {
			   	ListenableFuture<Date> task = datasetService.flipVisibility(tdescr, (DatasetContainer)tdescr.getContainer(), wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating dataset change visibility task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.DATASET_CREATE_DISTRIBUTION) {
			taskStarted(tdescr);
			try {
			   	ListenableFuture<Date> task = datasetService.createDistribution(tdescr, (DatasetContainer)tdescr.getContainer(), (CreateDatasetDistributionRequest)tdescr.getProperties().get(ServiceProperties.CREATE_DISTRIBUTION_OPTIONS), wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating create distribution execute task");
				ex.printStackTrace();
			}				

		} else if (type == TaskType.MAPPING_EXECUTE) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = mappingsService.execute(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating mapping execute task");
				ex.printStackTrace();
			}

		
		} else if (type == TaskType.ANNOTATOR_EXECUTE) {
			taskStarted(tdescr);
			try {
			   	ListenableFuture<Date> task = annotatorService.execute(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating annotator execute task");
				ex.printStackTrace();
			}		
		} else if (type == TaskType.ANNOTATOR_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.ANNOTATOR_UNPUBLISH) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = annotatorService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating annotator unpublish task");
				ex.printStackTrace();
			}			

		
		} else if (type == TaskType.EMBEDDER_EXECUTE) {
			taskStarted(tdescr);
			try {
			   	ListenableFuture<Date> task = embedderService.execute(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating embedder execute task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.EMBEDDER_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.EMBEDDER_UNPUBLISH) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = embedderService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating embedder unpublish task");
				ex.printStackTrace();
			}				

		
		} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_RESUME) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = pavService.resumeValidation(tdescr, (PagedAnnotationValidationContainer)tdescr.getContainer(), wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating paged annotation validation resume task");
				ex.printStackTrace();
			}		   		
		} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = pavService.execute(tdescr, wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating paged annotation validation execute task");
				ex.printStackTrace();
			}		   		
		} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = pavService.unpublish(tdescr, wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating paged annotation validation unpublish task");
				ex.printStackTrace();
			}	
			
		} else if (type == TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = favService.execute(tdescr, wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating filter annotation validation execute task");
				ex.printStackTrace();
			}		   		
		} else if (type == TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH) {
			taskStarted(tdescr);
			try {
				ListenableFuture<Date> task = favService.unpublish(tdescr, wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating filter annotation validation unpublish task");
				ex.printStackTrace();
			}				
		}		
	}
	
	public void call(TaskDescription tdescr) {
		call(tdescr, new ArrayList<>());
	}

	public void call(TaskDescription tdescr, List<TaskDescription> rest) {
		
//		System.out.println("CALLING " + tdescr.getType() + " " + tdescr.getDescription());
		
		if (tdescr.isGroup()) {
//			taskStarted(tdescr, tdescr.getStartTime());
			taskStarted(tdescr, new Date(System.currentTimeMillis()));
			
			TaskDescription tdescr1 = tdescr.getChildren().get(0);
			
			rest.addAll(tdescr.getChildren());
			rest.remove(0);
			
			call(tdescr1, rest);
			
		} else {
			callSingleTask(tdescr, rest);
		}
	}

	public interface PostTaskExecution {
		public void onComplete() throws Exception; 
	}
	
	private class CatalogTemplateHeaderPostTaskExecution implements PostTaskExecution {
		private TaskDescription parent;
		private DatasetContainer dc;
		private MappingContainer mc;
		
		public CatalogTemplateHeaderPostTaskExecution(TaskDescription parent, DatasetContainer dc, MappingContainer mc) {
			this.parent = parent;
			this.dc = dc;
			this.mc = mc;
		}
		
		@Override
		public void onComplete() throws Exception {
			ExecuteState es = mc.getMappingInstance().getExecuteState(fileSystemConfiguration.getId());

			if (es.getExecuteState() != MappingState.EXECUTED) {
				throw new Exception("Task not executed exception."); // this shouldn't happen
			}

			ObjectMapper mapper = new ObjectMapper();

			File file = folderService.getMappingExecutionTxtFile(mc.getCurrentUser(), mc.getDataset(), mc.getMappingDocument(), mc.getMappingInstance(), es, 0);
			
			List<CatalogTemplateResult> datasetTemplates = new ArrayList<>();
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = reader.readLine()) != null) {
//					System.out.println(line);
					datasetTemplates.add(mapper.readValue(line, CatalogTemplateResult.class));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			for (CatalogTemplateResult ctr : datasetTemplates) {
				Template datasetTemplate = templateService.getDatasetImportTemplate(ctr.getTemplate());
				
//				System.out.println("MAPPINGS " + ctr.getMappings());
				
				Dataset dataset = datasetService.createDataset(dc.getCurrentUser(), ctr.getName(), null, 
			           dc.getDataset().isPublik(), 
//			           dc.getTripleStore(), 
			           DatasetScope.COLLECTION, DatasetType.DATASET, SEMAVocabulary.DataCollection.toString(),
					   null,
					   null, datasetTemplate.getId(), ctr.getBindings());
				
				dc.getDataset().addDataset(dataset.getId());
				datasetRepository.save(dc.getDataset());
				
//				DatasetContainer newdc = datasetService.getContainer(dc.getCurrentUser(), dataset.getId().toString(), dc.getTripleStore());
				DatasetContainer newdc = datasetService.getContainer(dc.getCurrentUser(), dataset.getId().toString());
				
				TaskDescription task = newTemplateDatasetTask(newdc, parent);
				task.getChildren().get(task.getChildren().size() - 1).setPostTaskExecution(new CatalogTemplateMappingsPostTaskExecution(parent, newdc, ctr));
				call(task);
			}
			
			taskRepository.save(parent);
		}
	}	

	
	private class CatalogTemplateMappingsPostTaskExecution implements PostTaskExecution {
		private TaskDescription parent;
		private DatasetContainer dc;
		private CatalogTemplateResult ctr;
		
		public CatalogTemplateMappingsPostTaskExecution(TaskDescription parent, DatasetContainer dc, CatalogTemplateResult ctr) {
			this.parent = parent;
			this.dc = dc;
			this.ctr = ctr;
		}
		
		@Override
		public void onComplete() throws Exception {
//			System.out.println("CC ON COMPLETE");
			
	     	for (MappingTemplateResult mtd : ctr.getMappings()) {
	     		MappingDocument mapping = dc.getMapping(mtd.getTemplate());

//				MappingInstance mi = mappingsService.createParameterBinding(dc.getCurrentUser(), mapping.getId().toString(), mtd.getBindings());
	     		MappingContainer mc = mappingsService.getContainer(dc, mapping.getId().toString());
	     		MappingInstance mi = mappingsService.createParameterBinding(mc, mtd.getBindings());
				
//				next lines are to execute the mappings, better no
				
//				MappingContainer mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
//				TaskDescription task = newMappingExecuteTask(mc, parent);
//				call(task);
			}
			
			taskRepository.save(parent);
		}
	}	

	
	public TaskDescription newTemplateDatasetTask(DatasetContainer dc) throws Exception {
		return newTemplateDatasetTask(dc, null);
	}
	
	public TaskDescription newTemplateDatasetTask(DatasetContainer dc, TaskDescription parent) throws Exception {
//		System.out.println("newTemplateDatasetTask " + dc.getDatasetId());

		Dataset ds = dc.getDataset();
		
		TaskDescription tdescr;
		
		if (ds.getDatasetType() == DatasetType.CATALOG) {
			tdescr = new TaskDescription(dc,TaskType.CATALOG_IMPORT_BY_TEMPLATE);
		} else {
			tdescr = new TaskDescription(dc,TaskType.DATASET_IMPORT_BY_TEMPLATE);
		}

		tdescr = newTask(tdescr, parent);

		if (ds.getDatasetType() == DatasetType.DATASET) { 
			// Step 2: Metadata Mapping + Parameter Binding + Metadata Mapping Execution
			for (Template t : templateService.getDatasetImportTemplateHeaders(ds.getTemplateId())) {
	
				String mappingUuid = UUID.randomUUID().toString();
				
//				System.out.println("H M UUID " + mappingUuid);
				String d2rml = templateService.getEffectiveTemplateString(t);
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(mappingUuid).toString());
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_DATASET_URI@@}", resourceVocabulary.getDatasetAsResource(ds.getUuid()).toString());
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_ITEM_BASE@@}", resourceVocabulary.getItemBaseResource().toString());

				List<ParameterBinding> list  = new ArrayList<>();
				if (ds.getBinding() != null) { // this should be the case !
					for (ParameterBinding ep : ds.getBinding()) {
						list.add(ep);
					}
				}
				
				// Need to replace stuff in d2rml template string DATASET_UUID, MAPPING_UUID
				MappingDocument mapping = mappingsService.create(dc.getCurrentUser(), ds.getId().toString(), MappingType.HEADER, t.getName(), t.getParameterNames(), null, d2rml, mappingUuid, t);
//				
//				MappingInstance mi = mappingsService.createParameterBinding(dc.getCurrentUser(), mapping.getId().toString(), list);
	     		MappingContainer mc = mappingsService.getContainer(dc, mapping.getId().toString());
	     		MappingInstance mi = mappingsService.createParameterBinding(mc, list);

//				MappingContainer mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
	     		mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
				dc.addMapping(mapping.getName(), mapping);
				
//				newTask(newMappingExecuteTask(mc), tdescr);
				newMappingExecuteTask(mc, tdescr);
			}
	
			// Step 3: Create Mapping for query and collection
			for (Template t : templateService.getDatasetImportTemplateContents(ds.getTemplateId())) {
	
				String mappingUuid = UUID.randomUUID().toString();
	
//				System.out.println("H C UUID " + mappingUuid);
				String d2rml = templateService.getEffectiveTemplateString(t);
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(mappingUuid).toString());
				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_ITEM_BASE@@}", resourceVocabulary.getItemBaseResource().toString());

				MappingDocument mapping = mappingsService.create(dc.getCurrentUser(), ds.getId().toString(), MappingType.CONTENT, t.getName(), t.getParameterNames(), null, d2rml, mappingUuid, t);
				dc.addMapping(mapping.getName(), mapping);

			}
			
		} else if (ds.getDatasetType() == DatasetType.CATALOG) {

			Template template = templateRepository.findById(ds.getTemplateId()).get();
			
//			System.out.println("TEMPLATE " + template.getId());
			
			String mappingUuid = UUID.randomUUID().toString();

			String d2rml = templateService.getEffectiveTemplateString(template);
			d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(mappingUuid).toString());
			
			List<ParameterBinding> list  = new ArrayList<>();
			if (ds.getBinding() != null) { // this should be the case !
				for (ParameterBinding ep : ds.getBinding()) {
					list.add(ep);
				}
			}
			
			MappingDocument mapping = mappingsService.create(dc.getCurrentUser(), ds.getId().toString(), MappingType.CATALOG, template.getName(), template.getParameterNames(), null, d2rml, mappingUuid, template);			
//			MappingInstance mi = mappingsService.createParameterBinding(dc.getCurrentUser(), mapping.getId().toString(), list);
     		MappingContainer mc = mappingsService.getContainer(dc, mapping.getId().toString());
     		MappingInstance mi = mappingsService.createParameterBinding(mc, list);

//			MappingContainer mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
     		mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
			
			TaskDescription task = newMappingExecuteTask(mc, tdescr);
			task.setPostTaskExecution(new CatalogTemplateHeaderPostTaskExecution(tdescr, dc, mc));
		}

		//save again to store children
		taskRepository.save(tdescr);
		
		return tdescr;
	}	
	
	
	public TaskDescription newTemplateDatasetUpdateTask(DatasetContainer dc) {
		return newTemplateDatasetUpdateTask(dc, null);
	}
	
	public TaskDescription newTemplateDatasetUpdateTask(DatasetContainer dc, TaskDescription parent) {
//		System.out.println("newTemplateDatasetTask " + dc.getDatasetId());
		
		TaskDescription tdescr = new TaskDescription();
		
		Dataset ds = dc.getDataset();
		
		tdescr.setType(TaskType.DATASET_TEMPLATE_UPDATE);
		tdescr.setUserId(new ObjectId(dc.getCurrentUser().getId()));
		tdescr.setDatasetId(dc.getDataset().getId());
		tdescr.setDescription(dc.getDataset().getName());	
		
		//save to get id
		tdescr = newTask(tdescr, parent);

		if (ds.getDatasetType() == DatasetType.DATASET) { 
			for (Template t : templateService.getDatasetImportTemplateHeaders(ds.getTemplateId())) {

				MappingDocument mapping = mappingRepository.findByDatasetIdAndName(dc.getDatasetId(), t.getName()).get();
				
				if (mapping.getType() == MappingType.HEADER) {
					for (MappingInstance mi : mapping.getInstances()) {
						for (ParameterBinding pb : mi.getBinding()) {
							if (pb.getName().equals("TITLE")) { // hardcoded: assume TITLE is dataset metadata title
								pb.setValue(dc.getDataset().getName());
							}
						}

						mappingRepository.save(mapping);
						
						MappingContainer mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
						try {
							newMappingExecuteTask(mc, tdescr);
						} catch (TaskConflictException e) {
							// what happens here ????
							e.printStackTrace();
						}
					}
				}
			}
	
		} else if (ds.getDatasetType() == DatasetType.CATALOG) {

			// ???? the same ?
			
		}

		//save again to store children
		taskRepository.save(tdescr);
		
		return tdescr;
	}
	
	private void scheduleMappingsExecution(TaskDescription parent, DatasetContainer dc) throws TaskConflictException {
		for (MappingDocument mapping : datasetService.getMappings(dc.getCurrentUser(), dc.getDatasetId())) {
			if (mapping.hasParameters()) { 
				for (MappingInstance mi : mapping.getInstances()) {
					MappingContainer mc = mappingsService.getContainer(dc, mapping.getId().toString(), mi.getId().toString());
					
			    	synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
			    		try {
			    			checkIfActiveMappingExecuteTask(mc, parent);
			    		} catch (TaskConflictException ex) {
			    			throw new TaskConflictException("An instance of mapping '" + mc.getMappingDocument().getName() + "' is currently being executed");
			    		}
			    		
		    			newMappingExecuteTask(mc, parent);
			    	}
				}
			} else {
				MappingContainer mc = mappingsService.getContainer(dc, mapping.getId().toString(), null);
				
				synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
		    		try {
		    			checkIfActiveMappingExecuteTask(mc, parent);
		    		} catch (TaskConflictException ex) {
		    			throw new TaskConflictException("Mapping " + mc.getMappingDocument().getName() + " is currently being executed");
		    		}

	    			newMappingExecuteTask(mc, parent);
		    	}
			}
		}	
	}

	
	public TaskDescription newDatasetFlipVisibilityTask(DatasetContainer dc) {
		return newTask(new TaskDescription(dc, TaskType.DATASET_FLIP_VISIBILITY));
	}
	
	public TaskDescription newDatasetIndexTask(DatasetContainer dc, Properties props) throws TaskConflictException {
		return newTask(new TaskDescription(dc, TaskType.DATASET_INDEX, props)); 
	}

	public TaskDescription newDatasetUnindexTask(DatasetContainer dc) throws TaskConflictException {
		return newTask(new TaskDescription(dc, TaskType.DATASET_UNINDEX)); 
	}
	
	public TaskDescription newPagedAnnotationValidationResumeTask(PagedAnnotationValidationContainer pavc) throws TaskConflictException {
		return newPagedAnnotationValidationResumeTask(pavc, null);
	}
	
	public TaskDescription newPagedAnnotationValidationResumeTask(PagedAnnotationValidationContainer pavc, TaskDescription parent) throws TaskConflictException {
		synchronized (pavc.synchronizationString(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME)) {
			checkIfActivePagedAnnotationValidationTask(pavc, parent);
//			
			return newTask(new TaskDescription(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME), parent);
		}
	}

	public TaskDescription newPagedAnnotationValidationExecuteTask(PagedAnnotationValidationContainer pavc) throws TaskConflictException {
		return newPagedAnnotationValidationExecuteTask(pavc, null);
	}
	
	public TaskDescription newPagedAnnotationValidationExecuteTask(PagedAnnotationValidationContainer pavc, TaskDescription parent) throws TaskConflictException {
    	synchronized (pavc.synchronizationString(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME)) {
    		checkIfActivePagedAnnotationValidationTask(pavc, parent);
			
			return newTask(new TaskDescription(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE), parent);
    	}
	}
	
	public TaskDescription newPagedAnnotationValidationPublishTask(PagedAnnotationValidationContainer pavc) throws TaskConflictException {
		return newPagedAnnotationValidationPublishTask(pavc, null);
	}
	
	public TaskDescription newPagedAnnotationValidationPublishTask(PagedAnnotationValidationContainer pavc, TaskDescription parent) throws TaskConflictException {
    	synchronized (pavc.synchronizationString(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME)) {
    		checkIfActivePagedAnnotationValidationTask(pavc, parent);

    		return newTask(new TaskDescription(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH), parent);
    	}
	}
	
	public TaskDescription newPagedAnnotationValidationUnpublishTask(PagedAnnotationValidationContainer pavc) throws TaskConflictException {
		return newPagedAnnotationValidationUnpublishTask(pavc, null);
	}
	
	public TaskDescription newPagedAnnotationValidationUnpublishTask(PagedAnnotationValidationContainer pavc, TaskDescription parent) throws TaskConflictException {
    	synchronized (pavc.synchronizationString(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME)) {
    		checkIfActivePagedAnnotationValidationTask(pavc, parent);

    		return newTask(new TaskDescription(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH), parent);
    	}
	}
	
	public TaskDescription newPagedAnnotationValidationRepublishTask(PagedAnnotationValidationContainer pavc, TaskDescription parent) throws TaskConflictException {
		synchronized (pavc.synchronizationString(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME)) {
			checkIfActivePagedAnnotationValidationTask(pavc, parent);

			TaskDescription tdescr = newTask(new TaskDescription(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_REPUBLISH));

			newPagedAnnotationValidationUnpublishTask(pavc, tdescr);
			newPagedAnnotationValidationPublishTask(pavc, tdescr);
			
			//save again to store children
			taskRepository.save(tdescr);
	
			return tdescr;
		}
	}	

	public TaskDescription newFilterAnnotationValidationExecuteTask(TaskSpecification tspec, FilterAnnotationValidationContainer favc) throws TaskConflictException {
		return newFilterAnnotationValidationExecuteTask(tspec, favc, null);
	}
	
	public TaskDescription newFilterAnnotationValidationExecuteTask(TaskSpecification tspec, FilterAnnotationValidationContainer pavc, TaskDescription parent) throws TaskConflictException {
    	synchronized (pavc.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveFilterAnnotationValidationTask(pavc, parent, tspec.conflictingTasks());
			
			return newTask(new TaskDescription(pavc, TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE), parent);
    	}
	}
	
	public TaskDescription newFilterAnnotationValidationPublishTask(TaskSpecification tspec, FilterAnnotationValidationContainer favc) throws TaskConflictException {
		return newFilterAnnotationValidationPublishTask(tspec, favc, null);
	}
	
	public TaskDescription newFilterAnnotationValidationPublishTask(TaskSpecification tspec, FilterAnnotationValidationContainer favc, TaskDescription parent) throws TaskConflictException {
    	synchronized (favc.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveFilterAnnotationValidationTask(favc, parent, tspec.conflictingTasks());

    		return newTask(new TaskDescription(favc, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH), parent);
    	}
	}
	
	public TaskDescription newFilterAnnotationValidationUnpublishTask(TaskSpecification tspec, FilterAnnotationValidationContainer favc) throws TaskConflictException {
		return newFilterAnnotationValidationUnpublishTask(tspec, favc, null);
	}
	
	public TaskDescription newFilterAnnotationValidationUnpublishTask(TaskSpecification tspec, FilterAnnotationValidationContainer favc, TaskDescription parent) throws TaskConflictException {
    	synchronized (favc.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveFilterAnnotationValidationTask(favc, parent, tspec.conflictingTasks());

    		return newTask(new TaskDescription(favc, TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH), parent);
    	}
	}
	
	public TaskDescription newFilterAnnotationValidationRepublishTask(TaskSpecification tspec, FilterAnnotationValidationContainer favc, TaskDescription parent) throws TaskConflictException {
		synchronized (favc.synchronizationString(tspec.conflictingTasks())) {
			checkIfActiveFilterAnnotationValidationTask(favc, parent, tspec.conflictingTasks());

			TaskDescription tdescr = newTask(new TaskDescription(favc, TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH));

			newFilterAnnotationValidationUnpublishTask(tdescr.getTaskSpecification(), favc, tdescr);
			newFilterAnnotationValidationPublishTask(tdescr.getTaskSpecification(), favc, tdescr);
			
			//save again to store children
			taskRepository.save(tdescr);
	
			return tdescr;
		}
	}		
	
	public TaskDescription newMappingExecuteTask(MappingContainer mc) throws TaskConflictException {
		return newMappingExecuteTask(mc, null);
	}
	
	public TaskDescription newMappingExecuteTask(MappingContainer mc, TaskDescription parent) throws TaskConflictException {
		DatasetContainer dc = datasetService.getInDatasetContainer(mc);
		
		synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
			checkIfActiveDatasetTask(dc, parent);
    	
			synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
				checkIfActiveMappingExecuteTask(mc, parent);
			}
				
			return newTask(new TaskDescription(mc, TaskType.MAPPING_EXECUTE), parent);
    	}
	}
	
	public TaskDescription newAnnotatorExecuteTask(TaskSpecification tspec, AnnotatorContainer ac) throws TaskConflictException {
		return newAnnotatorExecuteTask(tspec, ac, null);
	}
	
	public TaskDescription newAnnotatorExecuteTask(TaskSpecification tspec, AnnotatorContainer ac, TaskDescription parent) throws TaskConflictException {
    	synchronized (ac.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveAnnotatorTask(ac, parent, tspec.conflictingTasks());

    		return newTask(new TaskDescription(ac, TaskType.ANNOTATOR_EXECUTE), parent);
    	}
	}
	
	public TaskDescription newAnnotatorPublishTask(TaskSpecification tspec, AnnotatorContainer ac) throws TaskConflictException {
		return newAnnotatorPublishTask(tspec, ac, null);
	}
	
	public TaskDescription newAnnotatorPublishTask(TaskSpecification tspec, AnnotatorContainer ac, TaskDescription parent) throws TaskConflictException {
    	synchronized (ac.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveAnnotatorTask(ac, parent, tspec.conflictingTasks());

    		return newTask(new TaskDescription(ac, TaskType.ANNOTATOR_PUBLISH), parent);
    	}
	}
	
	public TaskDescription newAnnotatorUnpublishTask(TaskSpecification tspec, AnnotatorContainer ac) throws TaskConflictException {
		return newAnnotatorUnpublishTask(tspec, ac, null);
	}
	
	public TaskDescription newAnnotatorUnpublishTask(TaskSpecification tspec, AnnotatorContainer ac, TaskDescription parent) throws TaskConflictException {
    	synchronized (ac.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveAnnotatorTask(ac, parent, tspec.conflictingTasks());

    		return newTask(new TaskDescription(ac, TaskType.ANNOTATOR_UNPUBLISH), parent);
    	}
	}
	
	public TaskDescription newAnnotatorRepublishTask(TaskSpecification tspec, AnnotatorContainer ac, TaskDescription parent) throws TaskConflictException {
		synchronized (ac.synchronizationString(tspec.conflictingTasks())) {
			checkIfActiveAnnotatorTask(ac, parent, tspec.conflictingTasks());

			TaskDescription tdescr = newTask(new TaskDescription(ac, TaskType.ANNOTATOR_REPUBLISH));

			newAnnotatorUnpublishTask(tdescr.getTaskSpecification(), ac, tdescr);
			newAnnotatorPublishTask(tdescr.getTaskSpecification(), ac, tdescr);
			
			//save again to store children
			taskRepository.save(tdescr);
	
			return tdescr;
		}
	}	

	public TaskDescription newEmbedderExecuteTask(TaskSpecification tspec, EmbedderContainer ec) throws TaskConflictException  {
		return newEmbedderExecuteTask(tspec, ec, null);
	}
	
	public TaskDescription newEmbedderExecuteTask(TaskSpecification tspec, EmbedderContainer ec, TaskDescription parent) throws TaskConflictException {
    	synchronized (ec.synchronizationString(TaskType.EMBEDDER_EXECUTE, TaskType.EMBEDDER_PUBLISH, TaskType.EMBEDDER_UNPUBLISH)) {
    		checkIfActiveEmbedderTask(ec, parent, tspec.conflictingTasks());

   			return newTask(new TaskDescription(ec, TaskType.EMBEDDER_EXECUTE), parent);
    	}
	}
	
	public TaskDescription newEmbedderPublishTask(TaskSpecification tspec, EmbedderContainer ec) throws TaskConflictException {
		return newEmbedderPublishTask(tspec, ec, null);
	}
	
	public TaskDescription newEmbedderPublishTask(TaskSpecification tspec, EmbedderContainer ec, TaskDescription parent) throws TaskConflictException {
		synchronized (ec.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveEmbedderTask(ec, parent, tspec.conflictingTasks());
		
    		return newTask(new TaskDescription(ec, TaskType.EMBEDDER_PUBLISH), parent);
		}
	}
	
	public TaskDescription newEmbedderRepublishTask(TaskSpecification tspec, EmbedderContainer ec, TaskDescription parent) throws TaskConflictException {
		synchronized (ec.synchronizationString(tspec.conflictingTasks())) {
			checkIfActiveEmbedderTask(ec, parent, tspec.conflictingTasks());

			TaskDescription tdescr = newTask(new TaskDescription(ec, TaskType.EMBEDDER_REPUBLISH));

			newEmbedderUnpublishTask(tdescr.getTaskSpecification(), ec, tdescr); // or may be have tspec instead of tdescr.getTaskSpecification()
			newEmbedderPublishTask(tdescr.getTaskSpecification(), ec, tdescr);
			
			//save again to store children
			taskRepository.save(tdescr);
	
			return tdescr;
		}
	}	

	public TaskDescription newEmbedderUnpublishTask(TaskSpecification tspec, EmbedderContainer ec) throws TaskConflictException {
		return newEmbedderUnpublishTask(tspec, ec, null);
	}
	
	public TaskDescription newEmbedderUnpublishTask(TaskSpecification tspec, EmbedderContainer ec, TaskDescription parent) throws TaskConflictException {
		synchronized (ec.synchronizationString(tspec.conflictingTasks())) {
    		checkIfActiveEmbedderTask(ec, parent, tspec.conflictingTasks());

			return newTask(new TaskDescription(ec, TaskType.EMBEDDER_UNPUBLISH), parent);
		}
	}

	public TaskDescription newDatasetPublishTask(DatasetContainer dc, Properties props) throws TaskConflictException {
		return newDatasetPublishTask(dc, props, null);
	}
	
	public TaskDescription newDatasetPublishTask(DatasetContainer dc, Properties props, TaskDescription parent) throws TaskConflictException {
		synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
			checkIfActiveDatasetTask(dc, parent);
			
			for (MappingContainer mc : mappingsService.getMappingContainers(dc)) {
				synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
					checkIfActiveMappingExecuteTask(mc, parent);
				}
			}

			return newTask(new TaskDescription(dc, TaskType.DATASET_PUBLISH, props), parent);
		}
	}

	public TaskDescription newDatasetUnpublishTask(DatasetContainer dc, Properties props) throws TaskConflictException {
		return newDatasetUnpublishTask(dc, props, null);
	}
	
	public TaskDescription newDatasetUnpublishTask(DatasetContainer dc, Properties props, TaskDescription parent) throws TaskConflictException {
		synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
			checkIfActiveDatasetTask(dc, parent);
			
			for (MappingContainer mc : mappingsService.getMappingContainers(dc)) {
				synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
					checkIfActiveMappingExecuteTask(mc, parent);
				}
			}

			return newTask(new TaskDescription(dc, TaskType.DATASET_UNPUBLISH, props), parent);
		}
	}
	
	public TaskDescription newDatasetRepublishTask(DatasetContainer dc) throws TaskConflictException {
		return newDatasetRepublishTask(dc, null);
	}
	
	public TaskDescription newDatasetRepublishTask(DatasetContainer dc, TaskDescription parent) throws TaskConflictException {
		synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
			checkIfActiveDatasetTask(dc, parent);

			TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_REPUBLISH));
			
			Properties props;
			
			props = new Properties();
			props.put(ServiceProperties.PUBLISH_METADATA, true);
			props.put(ServiceProperties.PUBLISH_CONTENT, true);
			
			newDatasetUnpublishTask(dc, props, tdescr);
			
			props = new Properties();
			props.put(ServiceProperties.PUBLISH_MODE, dc.getDataset().isPublik() ? ServiceProperties.PUBLISH_MODE_PUBLIC : ServiceProperties.PUBLISH_MODE_PRIVATE);
			props.put(ServiceProperties.PUBLISH_METADATA, true);
			props.put(ServiceProperties.PUBLISH_CONTENT, true);
			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
			props.put(ServiceProperties.TRIPLE_STORE, dc.getDatasetTripleStoreVirtuosoConfiguration());
			
			newDatasetPublishTask(dc, props, tdescr);
			
			//save again to store children
			taskRepository.save(tdescr);
	
			return tdescr;
		}
	}	
	
	public TaskDescription newDatasetRepublishMetadataTask(DatasetContainer dc) throws TaskConflictException, Exception {
		return newDatasetRepublishMetadataTask(dc, null);
	}
	
	public TaskDescription newDatasetRepublishMetadataTask(DatasetContainer dc, TaskDescription parent) throws TaskConflictException, Exception {
		synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
			checkIfActiveDatasetTask(dc, parent);

			TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_REPUBLISH_METADATA), parent);
			
			int isPublic = ServiceProperties.PUBLISH_MODE_CURRENT;
			if (dc.getPublishState().getPublishState() == DatasetState.PUBLISHED_PUBLIC) {
				isPublic = ServiceProperties.PUBLISH_MODE_PUBLIC;
			} else if (dc.getPublishState().getPublishState() == DatasetState.PUBLISHED_PRIVATE) {
				isPublic = ServiceProperties.PUBLISH_MODE_PRIVATE;
			}
	
			Properties props;
			
			props = new Properties();
			props.put(ServiceProperties.PUBLISH_METADATA, true);
			props.put(ServiceProperties.PUBLISH_CONTENT, false);
			
			newDatasetUnpublishTask(dc, props, tdescr);
			
			props = new Properties();
			props.put(ServiceProperties.PUBLISH_MODE, isPublic);
			props.put(ServiceProperties.PUBLISH_METADATA, true);
			props.put(ServiceProperties.PUBLISH_CONTENT, false);
			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
			props.put(ServiceProperties.TRIPLE_STORE, dc.getDatasetTripleStoreVirtuosoConfiguration());
			
			newDatasetPublishTask(dc, props, tdescr);
			
			//save again to save children
			taskRepository.save(tdescr);
			
			return tdescr;
		}
	}
	
	public TaskDescription newDatasetExecuteMappingsTask(DatasetContainer dc) throws TaskConflictException {
    	synchronized (dc.synchronizationString(TaskType.DATASET_MAPPINGS_EXECUTE)) {
    		if (getActiveTask(dc, TaskType.DATASET_MAPPINGS_EXECUTE) != null) {
    			throw new TaskConflictException("The dataset mappings are already being executed.");
    		} else {
				TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_MAPPINGS_EXECUTE));
				
				try {
					scheduleMappingsExecution(tdescr, dc);
				} catch (TaskConflictException tcex) {
					cancelNewTask(tdescr);
					throw tcex;
				}
				
				//save again to store children
				taskRepository.save(tdescr);
		
				return tdescr;
    		}
    	}
	}	
	
	public TaskDescription newDatasetExecuteMappingsAndRepublishTask(DatasetContainer dc) throws TaskConflictException {
    	synchronized (dc.synchronizationString(TaskType.DATASET_MAPPINGS_EXECUTE)) {
    		if (getActiveTask(dc, TaskType.DATASET_MAPPINGS_EXECUTE) != null) {
    			throw new TaskConflictException("The dataset mappings are already being executed.");
    		} else {
				synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
					checkIfActiveDatasetTask(dc, null);

					TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_MAPPINGS_EXECUTE_AND_REPUBLISH));
					
					try {
						scheduleMappingsExecution(tdescr, dc);
					} catch (TaskConflictException tcex) {
						cancelNewTask(tdescr);
						throw tcex;
					}
					
					Properties props;
					
					props = new Properties();
					props.put(ServiceProperties.PUBLISH_METADATA, true);
					props.put(ServiceProperties.PUBLISH_CONTENT, true);
					
					newDatasetUnpublishTask(dc, props, tdescr);
					
					props = new Properties();
					props.put(ServiceProperties.PUBLISH_MODE, dc.getDataset().isPublik() ? ServiceProperties.PUBLISH_MODE_PUBLIC : ServiceProperties.PUBLISH_MODE_PRIVATE);
					props.put(ServiceProperties.PUBLISH_METADATA, true);
					props.put(ServiceProperties.PUBLISH_CONTENT, true);
					props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
					props.put(ServiceProperties.TRIPLE_STORE, dc.getDatasetTripleStoreVirtuosoConfiguration());
					
					newDatasetPublishTask(dc, props, tdescr);
					
					//save again to store children
					taskRepository.save(tdescr);

					return tdescr;
				}
    		}
		}
	}	

	public TaskDescription newDatasetCreateDistributionTask(DatasetContainer dc, Properties props) throws TaskConflictException {
		synchronized (dc.synchronizationString(TaskType.DATASET_PUBLISH, TaskType.DATASET_UNPUBLISH, TaskType.DATASET_REPUBLISH, TaskType.DATASET_REPUBLISH_METADATA, TaskType.DATASET_CREATE_DISTRIBUTION)) {
			checkIfActiveDatasetTask(dc, null);

			TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_CREATE_DISTRIBUTION, props));
				
			return tdescr;
		}
	}

	public TaskDescription getActiveTask(DatasetContainer dc, TaskType type) {
		return taskRepository.findActiveByDatasetIdAndTripleStoreConfigurationId(dc.getDatasetId(), dc.getTripleStoreConfiguration() != null ? dc.getTripleStoreConfiguration().getId() : null, type).orElse(null);
	}

	public TaskDescription getActiveTask(MappingContainer mc, TaskType type) {
		return taskRepository.findActiveByMappingIdAndMappingInstanceIdAndFileSystemConfigurationId(mc.getMappingId(), mc.getMappingInstanceId(), mc.getContainerFileSystemConfiguration().getId(), type).orElse(null);
	}

	public TaskDescription getActiveTask(AnnotatorContainer ac, TaskType type) {
		return taskRepository.findActiveByAnnotatorIdAndFileSystemConfigurationId(ac.getAnnotatorId(), ac.getContainerFileSystemConfiguration().getId(), type).orElse(null);
	}
	
	public TaskDescription getActiveTask(EmbedderContainer ec, TaskType type) {
		return taskRepository.findActiveByEmbedderIdAndFileSystemConfigurationId(ec.getEmbedderId(), ec.getContainerFileSystemConfiguration().getId(), type).orElse(null);
	}

	public TaskDescription getActiveTask(PagedAnnotationValidationContainer pavc, TaskType type) {
		return taskRepository.findActiveByPagedAnnotationValidationIdAndFileSystemConfigurationId(pavc.getPagedAnnotationValidationId(), pavc.getContainerFileSystemConfiguration().getId(), type).orElse(null);
	}

	public TaskDescription getActiveTask(FilterAnnotationValidationContainer favc, TaskType type) {
		return taskRepository.findActiveByFilterAnnotationValidationIdAndFileSystemConfigurationId(favc.getFilterAnnotationValidationId(), favc.getContainerFileSystemConfiguration().getId(), type).orElse(null);
	}
	
	public void checkIfActiveDatasetTask(DatasetContainer dc, TaskDescription parent) throws TaskConflictException {
		
		TaskDescription check;
		
		check = getActiveTask(dc, TaskType.DATASET_PUBLISH);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The dataset is currently being published.");
		}
		
		check = getActiveTask(dc, TaskType.DATASET_UNPUBLISH);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The dataset is currently being unpublished.");
		}
		
		check = getActiveTask(dc, TaskType.DATASET_REPUBLISH);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The dataset is currently being republished.");
		}
		
		check = getActiveTask(dc, TaskType.DATASET_REPUBLISH_METADATA);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The dataset metadata are currently being republished.");
		} 
		
		check = getActiveTask(dc, TaskType.DATASET_CREATE_DISTRIBUTION);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("A dataset distribution is currently being created.");
		}
	}
	
	public void checkIfActiveAnnotatorTask(AnnotatorContainer ac, TaskDescription parent, List<TaskType> conflictingTasks) throws TaskConflictException {
		
		TaskDescription check;
		
		if (conflictingTasks.contains(TaskType.ANNOTATOR_EXECUTE)) {
			check = getActiveTask(ac, TaskType.ANNOTATOR_EXECUTE);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The annotator is being executed or is queued for execution.");
			} 
		}
		if (conflictingTasks.contains(TaskType.ANNOTATOR_PUBLISH)) {
			check = getActiveTask(ac, TaskType.ANNOTATOR_PUBLISH);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The annotator is being published.");
			}
		}

		if (conflictingTasks.contains(TaskType.ANNOTATOR_UNPUBLISH)) {			
			check = getActiveTask(ac, TaskType.ANNOTATOR_UNPUBLISH);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The annotator is being unpublished.");
			}
		}
	}

	public void checkIfActiveEmbedderTask(EmbedderContainer ec, TaskDescription parent, List<TaskType> conflictingTasks) throws TaskConflictException {
		TaskDescription check;

		if (conflictingTasks.contains(TaskType.EMBEDDER_EXECUTE)) {
			check = getActiveTask(ec, TaskType.EMBEDDER_EXECUTE);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The embedder is being executed or is queued for execution.");
			}
		}
		
		if (conflictingTasks.contains(TaskType.EMBEDDER_PUBLISH)) {
			check = getActiveTask(ec, TaskType.EMBEDDER_PUBLISH);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The embedder is being published.");
			}
		}
		
		if (conflictingTasks.contains(TaskType.EMBEDDER_UNPUBLISH)) {
			check = getActiveTask(ec, TaskType.EMBEDDER_UNPUBLISH);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The embedder is being unpublished.");
			}
		}

	}
	
	private boolean isChildOrSameOf(TaskDescription child, TaskDescription parent) {
		if (parent == null) {
			return false;
		}
		
		if (child.getId().equals(parent.getId())) {
			return true;
		}
		
		return child.getParentId() != null && child.getParentId().equals(parent.getId());
	}
	
	public void checkIfActiveMappingExecuteTask(MappingContainer mc, TaskDescription parent) throws TaskConflictException {
		TaskDescription check;
		
		check = getActiveTask(mc, TaskType.MAPPING_EXECUTE);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("Mapping " + mc.getMappingDocument().getName() + " is being executed or queued for execution.");
		}
	}
	
	public void checkIfActivePagedAnnotationValidationTask(PagedAnnotationValidationContainer pavc, TaskDescription parent) throws TaskConflictException {
		TaskDescription check;
		
		check = getActiveTask(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The paged annotation validation is being executed or is queued for execution.");
		}

		check = getActiveTask(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The paged annotation validation is currently being published.");
		} 
		
		check = getActiveTask(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The paged annotation validation is currently being unpublished.");
		} 

		check = getActiveTask(pavc, TaskType.PAGED_ANNOTATION_VALIDATION_RESUME);
		if (check != null && !isChildOrSameOf(check, parent)) {
			throw new TaskConflictException("The paged annotation validation is currently being resumed.");
		}
	}

	public void checkIfActiveFilterAnnotationValidationTask(FilterAnnotationValidationContainer favc, TaskDescription parent, List<TaskType> conflictingTasks) throws TaskConflictException {
		TaskDescription check;
		
		if (conflictingTasks.contains(TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE)) {
			check = getActiveTask(favc, TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The filter annotation validation is already executed or is queued for execution.");
			} 
		}
		
		if (conflictingTasks.contains(TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH)) {
			check = getActiveTask(favc, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The filter annotation validation is currently being published.");
			} 
		}
		
		if (conflictingTasks.contains(TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH)) {
			check = getActiveTask(favc, TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH);
			if (check != null && !isChildOrSameOf(check, parent)) {
				throw new TaskConflictException("The filter annotation validation is currently being unpublished.");
			}
		}
	}
}
