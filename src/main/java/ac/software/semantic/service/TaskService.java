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
import ac.software.semantic.model.CatalogTemplateResult;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.MappingTemplateResult;
import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.DocumentType;
import ac.software.semantic.model.constants.type.MappingType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.state.RunState;
import ac.software.semantic.payload.notification.NotificationObject;
import ac.software.semantic.payload.request.MappingInstanceUpdateRequest;
import ac.software.semantic.payload.request.MappingUpdateRequest;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.MappingDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.root.TemplateServiceRepository;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ExtendedParameter;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.TemplateService;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.UserTaskDescription;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.TaskSpecification.Conflicts;
import ac.software.semantic.service.TaskSpecification.Fork;
import ac.software.semantic.service.UserTaskService.UserTaskContainer;
import ac.software.semantic.service.container.CreatableContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.RunnableContainer;
import ac.software.semantic.service.container.TaskFunctionalInterface;
import ac.software.semantic.service.exception.TaskConflictException;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.vocs.SEMAVocabulary;

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
	private MappingDocumentRepository mappingRepository;

	@Lazy
	@Autowired
    private AnnotatorService annotatorService;

	@Lazy
	@Autowired
    private EmbedderService embedderService;

	@Lazy
	@Autowired
    private ClustererService clustererService;

	@Lazy
	@Autowired
    private IndexService indexService;

	@Lazy
	@Autowired
    private FileService fileService;

	@Lazy
	@Autowired
    private UserTaskService userTaskService;

	@Lazy
	@Autowired
    private DistributionService distributionService;

	@Lazy
	@Autowired
    private PagedAnnotationValidationService pavService;

	@Lazy
	@Autowired
    private FilterAnnotationValidationService favService;

	@Lazy
	@Autowired
	private DatasetService datasetService;

	@Lazy
	@Autowired
	private MappingService mappingService;

	@Autowired
	private TemplateServicesService templateService;

	@Autowired
	private TemplateServiceRepository templateRepository;

	@Autowired
	private WebSocketService wsService;

	@Autowired
	private DocumentService repositoryService;

	@Autowired
	private ServiceUtils serviceUtils;
	
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
			DataQueue queue = new DataQueue(10);
			tripleStoreTaskQueue.put(ts, queue);
			
		    Thread thread = new Thread(new Consumer(queue));
		    logger.info("Created publication worker thread for " + ts.getName());
		    thread.start();
		}
	}
    
    
	private void cascadeFailUnfinishedTask(TaskDescription t) {
		ObjectContainer<?,?> oc = repositoryService.getContainer(t);
		
		if (oc != null) {
			try {
				if (TaskType.isExecute(t.getType())) {
					oc.update(ioc -> { 
						ExecutableContainer<?,?,?,?> iec = (ExecutableContainer<?,?,?,?>)ioc;
						ExecuteState ies = iec.checkExecuteState();
						if (ies != null) {
							ies.fail(t.getEndTime(), "System has been restarted.");
						}
					});
				} else if (TaskType.isPublish(t.getType()) || TaskType.isUnpublish(t.getType())) {
					oc.update(ioc -> { 
						PublishableContainer<?,?,?,?,?> ipc = (PublishableContainer<?,?,?,?,?>)ioc;
						PublishState<?> ips = ipc.checkPublishState();
						if (ips != null) {
							ips.fail(t.getEndTime(), "System has been restarted.");
						}
					});			
				} else if (TaskType.isCreate(t.getType()) || TaskType.isDestroy(t.getType())) {
					oc.update(ioc -> { 
						CreatableContainer<?,?,?,?> icc = (CreatableContainer<?,?,?,?>)ioc;
						CreateState ics = icc.checkCreateState();
						if (ics != null) {
							ics.fail(t.getEndTime(), "System has been restarted.");
						}
					});
				} else if (TaskType.isRun(t.getType())) {
					oc.update(ioc -> { 
						RunnableContainer<?,?> irc = (RunnableContainer<?,?>)ioc;
						RunState irs = irc.checkRunState();
						if (irs != null) {
							irs.fail(t.getEndTime(), "System has been restarted.");
						}
					});					
				}
			} catch (Exception ex) {
				ex.printStackTrace();
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
		return getTasks(userId, null, parent);
	}	

	public List<TaskDescription> getTasks(String userId, Date from, boolean parent) {
		if (from == null) {
			return taskRepository.findByUserIdAndDatabaseIdAndSystemAndParentIdExistsOrderByCreateTimeDesc(new ObjectId(userId), database.getId(), mac + ":" + environment.getProperty("local.server.port"), parent);
		} else {
			return taskRepository.findByUserIdAndDatabaseIdAndSystemAndParentIdExistsAndCreateTimeGreaterThanOrderByCreateTimeDesc(new ObjectId(userId), database.getId(), mac + ":" + environment.getProperty("local.server.port"), parent, from);
		}
	}	

//	private TaskDescription newTask(TaskDescription tdescr) {
//		return newTask(tdescr, null);
//	}

	private TaskDescription newTask(TaskDescription tdescr, TaskDescription parent) {
//		System.out.println("NEW TASK " + tdescr.getType() + " " + tdescr.getDescription() + " " + parent);
		tdescr.setDatabaseId(database.getId());
		tdescr.setSystem(mac + ":" + environment.getProperty("local.server.port"));
		tdescr.setUuid(UUID.randomUUID().toString());
    	tdescr.setCreateTime(new Date(System.currentTimeMillis()));
		tdescr.setState(TaskState.QUEUED);

		tdescr.createMonitor(wsService);
		
		if (parent != null) {
			tdescr.setParent(parent);
			if (parent.getRootId() == null) {
				tdescr.setRootId(parent.getId());
			} else {
				tdescr.setRootId(parent.getRootId());
			}
		}
    	
    	tdescr = taskRepository.save(tdescr);
    	
		runningTasks.put(tdescr.getId(), tdescr);
		
		if (parent != null) {
			parent.addChild(tdescr);
		}
		
//		System.out.println("\t" + tdescr.getId());
		return tdescr;
	}
	
	private void cancelNewTask(TaskDescription tdescr) {
//		System.out.println("CANCELING TASK " + tdescr.getId());
		runningTasks.remove(tdescr.getId());
		taskRepository.deleteById(tdescr.getId());
		
		if (tdescr.getChildrenIds() != null) {
			for (ObjectId childId : tdescr.getChildrenIds()) {
				Optional<TaskDescription> childDescrOpt = taskRepository.findById(childId);
				
				if (childDescrOpt.isPresent()) {
					cancelNewTask(childDescrOpt.get());
				}
			}
		}
	}
	
	public void taskTerminated(TaskDescription tdescr, TaskState state, Date date) {
		tdescr.setState(state);
		tdescr.setEndTime(date);
		taskRepository.save(tdescr);
		
		runningTasks.remove(tdescr.getId());
	}

	public void taskFailed(TaskDescription tdescr, Date date) {
		taskTerminated(tdescr, TaskState.FAILED, date);
	}

	public void taskCanceled(TaskDescription tdescr, Date date) {
		taskTerminated(tdescr, TaskState.CANCELED, date);
	}

	public void taskStarted(TaskDescription tdescr) {
		taskStarted(tdescr, new Date());
	}

	public void taskStarted(TaskDescription tdescr, Date date) {
		tdescr.setState(TaskState.STARTED);
		tdescr.setStartTime(date);
		
		taskRepository.save(tdescr);
		
//		if (tdescr.getMonitor() != null) {
//			tdescr.getMonitor().start();
//		}
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
	            List<TaskDescription> rest = taskDescObj.getNextTaskDescriptions();
	            
				try {
					System.out.println("STARTING TASK QUEUE " + tdescr.getId() + " " + tdescr.toString());
					
					TaskType type = tdescr.getType();
					
//					if (type == TaskType.DATASET_PUBLISH) {
//						taskStarted(tdescr, new Date(System.currentTimeMillis()));
//						ListenableFuture<Date> task = datasetService.publishOld(tdescr, wsService);
//						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					if (type == TaskType.DATASET_PUBLISH_METADATA) {
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
						ListenableFuture<Date> task = datasetService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.MAPPING_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
						ListenableFuture<Date> task = mappingService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.FILE_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
						ListenableFuture<Date> task = fileService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.ANNOTATOR_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
						ListenableFuture<Date> task = annotatorService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.EMBEDDER_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
						ListenableFuture<Date> task = embedderService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.CLUSTERER_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
						ListenableFuture<Date> task = clustererService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
						ListenableFuture<Date> task = pavService.publish(tdescr, wsService);
						setTask(tdescr, task, taskDescObj.getNextTaskDescriptions());
					} else if (type == TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH) { 
						taskStarted(tdescr, new Date(System.currentTimeMillis()));
						callPreTask(tdescr, rest);
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
					
	            		System.out.println("FINISHED AND REMOVING FROM QUEUE " + tdescr.getId() + " " + tdescr.toString());
	            		
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
		public void onSuccess(Date completedAt) {
			System.out.println("C >>>  COMPLETED " + " " + tdescr.getType() + " " + tdescr.getDescription() + " " +tdescr.getParentId() + " > " + completedAt);
				
			try {
				PreFunctionalInterface pfi = getPostSuccessMethod(tdescr.getType());
				if (pfi != null) {
					Date date = pfi.preTask(tdescr, wsService);
					if (date != null) {
						completedAt = date;
					}
				}
				
				taskTerminated(tdescr, TaskState.COMPLETED, completedAt);
				
			} catch (TaskFailureException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
				
//				onFailure(ex);
				Date failureTime = ((TaskFailureException)ex).getFailureTime();

				taskTerminated(tdescr, TaskState.FAILED, failureTime);
				abandonTask(tdescr, rest, failureTime);
				
				return;
			
			} finally {
				if (tdescr.getMonitor() != null) {
					try {
						tdescr.getMonitor().close();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				synchronized (tdescr) {
					tdescr.notifyAll();
				}

			}
			
			

//			if (tdescr.getMonitor() != null) {
//				try {
//					tdescr.getMonitor().close();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//			
////			System.out.println("SUCCESS NOTIFYING " + tdescr.getId() + " " + tdescr.toString());
//			synchronized (tdescr) {
//				tdescr.notifyAll();
//			}
				
			if (tdescr.getPostTaskExecution() != null) {
				try {
					tdescr.getPostTaskExecution().onComplete();
				} catch (Exception ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
			}
			
			if (rest != null && rest.size() > 0) {
				TaskDescription next = rest.remove(0);
				cascadeSuccessToParents(tdescr, next);
				
				call(next, rest);
			} else {
				cascadeSuccessToParents(tdescr, null);
			}
		}
	
		private void cascadeSuccessToParents(TaskDescription tdescr, TaskDescription next) {
			ObjectId parent = tdescr.getParentId();
			if (parent != null) {
				if (next == null || (!next.getParentId().equals(tdescr.getParentId()))) {
					TaskDescription parentDescr = runningTasks.get(parent);
					
					taskTerminated(parentDescr, TaskState.COMPLETED, tdescr.getEndTime());
					
					try {
						PreFunctionalInterface pfi = getPostSuccessMethod(parentDescr.getType());
						if (pfi != null) {
							pfi.preTask(parentDescr, wsService);
						}
					} catch (Exception ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
						
						Date failureTime = ((TaskFailureException)ex).getFailureTime();

						taskTerminated(parentDescr, TaskState.FAILED, failureTime);
						abandonTask(parentDescr, rest, failureTime);
					}
					
					cascadeSuccessToParents(parentDescr, next);
				}
			}
		}
		
		@Override  
		public void onFailure(Throwable ex) {
			try {
				System.out.println("C >>>  FAILED" + ex);
				System.out.println("FAILURE NOTIFYING " + tdescr.getId() + " " + tdescr.toString());
				
				Date failureTime = new Date(System.currentTimeMillis());
				TaskState state;
				
				if (ex instanceof CancellationException) {
					state = TaskState.STOPPED;
				} else if (ex instanceof TaskFailureException) {
					state = TaskState.FAILED;
					failureTime = ((TaskFailureException)ex).getFailureTime();
				} else {
					ex.printStackTrace();
					state = TaskState.FAILED;
				}

				taskTerminated(tdescr, state, failureTime);
				abandonTask(tdescr, rest, failureTime);
				
				synchronized (tdescr) {
					tdescr.notifyAll();
				}
				
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
				cascadeCancelToChildren(next, completedAt);
			}
		}
		
		try {
			PreFunctionalInterface pfi = getPostFailMethod(tdescr.getType());
			if (pfi != null) {
				pfi.preTask(tdescr, wsService);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// fail parent
		cascadeFailureToParents(tdescr);
	}
	
	private void cascadeFailureToParents(TaskDescription tdescr) {
		ObjectId parent = tdescr.getParentId();

		if (parent != null) { 
			TaskDescription parentDescr = runningTasks.get(parent);
			
			taskTerminated(parentDescr, tdescr.getState(), tdescr.getEndTime());
			
			try {
				PreFunctionalInterface pfi = getPostFailMethod(parentDescr.getType());
				if (pfi != null) {
					pfi.preTask(parentDescr, wsService);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			cascadeFailureToParents(parentDescr);
		}
	}
	
	private void cascadeCancelToChildren(TaskDescription tdescr, Date completedAt) {
		taskCanceled(tdescr, completedAt);
		if (tdescr.getChildren() != null) {
			for (TaskDescription child : tdescr.getChildren()) {
				cascadeCancelToChildren(child, completedAt);
			}
		}
	}
	
	private void queueTripleStoreTask(TaskDescription tdescr, List<TaskDescription> rest) {
		System.out.println("QS A");
		DatasetContainer dc = datasetService.getInDatasetContainer((EnclosedObjectContainer<?,?,Dataset>)tdescr.getContainer());
		
		TripleStoreConfiguration ts = dc.getTripleStoreConfiguration();
		if (ts == null) { // this should happen only in dataset publish 
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
					tm.sendMessage(no);
				}
			}
			
			System.out.println("ADDED TO QUEUE" + tdescr.getId() + " " + tdescr.toString());
			
			list.notifyAllForEmpty();
		} else {
			
			System.out.println("QUEUE IS FULL : TASK REJECTED " + tdescr.getId() + " " + tdescr.toString());
			
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
					tm.sendMessage(no);
				}
			}

			
			// here should cancel task because queue limit exceeded
			taskCanceled(tdescr, completedAt);
			abandonTask(tdescr, rest, completedAt);
		}
	}
	
	private void callSingleTask(TaskDescription tdescr, List<TaskDescription> rest) {
		
		System.out.println("CALLING SINGLE " + tdescr.getType() + " " + tdescr.getDescription() + " " + tdescr);
		TaskType type = tdescr.getType();
		
//		if (type == TaskType.DATASET_PUBLISH) {
//			queueTripleStoreTask(tdescr, rest);
		if (type == TaskType.DATASET_PUBLISH_METADATA) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.DATASET_UNPUBLISH_CONTENT) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = datasetService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating dataset content unpublish task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.DATASET_UNPUBLISH_METADATA) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = datasetService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating dataset metadata unpublish task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.MAPPING_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.MAPPING_EXECUTE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = mappingService.execute(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating mapping execute task");
				ex.printStackTrace();
			}
			
		} else if (type == TaskType.MAPPING_SHACL_VALIDATE_LAST_EXECUTION) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = mappingService.validate(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating mapping validate task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.FILE_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.ANNOTATOR_EXECUTE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
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
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = annotatorService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating annotator unpublish task");
				ex.printStackTrace();
			}			
		
		} else if (type == TaskType.EMBEDDER_EXECUTE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
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
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = embedderService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating embedder unpublish task");
				ex.printStackTrace();
			}				
		
		} else if (type == TaskType.CLUSTERER_EXECUTE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
			   	ListenableFuture<Date> task = clustererService.execute(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating clusterer execute task");
				ex.printStackTrace();
			}		
		} else if (type == TaskType.CLUSTERER_PUBLISH) {
			queueTripleStoreTask(tdescr, rest);
		} else if (type == TaskType.CLUSTERER_UNPUBLISH) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = clustererService.unpublish(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating clusterer unpublish task");
				ex.printStackTrace();
			}			
					
		} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_RESUME) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = pavService.resumeValidation(tdescr, wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating paged annotation validation resume task");
				ex.printStackTrace();
			}		   		
		} else if (type == TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
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
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = pavService.unpublish(tdescr, wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating paged annotation validation unpublish task");
				ex.printStackTrace();
			}	
			
		} else if (type == TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
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
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = favService.unpublish(tdescr, wsService);
		   		setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating filter annotation validation unpublish task");
				ex.printStackTrace();
			}				
		
		} else if (type == TaskType.INDEX_CREATE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = indexService.create(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating index create task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.INDEX_DESTROY) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = indexService.destroy(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating index destroy task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.DISTRIBUTION_CREATE) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = distributionService.create(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating distribution create task");
				ex.printStackTrace();
			}
		} else if (type == TaskType.DISTRIBUTION_DESTROY) {
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = distributionService.destroy(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating distribution destroy task");
				ex.printStackTrace();
			}			
		
		} else { // for empty group tasks
			taskStarted(tdescr);
			callPreTask(tdescr, rest);
			try {
				ListenableFuture<Date> task = serviceUtils.dummyTask(tdescr, wsService);
				setTask(tdescr, task, rest);
			} catch (TaskFailureException ex) {
				logger.error("Error creating dummy task");
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
			taskStarted(tdescr, new Date());

			rest.addAll(0, tdescr.getChildren());
			
			callPreTask(tdescr, rest);
			
			TaskDescription tdescr1 = rest.remove(0);
			
			call(tdescr1, rest);
			
		} else {
			callSingleTask(tdescr, rest);
		}
	}
	
	private void callPreTask(TaskDescription tdescr, List<TaskDescription> rest) {
		try {
			PreFunctionalInterface pfi = getPreMethod(tdescr.getType());
			if (pfi != null) {
				pfi.preTask(tdescr, wsService);
			}
		} catch (TaskFailureException ex) {
			ex.printStackTrace();

			logger.error("Error creating pre task");
			
			taskTerminated(tdescr, TaskState.FAILED, ex.getFailureTime());
			abandonTask(tdescr, rest, ex.getFailureTime());
			
			synchronized (tdescr) {
				tdescr.notifyAll();
			}
			
			return;
		
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

			File file = folderService.getMappingExecutionTxtFile(mc.getCurrentUser(), mc.getEnclosingObject(), mc.getObject(), mc.getMappingInstance(), es, 0);
			
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
				TemplateService datasetTemplate = templateService.getDatasetImportTemplate(ctr.getTemplate());
				
//				System.out.println("MAPPINGS " + ctr.getMappings());
				
//				DatasetUpdateRequest ur = new DatasetUpdateRequest();
//				ur.setName(ctr.getName());
//				ur.setPublik(dc.getEnclosingObject().isPublik());
//				ur.setScope(DatasetScope.COLLECTION);
//				ur.setType(DatasetType.DATASET);
//				
//				TemplateResponse tr = modelMapper.template2TemplateResponse(datasetTemplate);
//				tr.setParameters(ctr.getBindings());
//				
//				datasetService.create(dc.getCurrentUser(), null, ur);
				
				Dataset dataset = datasetService.createDataset(dc.getCurrentUser(), ctr.getName(), null, 
			           dc.getEnclosingObject().isPublik(), false, null,
//			           dc.getTripleStore(), 
			           DatasetScope.COLLECTION, DatasetType.DATASET, SEMAVocabulary.DataCollection.toString(),
					   null,
					   null, datasetTemplate.getId(), ctr.getBindings(), null, null, null);
				
				dc.getEnclosingObject().addDataset(dataset.getId());
				datasetRepository.save(dc.getEnclosingObject());
				
//				DatasetContainer newdc = datasetService.getContainer(dc.getCurrentUser(), dataset.getId().toString(), dc.getTripleStore());
				DatasetContainer newdc = datasetService.getContainer(dc.getCurrentUser(), dataset.getId());
				
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
	     		MappingContainer mc = mappingService.getContainer(dc, mapping.getId().toString());
	     		
	     		MappingInstanceUpdateRequest ur = new MappingInstanceUpdateRequest();
	     		ur.setBindings(mtd.getBindings());
	     		MappingInstance mi = mappingService.createParameterBinding(mc, ur);
	     		mi.setActive(true);
				
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

		Dataset ds = dc.getEnclosingObject();
		
		TaskDescription tdescr;
		
		if (ds.getDatasetType() == DatasetType.CATALOG) {
			tdescr = new TaskDescription(dc,TaskType.CATALOG_IMPORT_BY_TEMPLATE);
		} else {
			tdescr = new TaskDescription(dc,TaskType.DATASET_IMPORT_BY_TEMPLATE);
		}

		tdescr = newTask(tdescr, parent);

		if (ds.getDatasetType() == DatasetType.DATASET) { 
			// Step 2: Metadata Mapping + Parameter Binding + Metadata Mapping Execution
			for (TemplateService t : templateService.getDatasetImportTemplateHeaders(ds.getTemplateId())) {
	
//				String mappingUuid = UUID.randomUUID().toString();
//				
////				System.out.println("H M UUID " + mappingUuid);
//				String d2rml = templateService.getEffectiveTemplateString(t);
//				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(mappingUuid).toString());
//				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_DATASET_URI@@}", resourceVocabulary.getDatasetAsResource(ds.getUuid()).toString());
//				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_ITEM_BASE@@}", resourceVocabulary.getItemBaseResource().toString());

				List<ParameterBinding> list  = new ArrayList<>();
				if (ds.getBinding() != null) { // this should be the case !
					for (ParameterBinding ep : ds.getBinding()) {
						list.add(ep);
					}
				}
				
				MappingUpdateRequest mr = new  MappingUpdateRequest();
				mr.setType(MappingType.HEADER);
				mr.setName(t.getName());
//				mr.setParameters(t.getParameterNames());
				List<DataServiceParameter> params = new ArrayList<>();
				for (ExtendedParameter ep : t.getParameters()) {
					params.add(new DataServiceParameter(ep.getName()));
				}
				mr.setParameters(params);
				mr.setActive(true);
				mr.setTemplateId(t.getId().toString());
				
				// Need to replace stuff in d2rml template string DATASET_UUID, MAPPING_UUID
//				MappingDocument mapping = mappingService.create(dc.getCurrentUser(), ds, MappingType.HEADER, t.getName(), t.getParameterNames(), d2rml, mappingUuid, t, true);
				MappingDocument mapping = mappingService.create(dc.getCurrentUser(), ds, mr);
				mapping.setActive(true);
//				
//				MappingInstance mi = mappingsService.createParameterBinding(dc.getCurrentUser(), mapping.getId().toString(), list);
	     		MappingContainer mc = mappingService.getContainer(dc, mapping.getId().toString());
	     		
	     		MappingInstanceUpdateRequest ur = new MappingInstanceUpdateRequest();
	     		ur.setBindings(list);
	     		MappingInstance mi = mappingService.createParameterBinding(mc, ur);
	     		mi.setActive(true);

//				MappingContainer mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
	     		mc = mappingService.getContainer(dc.getCurrentUser(),  mapping.getId(), mi.getId()); // !! should reread data from mongo to update
				dc.addMapping(mapping.getName(), mapping);
				
//				newMappingExecuteTask(mc, tdescr);
				TaskSpecification.getTaskSpecification(TaskType.MAPPING_EXECUTE).createTask(mc, null, tdescr);
			}
	
			// Step 3: Create Mapping for query and collection
			for (TemplateService t : templateService.getDatasetImportTemplateContents(ds.getTemplateId())) {
	
//				String mappingUuid = UUID.randomUUID().toString();
//	
////				System.out.println("H C UUID " + mappingUuid);
//				String d2rml = templateService.getEffectiveTemplateString(t);
//				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(mappingUuid).toString());
//				d2rml = d2rml.replace("{@@SAGE_TEMPLATE_ITEM_BASE@@}", resourceVocabulary.getItemBaseResource().toString());

				MappingUpdateRequest mr = new  MappingUpdateRequest();
				mr.setType(MappingType.CONTENT);
				mr.setName(t.getName());
//				mr.setParameters(t.getParameterNames());
				List<DataServiceParameter> params = new ArrayList<>();
				for (ExtendedParameter ep : t.getParameters()) {
					params.add(new DataServiceParameter(ep.getName()));
				}
				mr.setParameters(params);
				mr.setActive(true);
				mr.setTemplateId(t.getId().toString());
				
//				MappingDocument mapping = mappingService.create(dc.getCurrentUser(), ds, MappingType.CONTENT, t.getName(), t.getParameterNames(), d2rml, mappingUuid, t);
				MappingDocument mapping = mappingService.create(dc.getCurrentUser(), ds, mr);
				dc.addMapping(mapping.getName(), mapping);

			}
			
		} else if (ds.getDatasetType() == DatasetType.CATALOG) {

			TemplateService template = templateRepository.findById(ds.getTemplateId()).get();
			
//			System.out.println("TEMPLATE " + template.getId());
			
//			String mappingUuid = UUID.randomUUID().toString();
//
//			String d2rml = templateService.getEffectiveTemplateString(template);
//			d2rml = d2rml.replace("{@@SAGE_TEMPLATE_MAPPING_URI@@}", resourceVocabulary.getMappingAsResource(mappingUuid).toString());
			
			List<ParameterBinding> list  = new ArrayList<>();
			if (ds.getBinding() != null) { // this should be the case !
				for (ParameterBinding ep : ds.getBinding()) {
					list.add(ep);
				}
			}
			
			MappingUpdateRequest mr = new  MappingUpdateRequest();
			mr.setType(MappingType.CATALOG);
			mr.setName(template.getName());
//			mr.setParameters(template.getParameterNames());
			List<DataServiceParameter> params = new ArrayList<>();
			for (ExtendedParameter ep : template.getParameters()) {
				params.add(new DataServiceParameter(ep.getName()));
			}
			mr.setParameters(params);
			mr.setActive(true);
			mr.setTemplateId(template.getId().toString());
			
//			MappingDocument mapping = mappingService.create(dc.getCurrentUser(), ds, MappingType.CATALOG, template.getName(), template.getParameterNames(), null, d2rml, mappingUuid, template);			
			MappingDocument mapping = mappingService.create(dc.getCurrentUser(), ds, mr);
			
//			MappingInstance mi = mappingsService.createParameterBinding(dc.getCurrentUser(), mapping.getId().toString(), list);
     		MappingContainer mc = mappingService.getContainer(dc, mapping.getId().toString());
     		
     		MappingInstanceUpdateRequest ur = new MappingInstanceUpdateRequest();
     		ur.setBindings(list);
     		MappingInstance mi = mappingService.createParameterBinding(mc, ur);

//			MappingContainer mc = mappingsService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
     		mc = mappingService.getContainer(dc.getCurrentUser(),  mapping.getId().toString(), mi.getId().toString()); // !! should reread data from mongo to update
			
//			TaskDescription task = newMappingExecuteTask(mc, tdescr);
     		TaskDescription task = TaskSpecification.getTaskSpecification(TaskType.MAPPING_EXECUTE).createTask(mc, null, tdescr);
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
		
		Dataset ds = dc.getEnclosingObject();
		
		tdescr.setType(TaskType.DATASET_TEMPLATE_UPDATE);
		tdescr.setUserId(new ObjectId(dc.getCurrentUser().getId()));
		tdescr.setDatasetId(ds.getId());
		tdescr.setDescription(ds.getName());	
		
		//save to get id
		tdescr = newTask(tdescr, parent);

		if (ds.getDatasetType() == DatasetType.DATASET) { 
			for (TemplateService t : templateService.getDatasetImportTemplateHeaders(ds.getTemplateId())) {

				MappingDocument mapping = mappingRepository.findByDatasetIdAndName(dc.getDatasetId(), t.getName()).get();
				
				if (mapping.getType() == MappingType.HEADER) {
					for (MappingInstance mi : mapping.getInstances()) {
						for (ParameterBinding pb : mi.getBinding()) {
							if (pb.getName().equals("TITLE")) { // hardcoded: assume TITLE is dataset metadata title
								pb.setValue(ds.getName());
							}
						}

						mappingRepository.save(mapping);
						
						MappingContainer mc = mappingService.getContainer(dc.getCurrentUser(),  mapping.getId(), mi.getId()); // !! should reread data from mongo to update
						try {
//							newMappingExecuteTask(mc, tdescr);
							TaskSpecification.getTaskSpecification(TaskType.MAPPING_EXECUTE).createTask(mc, null, tdescr);
						} catch (Exception e) {
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
	
//	private void scheduleMappingsExecution(TaskDescription parent, DatasetContainer dc) throws TaskConflictException {
//		
//		for (MappingDocument mapping : datasetService.getMappings(dc.getCurrentUser(), dc.getDatasetId())) {
//			if (mapping.hasParameters()) { 
//				for (MappingInstance mi : mapping.getInstances()) {
//					MappingContainer mc = mappingsService.getContainer(dc, mapping.getId().toString(), mi.getId().toString());
//					
//			    	synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
//			    		try {
//			    			mc.checkIfActiveTask(parent);
//			    		} catch (TaskConflictException ex) {
//			    			throw new TaskConflictException("An instance of mapping '" + mc.getObject().getName() + "' is currently being executed");
//			    		}
//			    		
//		    			newMappingExecuteTask(mc, parent);
//			    	}
//				}
//			} else {
//				MappingContainer mc = mappingsService.getContainer(dc, mapping.getId().toString(), null);
//				
//				synchronized (mc.synchronizationString(TaskType.MAPPING_EXECUTE)) {
//		    		try {
//		    			mc.checkIfActiveTask(parent);
//		    		} catch (TaskConflictException ex) {
//		    			throw new TaskConflictException("Mapping " + mc.getObject().getName() + " is currently being executed");
//		    		}
//
//	    			newMappingExecuteTask(mc, parent);
//		    	}
//			}
//		}	
//	}

	
//	public TaskDescription newDatasetRepublishTask(Conflicts conflicts, DatasetContainer dc, TaskDescription parent) throws TaskConflictException, Exception {
//		synchronized (dc.synchronizationString()) {
//			dc.checkIfActiveTask(parent, conflicts.getOne(dc.getClass()));
//
//			TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_REPUBLISH));
//			
//			Properties props;
//			
//			props = new Properties();
//			props.put(ServiceProperties.PUBLISH_METADATA, true);
//			props.put(ServiceProperties.PUBLISH_CONTENT, true);
//			
////			newDatasetUnpublishTask(tspec, dc, props, tdescr);
//			createNewTask(TaskType.DATASET_UNPUBLISH, conflicts, dc, props, tdescr);
//			
//			props = new Properties();
//			props.put(ServiceProperties.PUBLISH_MODE, dc.getDataset().isPublik() ? ServiceProperties.PUBLISH_MODE_PUBLIC : ServiceProperties.PUBLISH_MODE_PRIVATE);
//			props.put(ServiceProperties.PUBLISH_METADATA, true);
//			props.put(ServiceProperties.PUBLISH_CONTENT, true);
//			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
//			props.put(ServiceProperties.TRIPLE_STORE, dc.getDatasetTripleStoreVirtuosoConfiguration());
//			
////			newDatasetPublishTask(tspec, dc, props, tdescr);
//			createNewTask(TaskType.DATASET_PUBLISH, conflicts, dc, props, tdescr);
//			
//			//save again to store children
//			taskRepository.save(tdescr);
//	
//			return tdescr;
//		}
//	}	

//	public TaskDescription newDatasetRepublishMetadataTask(Conflicts conflicts, DatasetContainer dc, TaskDescription parent) throws TaskConflictException, Exception {
//		synchronized (dc.synchronizationString()) {
//			dc.checkIfActiveTask(parent, conflicts.getOne(dc.getClass()));
//
//			TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_REPUBLISH_METADATA), parent);
//			
//			int isPublic = ServiceProperties.PUBLISH_MODE_CURRENT;
//			if (dc.getPublishState().getPublishState() == DatasetState.PUBLISHED_PUBLIC) {
//				isPublic = ServiceProperties.PUBLISH_MODE_PUBLIC;
//			} else if (dc.getPublishState().getPublishState() == DatasetState.PUBLISHED_PRIVATE) {
//				isPublic = ServiceProperties.PUBLISH_MODE_PRIVATE;
//			}
//	
//			Properties props;
//			
//			props = new Properties();
//			props.put(ServiceProperties.PUBLISH_METADATA, true);
//			props.put(ServiceProperties.PUBLISH_CONTENT, false);
//			
////			newDatasetUnpublishTask(tspec, dc, props, tdescr);
//			createNewTask(TaskType.DATASET_UNPUBLISH, conflicts, dc, props, tdescr);
//			
//			props = new Properties();
//			props.put(ServiceProperties.PUBLISH_MODE, isPublic);
//			props.put(ServiceProperties.PUBLISH_METADATA, true);
//			props.put(ServiceProperties.PUBLISH_CONTENT, false);
//			props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
//			props.put(ServiceProperties.TRIPLE_STORE, dc.getDatasetTripleStoreVirtuosoConfiguration());
//			
////			newDatasetPublishTask(tspec, dc, props, tdescr);
//			createNewTask(TaskType.DATASET_PUBLISH, conflicts, dc, props, tdescr);
//			
//			//save again to save children
//			taskRepository.save(tdescr);
//			
//			return tdescr;
//		}
//	}

//	public TaskDescription newDatasetMappingsExecuteTask(TaskSpecification tspec, DatasetContainer dc, TaskDescription parent) throws TaskConflictException {
//    	synchronized (dc.synchronizationString()) { // parent is not checked
//    		dc.checkIfActiveTask(parent, tspec.conflictingTasks());
//
//			TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_MAPPINGS_EXECUTE), parent);
//			
//			try {
//				scheduleMappingsExecution(tdescr, dc);
//			} catch (TaskConflictException tcex) {
//				cancelNewTask(tdescr);
//				throw tcex;
//			}
//			
//			//save again to store children
//			taskRepository.save(tdescr);
//	
//			return tdescr;
//    	}
//	}	
	
//	public TaskDescription newDatasetMappingsExecuteAndRepublishTask(TaskSpecification tspec, DatasetContainer dc, TaskDescription parent) throws TaskConflictException {
//    	synchronized (dc.synchronizationString(TaskType.DATASET_MAPPINGS_EXECUTE)) {
//    		if (getActiveTask(dc, TaskType.DATASET_MAPPINGS_EXECUTE) != null) {
//    			throw new TaskConflictException("The dataset mappings are already being executed.");
//    		} else {
//
//    			synchronized (dc.synchronizationString()) {
//    				dc.checkIfActiveTask(parent, TaskSpecification.getTaskSpecification(TaskType.DATASET_PUBLISH).conflictingTasks());
//
//					TaskDescription tdescr = newTask(new TaskDescription(dc, TaskType.DATASET_MAPPINGS_EXECUTE_AND_REPUBLISH), parent);
//					
//					try {
//						scheduleMappingsExecution(tdescr, dc);
//					} catch (TaskConflictException tcex) {
//						cancelNewTask(tdescr);
//						throw tcex;
//					}
//					
//					Properties props;
//					
//					props = new Properties();
//					props.put(ServiceProperties.PUBLISH_METADATA, true);
//					props.put(ServiceProperties.PUBLISH_CONTENT, true);
//					
//					newDatasetUnpublishTask(tspec, dc, props, tdescr);
//					
//					props = new Properties();
//					props.put(ServiceProperties.PUBLISH_MODE, dc.getDataset().isPublik() ? ServiceProperties.PUBLISH_MODE_PUBLIC : ServiceProperties.PUBLISH_MODE_PRIVATE);
//					props.put(ServiceProperties.PUBLISH_METADATA, true);
//					props.put(ServiceProperties.PUBLISH_CONTENT, true);
//					props.put(ServiceProperties.PUBLISH_ONLY_NEW_CONTENT, false);
//					props.put(ServiceProperties.TRIPLE_STORE, dc.getDatasetTripleStoreVirtuosoConfiguration());
//					
//					newDatasetPublishTask(tspec, dc, props, tdescr);
//					
//					//save again to store children
//					taskRepository.save(tdescr);
//
//					return tdescr;
//				}
//    		}
//		}
//	}	
	

	
	public TaskDescription newUserTaskDatasetCustomTask(Conflicts conflicts, UserTaskContainer uc, Properties props, TaskDescription parent) throws TaskConflictException, Exception {
		DatasetContainer dc = datasetService.getInDatasetContainer(uc);
		
		synchronized (dc.synchronizationString()) {
			dc.checkIfActiveTask(parent, conflicts.getOne(dc.getClass()));

			TaskDescription tdescr = newTask(new TaskDescription(uc, TaskType.USER_TASK_DATASET_RUN, props), parent);

			boolean skip = false;
			
			Dataset dataset = dc.getObject();
			if (dataset.getRemoteTripleStore() != null) {
				RunState runState = uc.getObject().checkRunState(fileSystemConfiguration.getId());
				Date lastUpdated = dataset.getRemoteTripleStore().findLastUpdated();
				
				Date lastRun = null;
				if (runState != null) {
					lastRun = runState.getRunStartedAt();
				}
				
				if (lastUpdated != null && lastRun != null && lastUpdated.before(lastRun)) {
					skip = true;
				}
			}

			if (!skip) {
				for (UserTaskDescription t : uc.getObject().getTasks()) {
					try {
						Properties tProps = new Properties();
						tProps.putAll(props);
						if (tProps.get(ServiceProperties.DATASET_GROUP) == null) { // is this check needed ? 
							tProps.put(ServiceProperties.DATASET_GROUP, t.getGroup() != null ? t.getGroup() : -1);
						}
						
						TaskSpecification.getTaskSpecification(t.getType()).createTask(dc, tProps, tdescr);
					} catch (Exception tcex) {
						cancelNewTask(tdescr);
						throw tcex;
					}	
				}
			} else {
				cancelNewTask(tdescr);
			}
			
			//to save children
			taskRepository.save(tdescr);
			
			return tdescr;
		}
	}
	
	public TaskDescription createNewTask(TaskSpecification tspec, ObjectContainer<?,?> oc, Properties props, TaskDescription parent) throws TaskConflictException, Exception {
		EnclosedObjectContainer<?,?,Dataset> doc = (EnclosedObjectContainer<?,?,Dataset>)oc; // for the time being...
				
		DatasetContainer dc = datasetService.getInDatasetContainer(doc);
		
		Conflicts conflicts = tspec.conflictingTasks;
		
//		System.out.println(" CREATING >>> " + tspec.getType());
		synchronized (dc.synchronizationString()) {
//			System.out.println("CHECKING ACTIVE 1 " + dc + " " + (parent != null ? parent.getId() : "null") + " " + conflicts.getOne(dc.getClass()));
			dc.checkIfActiveTask(parent, conflicts.getOne(dc.getClass()));
			
			for (Class<? extends ObjectContainer> anyContainer : conflicts.getAnyContainers()) {
				for (ObjectContainer<?,?> cc : dc.getActiveInnerContainers(anyContainer)) {
//					System.out.println("CHECKING ACTIVE 2 " + cc + " " + (parent != null ? parent.getId() : "null") + " " + conflicts.getAny(cc.getClass()));
					cc.checkIfActiveTask(parent, conflicts.getAny(cc.getClass()));
				}
			}
			
			Class<? extends ObjectContainer> oneContainer = conflicts.getOneContainer();
			if (oneContainer != null) {
//				System.out.println("CHECKING ACTIVE 3 " + oc + " " + (parent != null ? parent.getId() : "null") + " " + conflicts.getOne(oneContainer));
				oc.checkIfActiveTask(parent, conflicts.getOne(oneContainer));
			}

			TaskDescription tdescr = newTask(new TaskDescription(doc, tspec.getType(), props), parent);

			Fork<?>[] forkTypes = tspec.forkTypes;
			
			if (forkTypes != null) {
				for (Fork<? extends SpecificationDocument> fork : forkTypes) {
					if (fork.getContainerClass() != null) {
						for (TaskType t : fork.getTypes()) {
		    				List<ObjectContainer> list = dc.getActiveInnerContainers(fork.getContainerClass());
		    				List<ObjectContainer> effectiveList = new ArrayList<>();
		    				for (ObjectContainer ioc : list) {
				    			if (fork.getCondition() == null || fork.getCondition().evaluate(props, ioc)) {
				    				effectiveList.add(ioc);
				    			}
		    				}
		    				
		    				for (int i = 0; i < effectiveList.size(); i++) {
		    					ObjectContainer ioc = effectiveList.get(i);
		    					Properties newProps = (Properties)props.clone();
		    					if (ioc instanceof MappingContainer) {
		    						int currentGroup = ((MappingContainer)ioc).getObject().getGroup();
		    						int prevGroup = -1;
		    						if (i > 0) {
		    							prevGroup = ((MappingContainer)effectiveList.get(i - 1)).getObject().getGroup();
		    						}
		    						int nextGroup = -1;
		    						if (i < effectiveList.size() - 1) {
		    							nextGroup = ((MappingContainer)effectiveList.get(i + 1)).getObject().getGroup();
		    						}
		    								
			    					if (prevGroup != currentGroup) {
			    						newProps.put(ServiceProperties.IS_FIRST, true);
			    					} else {
			    						newProps.put(ServiceProperties.IS_FIRST, false);
			    					}
			    					if (nextGroup != currentGroup) {
			    						newProps.put(ServiceProperties.IS_LAST, true);
			    					} else {
			    						newProps.put(ServiceProperties.IS_LAST, false);
			    					}
		    					}
		    					
								try {
									TaskSpecification.getTaskSpecification(t).createTask(ioc, newProps, tdescr);
								} catch (Exception tcex) {
									tcex.printStackTrace();
									cancelNewTask(tdescr);
									throw tcex;
								}
				    		}
						}
						
						taskRepository.save(tdescr);
		
					} else {
						for (TaskType t : fork.getTypes()) {
			    			if (fork.getCondition() == null || fork.getCondition().evaluate(props, (ObjectContainer)oc)) {
								try {
									TaskSpecification.getTaskSpecification(t).createTask(oc, props, tdescr);
								} catch (Exception tcex) {
									tcex.printStackTrace();
									cancelNewTask(tdescr);
									throw tcex;
								}
			    			}
						}
						
						taskRepository.save(tdescr);
					}
				}
			}
			
			return tdescr;
		}
	}
	
	public TaskFunctionalInterface getMethod(TaskType type) {
		ContainerService<? extends SpecificationDocument, ? extends Response> service = getService(type);
		
		if (service == null) {
			return null;
		}
		
		if (type.toString().endsWith("_PUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).publish(tdescr, wsService);
		} else if (type.toString().endsWith("_UNPUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).unpublish(tdescr, wsService);
		} else if (type.toString().endsWith("_EXECUTE")) {
			return (tdescr, wsService) -> ((ExecutingService<?,?>)service).execute(tdescr, wsService);
		} else if (type.toString().endsWith("_CREATE")) {
			return (tdescr, wsService) -> ((CreatingService<?,?>)service).create(tdescr, wsService);
		} else if (type.toString().endsWith("_DESTROY")) {
			return (tdescr, wsService) -> ((CreatingService<?,?>)service).destroy(tdescr, wsService);
		}
		
		return null;
	}
	
	public PreFunctionalInterface getPreMethod(TaskType type) {
		ContainerService<? extends SpecificationDocument, ? extends Response> service = getService(type);
		
		if (service == null) {
			return null;
		}

		String t = type.toString(); 
		if (t.endsWith("_DATASET_RUN")) {
			return (tdescr, wsService) -> ((RunningService<?,?>)service).preRun(tdescr, wsService);
		} else if (t.endsWith("_PUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).prePublish(tdescr, wsService);
		} else if (t.endsWith("_UNPUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).preUnpublish(tdescr, wsService);
		} 
 		
		return null;
	}
	
	public PreFunctionalInterface getPostSuccessMethod(TaskType type) throws TaskFailureException{
		ContainerService<? extends SpecificationDocument, ? extends Response> service = getService(type);
		
		if (service == null) {
			return null;
		}
		
		String t = type.toString(); 
		if (t.endsWith("_DATASET_RUN")) {
			return (tdescr, wsService) -> ((RunningService<?,?>)service).postRunSuccess(tdescr, wsService);
		} else if (t.endsWith("_PUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).postPublishSuccess(tdescr, wsService);
		} else if (t.endsWith("_UNPUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).postUnpublishSuccess(tdescr, wsService);
		}
		
		return null;
	}
	
	public PreFunctionalInterface getPostFailMethod(TaskType type) {
		ContainerService<? extends SpecificationDocument,? extends Response> service = getService(type);
		
		if (service == null) {
			return null;
		}
		
		String t = type.toString(); 
		if (t.endsWith("_DATASET_RUN")) {
			return (tdescr, wsService) -> ((RunningService<?,?>)service).postRunFail(tdescr, wsService);
		} else if (t.endsWith("_PUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).postPublishFail(tdescr, wsService);
		} else if (t.endsWith("_UNPUBLISH")) {
			return (tdescr, wsService) -> ((PublishingService<?,?>)service).postUnpublishFail(tdescr, wsService);
		} 

		
		return null;
	}

	
	public ContainerService<?,?> getService(TaskType type) {
		DocumentType t = TaskType.getDocumentType(type);
		
		if (t == DocumentType.DATASET) {
			return datasetService;
		} else if (t == DocumentType.MAPPING) {
			return mappingService;
		} else if (t == DocumentType.ANNOTATOR) {
			return annotatorService;
		} else if (t == DocumentType.EMBEDDER) {
			return embedderService;
		} else if (t == DocumentType.INDEX) {
			return indexService;
		} else if (t == DocumentType.USER_TASK) {
			return userTaskService;
		} else if (t == DocumentType.DISTRIBUTION) {
			return distributionService;
		} else if (t == DocumentType.PAGED_ANNOTATION_VALIDATION) {
			return pavService;
		} else if (t == DocumentType.FILTER_ANNOTATION_VALIDATION) {
			return favService;
		} else if (t == DocumentType.FILE) {
			return fileService;
		}
		
		return null;
	}

}
