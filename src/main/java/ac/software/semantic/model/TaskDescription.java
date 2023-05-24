package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.model.constants.TaskState;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.payload.TaskResponse;
import ac.software.semantic.service.ObjectContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.TaskService.PostTaskExecution;
import ac.software.semantic.service.TaskSpecification;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Tasks")
public class TaskDescription {
	
	@Transient
	Logger logger = LoggerFactory.getLogger(TaskDescription.class);
	
	@Id
	private ObjectId id;
	
	private String system;
	
	private ObjectId databaseId;
	
	private String uuid;
	
	private Date createTime;
	private Date startTime;
	private Date endTime;
	private ObjectId userId;
	
	private ObjectId fileSystemConfigurationId;
	private ObjectId tripleStoreConfigurationId;
	private ObjectId elasticConfigurationId;
	
	@Transient
	@JsonIgnore
	private ObjectContainer container; 
	
	@Transient
	@JsonIgnore
	private PostTaskExecution pte;

	@Transient
	@JsonIgnore
	private List<TaskDescription> children;
	
	private List<ObjectId> childrenIds;
	private ObjectId parentId;

	@Transient
	@JsonIgnore
	private Properties properties; 

	@Transient
	@JsonIgnore
	private ListenableFuture<?> task;

	@Transient
	@JsonIgnore
	private TaskMonitor monitor;
	
	private ObjectId datasetId;
	private ObjectId mappingId;
	private ObjectId mappingInstanceId;
	
	private ObjectId annotatorId;
	private ObjectId embedderId;
	
	private ObjectId pagedAnnotationValidationId;
	private ObjectId filterAnnotationValidationId;
	
	private TaskType type;
	private String description;
	
	private TaskState state;
	
	@Transient
	@JsonIgnore
	private TaskSpecification tSpec;
	
//	private boolean stoppable;
	
//	@Autowired
//	@Transient
//	@JsonIgnore
//	TaskService taskService;

	public TaskDescription() { }
	
	public TaskDescription(ObjectContainer container, TaskType type) {
		this(container, type, null);
	}
	
	public TaskDescription(ObjectContainer container, TaskType type, Properties properties) { 
		this.container = container;
		this.type = type;
		this.properties = properties;
		
		userId = new ObjectId(container.getCurrentUser().getId());

		if (container instanceof MappingContainer) {
			MappingContainer mc = (MappingContainer)container;
			
			mappingId = mc.getMappingId();
			mappingInstanceId = mc.getMappingInstanceId();
			datasetId = mc.getDataset().getId();
			description = mc.getDataset().getName() + " / " + mc.getMappingDocument().getName();
			fileSystemConfigurationId = mc.getContainerFileSystemConfiguration().getId();
			
		} else if (container instanceof AnnotatorContainer) {
			AnnotatorContainer ac = (AnnotatorContainer)container;
			
			annotatorId = ac.getAnnotatorId();
			datasetId = ac.getDataset().getId();
			description = ac.getDataset().getName() + " / " + ac.getAnnotatorDocument().getAnnotator();
			fileSystemConfigurationId = ac.getContainerFileSystemConfiguration().getId();
			
		} else if (container instanceof EmbedderContainer) {
			EmbedderContainer ec = (EmbedderContainer)container;
			
			embedderId = ec.getEmbedderId();
			datasetId = ec.getDataset().getId();
			description = ec.getDataset().getName() + " / " + ec.getEmbedderDocument().getEmbedder();
			fileSystemConfigurationId = ec.getContainerFileSystemConfiguration().getId();
			
		} else if (container instanceof DatasetContainer) {
			DatasetContainer dc = (DatasetContainer)container;
			
			datasetId = dc.getDatasetId();
			description = dc.getDataset().getName();
			if (dc.getTripleStoreConfiguration() != null) {
				tripleStoreConfigurationId = dc.getTripleStoreConfiguration().getId();
			}
			if (dc.getElasticConfiguration() != null) {
				elasticConfigurationId = dc.getElasticConfiguration().getId();
			}
			
		} else if (container instanceof PagedAnnotationValidationContainer) {
			PagedAnnotationValidationContainer pavc = (PagedAnnotationValidationContainer)container;
					
			pagedAnnotationValidationId = pavc.getPagedAnnotationValidationId();
			datasetId = pavc.getDataset().getId();
			description = pavc.getDataset().getName() + " / " + pavc.getPagedAnnotationValidation().getName();
			fileSystemConfigurationId = pavc.getContainerFileSystemConfiguration().getId();

		} else if (container instanceof FilterAnnotationValidationContainer) {
			FilterAnnotationValidationContainer favc = (FilterAnnotationValidationContainer)container;
					
			filterAnnotationValidationId = favc.getFilterAnnotationValidationId();
			datasetId = favc.getDataset().getId();
			description = favc.getDataset().getName() + " / " + favc.getFilterAnnotationValidation().getName();
			fileSystemConfigurationId = favc.getContainerFileSystemConfiguration().getId();
			
		}
		
	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Future<?> getTask() {
		return task;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public TaskMonitor getMonitor() {
		return monitor;
	}

	public TaskMonitor createMonitor(WebSocketService wsService) {
		monitor = getTaskSpecification().createTaskMonitor(this, wsService);
		return monitor;
	}
	
//	public void setMonitor(TaskMonitor monitor) {
//		this.monitor = monitor;
//	}
	
	public List<TaskDescription> getChildren() {
		return children;
	}
	
	public boolean isGroup() {
		return children != null;
	}
	
	//tdescr should have an id
	public void addChild(TaskDescription tdescr) {
		if (children == null) {
			children = new ArrayList<>();
			childrenIds = new ArrayList<>();
		}
		children.add(tdescr);
		childrenIds.add(tdescr.getId());
	}
	
	public void setTask(ListenableFuture<?> task) {
		this.task = task;
	}
	
	public Throwable getFailureException() {
		if (monitor != null) {
			return monitor.getFailureException();
		} else {
			return null;
		}
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public TaskType getType() {
		return type;
	}

	public void setType(TaskType type) {
		this.type = type;
	}

	public synchronized boolean requestStop() {
		if (state == TaskState.STARTED || state == TaskState.QUEUED) {
			state = TaskState.STOPPING;
			return task.cancel(true);
		} else {
			return false;
		}
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public ObjectId getMappingId() {
		return mappingId;
	}

	public void setMappingId(ObjectId mappingId) {
		this.mappingId = mappingId;
	}

	public ObjectId getMappingInstanceId() {
		return mappingInstanceId;
	}

	public void setMappingInstanceId(ObjectId mappingInstanceId) {
		this.mappingInstanceId = mappingInstanceId;
	}
	
	public TaskResponse toTaskResponse() {
		TaskResponse task = new TaskResponse();
		task.setId(getId().toString());
		task.setUuid(getUuid());
		task.setStartTime(getStartTime());
		task.setEndTime(getEndTime());
		task.setFailureException(getFailureException());
		task.setDescription(getDescription());
		task.setType(getType());
		
		if (this.getType() == TaskType.MAPPING_EXECUTE || 
			this.getType() == TaskType.ANNOTATOR_EXECUTE || 
			this.getType() == TaskType.EMBEDDER_EXECUTE || 
			this.getType() == TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE ||
			this.getType() == TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE) {
			task.setStoppable(true);
		}
		
		Future<?> future = getTask();
		
		if (future != null) {
			
			if (future.isCancelled() || getFailureException() != null) {
				task.setState(TaskState.STOPPED);
			} else if (future.isDone()) {
				task.setState(TaskState.COMPLETED);
			} else {
				task.setState(state);
			}
		} else {
			task.setState(state);
		}
		
		return task;
	}

	public TaskState getState() {
		return state;
	}

	public void setState(TaskState state) {
		this.state = state;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public ObjectContainer getContainer() {
		return container;
	}

//	public void setContainer(ObjectContainer container) {
//		this.container = container;
//	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public String getSystemc() {
		return system;
	}

	public void setSystem(String system) {
		this.system = system;
	}

	public ObjectId getParentId() {
		return parentId;
	}

	public void setParentId(ObjectId parentId) {
		this.parentId = parentId;
	}		
	
	public List<ObjectId> getChildrenIds() {
		return childrenIds;
	}

	public void setChildrenIds(List<ObjectId> childrenIds) {
		this.childrenIds = childrenIds;
	}

	public PostTaskExecution getPostTaskExecution() {
		return pte;
	}

	public void setPostTaskExecution(PostTaskExecution pte) {
		this.pte = pte;
	}

	public ObjectId getAnnotatorId() {
		return annotatorId;
	}

	public void setAnnotatorId(ObjectId annotatorId) {
		this.annotatorId = annotatorId;
	}

	public ObjectId getPagedAnnotationValidationId() {
		return pagedAnnotationValidationId;
	}

	public void setPagedAnnotationValidationId(ObjectId pagedAnnotationValidationId) {
		this.pagedAnnotationValidationId = pagedAnnotationValidationId;
	}	
	
	public ObjectId getFilterAnnotationValidationId() {
		return filterAnnotationValidationId;
	}

	public void setFilterAnnotationValidationId(ObjectId filterAnnotationValidationId) {
		this.filterAnnotationValidationId = filterAnnotationValidationId;
	}	

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}
	
//	public String synchronizationString() {
//		if (type == TaskType.ANNOTATOR_EXECUTE) {
//			return (type.toString() + ":" + databaseConfigurationId.toString() + ":" + annotatorId.toString()).intern();
//		}
//		
//		return null;
//	}

	public ObjectId getEmbedderId() {
		return embedderId;
	}

	public void setEmbedderId(ObjectId embedderId) {
		this.embedderId = embedderId;
	}

	public ObjectId getTripleStoreConfigurationId() {
		return tripleStoreConfigurationId;
	}

	public void setTripleStoreConfigurationId(ObjectId tripleStoreConfigurationId) {
		this.tripleStoreConfigurationId = tripleStoreConfigurationId;
	}

	public ObjectId getElasticConfigurationId() {
		return elasticConfigurationId;
	}

	public void setElasticConfigurationId(ObjectId elasticConfigurationId) {
		this.elasticConfigurationId = elasticConfigurationId;
	}
	
	public TaskSpecification getTaskSpecification() {
		if (tSpec == null) {
			tSpec = TaskSpecification.getTaskSpecification(type);
		}
		
		return tSpec;
	}

}
