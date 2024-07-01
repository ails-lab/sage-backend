package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.DocumentType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.response.TaskResponse;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.ServiceProperties;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.ClustererService.ClustererContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.DistributionService.DistributionContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.FileService.FileContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.IndexService.IndexContainer;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.TaskService.PostTaskExecution;
import ac.software.semantic.service.TaskSpecification;
import ac.software.semantic.service.WebSocketService;
import ac.software.semantic.service.UserTaskService.UserTaskContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Tasks")
public class TaskDescription {
	
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
	private EnclosedObjectContainer<?,?,?> container; 
	
	@Transient
	@JsonIgnore
	private PostTaskExecution pte;

	@Transient
	@JsonIgnore
	private List<TaskDescription> children;
	
	private List<ObjectId> childrenIds;
	
	@Transient
	@JsonIgnore
	private TaskDescription parent;
	
	private ObjectId parentId;
	
	private ObjectId rootId;

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
	
	private ObjectId fileId;
	
	private ObjectId annotatorId;
	private ObjectId embedderId;
	private ObjectId clustererId;
	
	private ObjectId indexId;
	private ObjectId userTaskId;
	private ObjectId distributionId;
	
	private ObjectId pagedAnnotationValidationId;
	private ObjectId filterAnnotationValidationId;
	
	private TaskType type;
	private String description;
	
	private Integer datasetGroup;
	
	private TaskState state;
	
//	private DocumentType documentType;
	
	@Transient
	@JsonIgnore
	private TaskSpecification tSpec;
	
//	private boolean stoppable;
	
//	@Autowired
//	@Transient
//	@JsonIgnore
//	TaskService taskService;

	public TaskDescription() { }
	
	public TaskDescription(EnclosedObjectContainer<?,?,?> container, TaskType type) {
		this(container, type, null);
	}
	
	public TaskDescription(EnclosedObjectContainer<?,?,?> container, TaskType type, Properties properties) { 
		this.container = container;
		this.type = type;
		this.properties = properties;
		
		userId = new ObjectId(container.getCurrentUser().getId());
		datasetId = container.getEnclosingObject().getId();
		
		if (container instanceof MappingContainer) {
			MappingContainer mc = (MappingContainer)container;
			
			mappingId = mc.getObject().getId();
			mappingInstanceId = mc.getMappingInstanceId();
			
			description = mc.getEnclosingObject().getName() + " / " + mc.getObject().getName();
			fileSystemConfigurationId = mc.getContainerFileSystemConfiguration().getId();
		
		} else if (container instanceof FileContainer) {
			FileContainer fc = (FileContainer)container;
				
			fileId = fc.getObject().getId();
			description = fc.getEnclosingObject().getName() + " / " + fc.getObject().getName();
			fileSystemConfigurationId = fc.getContainerFileSystemConfiguration().getId();
				
		} else if (container instanceof AnnotatorContainer) {
			AnnotatorContainer ac = (AnnotatorContainer)container;
			
			annotatorId = ac.getObject().getId();
			description = ac.getEnclosingObject().getName() + " / " + ac.getObject().getAnnotator();
			fileSystemConfigurationId = ac.getContainerFileSystemConfiguration().getId();
			
		} else if (container instanceof EmbedderContainer) {
			EmbedderContainer ec = (EmbedderContainer)container;
			
			embedderId = ec.getObject().getId();
			description = ec.getEnclosingObject().getName() + " / " + ec.getObject().getEmbedder();
			fileSystemConfigurationId = ec.getContainerFileSystemConfiguration().getId();
		
		} else if (container instanceof ClustererContainer) {
			ClustererContainer ec = (ClustererContainer)container;
			
			clustererId = ec.getObject().getId();
			description = ec.getEnclosingObject().getName() + " / " + ec.getObject().getName();
			fileSystemConfigurationId = ec.getContainerFileSystemConfiguration().getId();
			
			
		} else if (container instanceof DatasetContainer) {
			DatasetContainer dc = (DatasetContainer)container;
			
			datasetGroup = (Integer)properties.get(ServiceProperties.DATASET_GROUP) != -1 ? (Integer)properties.get(ServiceProperties.DATASET_GROUP) : null;
			
			datasetId = dc.getDatasetId();
			description = dc.getEnclosingObject().getName() + (datasetGroup != null && (type != TaskType.DATASET_PUBLISH_METADATA && type != TaskType.DATASET_UNPUBLISH_METADATA) ? " / Group " + datasetGroup : "");
			fileSystemConfigurationId = dc.getFileSystemConfiguration().getId();
			if (dc.getTripleStoreConfiguration() != null) {
				tripleStoreConfigurationId = dc.getTripleStoreConfiguration().getId();
			}
			
			
			
//			if (properties != null && properties.get(ServiceProperties.INDEX_DOCUMENT) != null) {
//				IndexDocument idoc = ((IndexDocument)properties.get(ServiceProperties.INDEX_DOCUMENT));
//				
//				indexId = idoc.getId();
//				elasticConfigurationId = idoc.getElasticConfigurationId();
//			}
		} else if (container instanceof PagedAnnotationValidationContainer) {
			PagedAnnotationValidationContainer pavc = (PagedAnnotationValidationContainer)container;
					
			pagedAnnotationValidationId = pavc.getObject().getId();
			description = pavc.getEnclosingObject().getName() + " / " + pavc.getObject().getName();
			fileSystemConfigurationId = pavc.getContainerFileSystemConfiguration().getId();

		} else if (container instanceof FilterAnnotationValidationContainer) {
			FilterAnnotationValidationContainer favc = (FilterAnnotationValidationContainer)container;
					
			filterAnnotationValidationId = favc.getObject().getId();
			description = favc.getEnclosingObject().getName() + " / " + favc.getObject().getName();
			fileSystemConfigurationId = favc.getContainerFileSystemConfiguration().getId();
			
		} else if (container instanceof IndexContainer) {
			IndexContainer ic = (IndexContainer)container;
			
			indexId = ic.getObject().getId();
			description = ic.getEnclosingObject().getName() + " / " + ic.getIndexStructure().getIdentifier();
			elasticConfigurationId = ic.getContainerElasticConfiguration().getId();
		
		} else if (container instanceof UserTaskContainer) {
			UserTaskContainer uc = (UserTaskContainer)container;
			
			userTaskId = uc.getObject().getId();
			description = uc.getEnclosingObject().getName() + " / " + uc.getObject().getName();
			fileSystemConfigurationId = uc.getContainerFileSystemConfiguration().getId();
//			elasticConfigurationId = ic.getContainerElasticConfiguration().getId();

		} else if (container instanceof DistributionContainer) {
			DistributionContainer dc = (DistributionContainer)container;
			
			distributionId = dc.getObject().getId();
			description = dc.getEnclosingObject().getName() + " / " + dc.getObject().getName();
			fileSystemConfigurationId = dc.getContainerFileSystemConfiguration().getId();
//			elasticConfigurationId = ic.getContainerElasticConfiguration().getId();
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
			this.getType() == TaskType.CLUSTERER_EXECUTE ||
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

	public EnclosedObjectContainer<?,?,?> getContainer() {
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

	public String getSystem() {
		return system;
	}

	public void setSystem(String system) {
		this.system = system;
	}

	public ObjectId getTopId() {
		return rootId != null ? rootId : id;
	}

	public ObjectId getParentId() {
		return parentId;
	}

	public void setParentId(ObjectId parentId) {
		this.parentId = parentId;
	}		

	public void setParent(TaskDescription parent) {
		this.parent = parent;
		setParentId(parent.getId());
	}		
	
	public TaskDescription getParent() {
		return parent;
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

	public ObjectId getEmbedderId() {
		return embedderId;
	}

	public void setEmbedderId(ObjectId embedderId) {
		this.embedderId = embedderId;
	}
	
	public ObjectId getClustererId() {
		return embedderId;
	}

	public void setClustererId(ObjectId clustererId) {
		this.clustererId = clustererId;
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

	public ObjectId getIndexId() {
		return indexId;
	}

	public void setIndexId(ObjectId indexId) {
		this.indexId = indexId;
	}

	public ObjectId getUserTaskId() {
		return userTaskId;
	}

	public void setUserTaskId(ObjectId userTaskId) {
		this.userTaskId = userTaskId;
	}

//	public DocumentType getDocumentType() {
//		return documentType;
//	}

//	public void setDocumentType(DocumentType documentType) {
//		this.documentType = documentType;
//	}

	public ObjectId getFileId() {
		return fileId;
	}

	public void setFileId(ObjectId fileId) {
		this.fileId = fileId;
	}

	public ObjectId getRootId() {
		return rootId;
	}

	public void setRootId(ObjectId rootId) {
		this.rootId = rootId;
	}

	public ObjectId getDistributionId() {
		return distributionId;
	}

	public void setDistributionId(ObjectId distributionId) {
		this.distributionId = distributionId;
	}

}
