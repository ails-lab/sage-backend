package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;

@Service
public class TaskSpecification {
	
    @Autowired
	private TaskService taskService;

    private static TaskService staticTaskService;
    
	@PostConstruct
	public void init() {
		staticTaskService = taskService;
	}
    
	private static Map<TaskType, TaskSpecification> taskMap = new HashMap<>();
	
	static {
		taskMap.put(TaskType.CATALOG_IMPORT_BY_TEMPLATE, new CatalogImportByTemplateTaskSpecification());
		taskMap.put(TaskType.IMPORT_CATALOG, new CatalogImportByTemplateTaskSpecification());
		
		taskMap.put(TaskType.DATASET_PUBLISH, new DatasetPublishTaskSpecification());
		taskMap.put(TaskType.DATASET_UNPUBLISH, new DatasetUnpublishTaskSpecification());
		taskMap.put(TaskType.DATASET_REPUBLISH, new DatasetRepublishTaskSpecification());
		taskMap.put(TaskType.DATASET_FLIP_VISIBILITY, new DatasetFlipVisibilityTaskSpecification());
		taskMap.put(TaskType.DATASET_REPUBLISH_METADATA, new DatasetRepublishMetadataTaskSpecification());
		taskMap.put(TaskType.DATASET_CREATE_DISTRIBUTION, new DatasetCreateDistributionTaskSpecification());
		taskMap.put(TaskType.DATASET_INDEX, new DatasetIndexTaskSpecification());
		taskMap.put(TaskType.DATASET_UNINDEX, new DatasetUnindexTaskSpecification());
		taskMap.put(TaskType.DATASET_MAPPINGS_EXECUTE, new DatasetMappingsExecuteTaskSpecification());
		taskMap.put(TaskType.DATASET_MAPPINGS_EXECUTE_AND_REPUBLISH, new DatasetMappingsExecuteAndRepublishTaskSpecification());
		taskMap.put(TaskType.DATASET_IMPORT_BY_TEMPLATE, new DatasetImportByTemplateTaskSpecification());
		taskMap.put(TaskType.IMPORT_DATASET, new DatasetImportByTemplateTaskSpecification());
		taskMap.put(TaskType.DATASET_TEMPLATE_UPDATE, new DatesetTemplateUpdateTaskSpecification());
		taskMap.put(TaskType.UPDATE_TEMPLATE_DATASET, new DatesetTemplateUpdateTaskSpecification());
		
		taskMap.put(TaskType.MAPPING_EXECUTE, new MappingExecuteTaskSpecification());
		taskMap.put(TaskType.MAPPING_CLEAR_LAST_EXECUTION, new MappingClearLastExecutionTaskSpecification());
		
		taskMap.put(TaskType.ANNOTATOR_EXECUTE, new AnnotatorExecuteTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_PUBLISH, new AnnotatorPublishTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_UNPUBLISH, new AnnotatorUnpublishTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_REPUBLISH, new AnnotatorRepublishTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION, new AnnotatorClearLastExecutionTaskSpecification());
		
		taskMap.put(TaskType.EMBEDDER_EXECUTE, new EmbedderExecuteTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_PUBLISH, new EmbedderPublishTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_UNPUBLISH, new EmbedderUnpublishTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_REPUBLISH, new EmbedderRepublishTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_CLEAR_LAST_EXECUTION, new EmbedderClearLastExecutionTaskSpecification());
		
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, new PagedAnnotationValidationResumeTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, new PagedAnnotationValidationExecuteTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, new PagedAnnotationValidationPublishTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, new PagedAnnotationValidationUnpublishTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_REPUBLISH, new PagedAnnotationValidationRepublishTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, new PagedAnnotationValidationClearLastExecutionTaskSpecification());
		
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, new FilterAnnotationValidationExecuteTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, new FilterAnnotationValidationPublishTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, new FilterAnnotationValidationUnpublishTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH, new FilterAnnotationValidationRepublishTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, new FilterAnnotationValidationClearLastExecutionTaskSpecification());
	}
	
	public static TaskSpecification getTaskSpecification(TaskType type) {
		return taskMap.get(type);
	}

	protected TaskType taskType;
	protected NotificationType notificationType;
	protected NotificationChannel notificationChannel;
	protected boolean stoppable;
	
	protected String cancelledStateString; 
	protected String queuedStateString;

	public TaskType getType() {
		return taskType;
	}
	
	public NotificationType getNotificationType() {
		return notificationType;
	}
	
	public NotificationChannel getNotificationChannel() {
		return notificationChannel;
	}
	
	public boolean isStoppable() {
		return stoppable;
	}
	
	public TaskMonitor createTaskMonitor(TaskDescription tdescr, WebSocketService wsService) {
		if (notificationType == NotificationType.execute) {
			return new ExecuteMonitor(notificationChannel, tdescr.getContainer(), wsService, tdescr.getCreateTime());
		} else if (notificationType != null) {
			return new GenericMonitor(notificationChannel, tdescr.getContainer(), wsService, tdescr.getCreateTime());
		}
		
		return null;
	}

	public String getCancelledStateAsString() {
		return cancelledStateString;
	}
	
	public String getQueuedStateAsString() {
		return queuedStateString;
	}

	// CAUTION: if abstract @PostConstruct does not work
	public TaskDescription createTask(ObjectContainer ec) throws TaskConflictException {
		return null;
	}
	
	public String toString() {
		return taskType.toString();
	}
	
	public List<TaskType> conflictingTasks() {
		return new ArrayList<>();
	}
	
	static class CatalogImportByTemplateTaskSpecification extends TaskSpecification {

		CatalogImportByTemplateTaskSpecification() {
			this.taskType = TaskType.CATALOG_IMPORT_BY_TEMPLATE;
			this.notificationType = null;
			this.notificationChannel = null;
			this.stoppable = false;
		}
	}
	
	static class DatasetPublishTaskSpecification extends TaskSpecification {

		DatasetPublishTaskSpecification() {
			this.taskType = TaskType.DATASET_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
		}
	}
	
	static class DatasetUnpublishTaskSpecification extends TaskSpecification {

		DatasetUnpublishTaskSpecification() {
			this.taskType = TaskType.DATASET_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class DatasetRepublishTaskSpecification extends TaskSpecification {

		DatasetRepublishTaskSpecification() {
			this.taskType = TaskType.DATASET_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class DatasetFlipVisibilityTaskSpecification extends TaskSpecification {

		DatasetFlipVisibilityTaskSpecification() {
			this.taskType = TaskType.DATASET_FLIP_VISIBILITY;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class DatasetRepublishMetadataTaskSpecification extends TaskSpecification {

		DatasetRepublishMetadataTaskSpecification() {
			this.taskType = TaskType.DATASET_REPUBLISH_METADATA;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = true;
		}
	}
		
	static class DatasetCreateDistributionTaskSpecification extends TaskSpecification {

		DatasetCreateDistributionTaskSpecification() {
			this.taskType = TaskType.DATASET_CREATE_DISTRIBUTION;
			this.notificationType = NotificationType.createDistribution;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = true;
		}
	}
	
	static class DatasetIndexTaskSpecification extends TaskSpecification {

		DatasetIndexTaskSpecification() {
			this.taskType = TaskType.DATASET_INDEX;
			this.notificationType = NotificationType.index;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = true;
		}
	}
	
	static class DatasetUnindexTaskSpecification extends TaskSpecification {

		DatasetUnindexTaskSpecification() {
			this.taskType = TaskType.DATASET_UNINDEX;
			this.notificationType = NotificationType.index;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class DatasetMappingsExecuteTaskSpecification extends TaskSpecification {

		DatasetMappingsExecuteTaskSpecification() {
			this.taskType = TaskType.DATASET_MAPPINGS_EXECUTE;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class DatasetMappingsExecuteAndRepublishTaskSpecification extends TaskSpecification {

		DatasetMappingsExecuteAndRepublishTaskSpecification() {
			this.taskType = TaskType.DATASET_MAPPINGS_EXECUTE_AND_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class DatasetImportByTemplateTaskSpecification extends TaskSpecification {

		DatasetImportByTemplateTaskSpecification() {
			this.taskType = TaskType.DATASET_IMPORT_BY_TEMPLATE;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class DatesetTemplateUpdateTaskSpecification extends TaskSpecification {

		DatesetTemplateUpdateTaskSpecification() {
			this.taskType = TaskType.DATASET_TEMPLATE_UPDATE;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
		}
	}
	
	static class MappingExecuteTaskSpecification extends TaskSpecification {

		MappingExecuteTaskSpecification() {
			this.taskType = TaskType.MAPPING_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.mapping;
			this.stoppable = true;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newMappingExecuteTask((MappingContainer)oc);
		}
	}
	
	static class MappingClearLastExecutionTaskSpecification extends TaskSpecification {

		MappingClearLastExecutionTaskSpecification() {
			this.taskType = TaskType.MAPPING_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.mapping;
			this.stoppable = false;
		}
	}
	
	static class AnnotatorExecuteTaskSpecification extends TaskSpecification {

		AnnotatorExecuteTaskSpecification() {
			this.taskType = TaskType.ANNOTATOR_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = true;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newAnnotatorExecuteTask(this, (AnnotatorContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, TaskType.ANNOTATOR_PUBLISH, TaskType.ANNOTATOR_UNPUBLISH, TaskType.ANNOTATOR_REPUBLISH } );
		}

	}
	
	static class AnnotatorPublishTaskSpecification extends TaskSpecification {

		AnnotatorPublishTaskSpecification() {
			this.taskType = TaskType.ANNOTATOR_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newAnnotatorPublishTask(this, (AnnotatorContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, TaskType.ANNOTATOR_PUBLISH, TaskType.ANNOTATOR_UNPUBLISH, TaskType.ANNOTATOR_REPUBLISH } );
		}
	}
	
	static class AnnotatorUnpublishTaskSpecification extends TaskSpecification {

		AnnotatorUnpublishTaskSpecification() {
			this.taskType = TaskType.ANNOTATOR_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newAnnotatorUnpublishTask(this, (AnnotatorContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, TaskType.ANNOTATOR_PUBLISH, TaskType.ANNOTATOR_UNPUBLISH, TaskType.ANNOTATOR_REPUBLISH } );
		}

	}
	
	static class AnnotatorRepublishTaskSpecification extends TaskSpecification {

		AnnotatorRepublishTaskSpecification() {
			this.taskType = TaskType.ANNOTATOR_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, TaskType.ANNOTATOR_PUBLISH, TaskType.ANNOTATOR_UNPUBLISH, TaskType.ANNOTATOR_REPUBLISH } );
		}

	}
	
	static class AnnotatorClearLastExecutionTaskSpecification extends TaskSpecification {

		AnnotatorClearLastExecutionTaskSpecification() {
			this.taskType = TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
		}
		
		
	}
	
	static class EmbedderExecuteTaskSpecification extends TaskSpecification {

		EmbedderExecuteTaskSpecification() {
			this.taskType = TaskType.EMBEDDER_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = true;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newEmbedderExecuteTask(this, (EmbedderContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, TaskType.EMBEDDER_PUBLISH, TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_REPUBLISH } );
		}

	}
	
	static class EmbedderPublishTaskSpecification extends TaskSpecification {

		EmbedderPublishTaskSpecification() {
			this.taskType = TaskType.EMBEDDER_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newEmbedderPublishTask(this, (EmbedderContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, TaskType.EMBEDDER_PUBLISH, TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_REPUBLISH } );
		}
	}
	
	static class EmbedderUnpublishTaskSpecification extends TaskSpecification {

		EmbedderUnpublishTaskSpecification() {
			this.taskType = TaskType.EMBEDDER_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newEmbedderUnpublishTask(this, (EmbedderContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, TaskType.EMBEDDER_PUBLISH, TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_REPUBLISH } );
		}

	}
	
	static class EmbedderRepublishTaskSpecification extends TaskSpecification {

		EmbedderRepublishTaskSpecification() {
			this.taskType = TaskType.EMBEDDER_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, TaskType.EMBEDDER_PUBLISH, TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_REPUBLISH } );
		}

	}
	
	static class EmbedderClearLastExecutionTaskSpecification extends TaskSpecification {

		EmbedderClearLastExecutionTaskSpecification() {
			this.taskType = TaskType.EMBEDDER_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
		}
	}
	
	static class PagedAnnotationValidationResumeTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationResumeTaskSpecification() {
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_RESUME;
			this.notificationType = NotificationType.lifecycle;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newPagedAnnotationValidationResumeTask((PagedAnnotationValidationContainer)oc);
		}
	}
	
	static class PagedAnnotationValidationExecuteTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationExecuteTaskSpecification() {
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = true;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newPagedAnnotationValidationExecuteTask((PagedAnnotationValidationContainer)oc);
		}

	}
	
	static class PagedAnnotationValidationPublishTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationPublishTaskSpecification() {
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newPagedAnnotationValidationPublishTask((PagedAnnotationValidationContainer)oc);
		}
	}
	
	static class PagedAnnotationValidationUnpublishTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationUnpublishTaskSpecification() {
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newPagedAnnotationValidationUnpublishTask((PagedAnnotationValidationContainer)oc);
		}

	}
	
	static class PagedAnnotationValidationRepublishTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationRepublishTaskSpecification() {
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
		}
	}
	
	static class PagedAnnotationValidationClearLastExecutionTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationClearLastExecutionTaskSpecification() {
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
		}
	}
	
	static class FilterAnnotationValidationExecuteTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationExecuteTaskSpecification() {
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = true;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newFilterAnnotationValidationExecuteTask(this, (FilterAnnotationValidationContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH } );
		}

	}
	
	static class FilterAnnotationValidationPublishTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationPublishTaskSpecification() {
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newFilterAnnotationValidationPublishTask(this, (FilterAnnotationValidationContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH } );
		}

	}
	
	static class FilterAnnotationValidationUnpublishTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationUnpublishTaskSpecification() {
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
		}
		
		@Override
		public TaskDescription createTask(ObjectContainer oc) throws TaskConflictException {
			return staticTaskService.newFilterAnnotationValidationUnpublishTask(this, (FilterAnnotationValidationContainer)oc);
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH } );
		}

	}
	
	static class FilterAnnotationValidationRepublishTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationRepublishTaskSpecification() {
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
		}
		
		@Override
		public List<TaskType> conflictingTasks() {
			return Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH } );
		}
	}
	
	static class FilterAnnotationValidationClearLastExecutionTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationClearLastExecutionTaskSpecification() {
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
		}
	}

}
