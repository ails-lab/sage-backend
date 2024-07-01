package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.notification.NotificationChannel;
import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.type.MappingType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.ClustererService.ClustererContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.DistributionService.DistributionContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.FileService.FileContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.IndexService.IndexContainer;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.UserTaskService.UserTaskContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.exception.TaskConflictException;
import ac.software.semantic.service.monitor.ExecuteMonitor;
import ac.software.semantic.service.monitor.GenericMonitor;
import ac.software.semantic.service.monitor.IndexMonitor;

@Service
public class TaskSpecification {
	
    @Autowired
	private TaskService taskService;

    private static TaskService staticTaskService;
    
	@PostConstruct
	public void init() {
		staticTaskService = taskService;
	}
    
	private static Map<TaskType, TaskSpecification> taskMap;
	private static Map<Class<? extends EnclosedObjectContainer<?,?,?>>, List<TaskType>> containerTasksMap;
	
	static {
		taskMap = new HashMap<>();
		
//		taskMap.put(TaskType.IMPORT_CATALOG, new CatalogImportByTemplateTaskSpecification());
		taskMap.put(TaskType.CATALOG_IMPORT_BY_TEMPLATE, new CatalogImportByTemplateTaskSpecification());
//		taskMap.put(TaskType.IMPORT_DATASET, new DatasetImportByTemplateTaskSpecification());
		taskMap.put(TaskType.DATASET_IMPORT_BY_TEMPLATE, new DatasetImportByTemplateTaskSpecification());

		taskMap.put(TaskType.UPDATE_TEMPLATE_DATASET, new DatesetTemplateUpdateTaskSpecification());
		taskMap.put(TaskType.DATASET_TEMPLATE_UPDATE, new DatesetTemplateUpdateTaskSpecification());
		
		taskMap.put(TaskType.DATASET_PUBLISH, new DatasetPublishTaskSpecification());
		taskMap.put(TaskType.DATASET_UNPUBLISH, new DatasetUnpublishTaskSpecification());
		
		taskMap.put(TaskType.DATASET_REPUBLISH, new DatasetRepublishTaskSpecification());
		taskMap.put(TaskType.DATASET_REPUBLISH_METADATA, new DatasetRepublishMetadataTaskSpecification());
		taskMap.put(TaskType.DATASET_RECREATE_INDEXES, new DatasetRecreateIndexesTaskSpecification());
		taskMap.put(TaskType.DATASET_RECREATE_DISTRIBUTIONS, new DatasetRecreateDistributionsTaskSpecification());
		taskMap.put(TaskType.DATASET_EXECUTE_MAPPINGS, new DatasetMappingsExecuteTaskSpecification());
		taskMap.put(TaskType.DATASET_EXECUTE_ANNOTATORS, new DatasetAnnotatorsExecuteTaskSpecification());
		taskMap.put(TaskType.DATASET_PUBLISH_ANNOTATORS, new DatasetAnnotatorsPublishTaskSpecification());
		taskMap.put(TaskType.DATASET_UNPUBLISH_ANNOTATORS, new DatasetAnnotatorsUnpublishTaskSpecification());
		taskMap.put(TaskType.DATASET_REPUBLISH_ANNOTATORS, new DatasetAnnotatorsRepublishTaskSpecification());
//		taskMap.put(TaskType.DATASET_MAPPINGS_PUBLISH, new DatasetMappingsPublishTaskSpecification());
		
		taskMap.put(TaskType.DATASET_PUBLISH_METADATA, new DatasetPublishMetadataTaskSpecification());
		
		taskMap.put(TaskType.DATASET_UNPUBLISH_METADATA, new DatasetUnpublishMetadataTaskSpecification());
		taskMap.put(TaskType.DATASET_UNPUBLISH_CONTENT, new DatasetUnpublishContentTaskSpecification());


		taskMap.put(TaskType.MAPPING_EXECUTE, new MappingExecuteTaskSpecification());
		taskMap.put(TaskType.MAPPING_CLEAR_LAST_EXECUTION, new MappingClearLastExecutionTaskSpecification());
		taskMap.put(TaskType.MAPPING_SHACL_VALIDATE_LAST_EXECUTION, new MappingShaclValidateLastExecutionTaskSpecification());
		
		taskMap.put(TaskType.MAPPING_PUBLISH, new MappingPublishTaskSpecification());
		
		taskMap.put(TaskType.FILE_PUBLISH, new FilePublishTaskSpecification());
		
		taskMap.put(TaskType.ANNOTATOR_EXECUTE, new AnnotatorExecuteTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION, new AnnotatorClearLastExecutionTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_PUBLISH, new AnnotatorPublishTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_UNPUBLISH, new AnnotatorUnpublishTaskSpecification());
		taskMap.put(TaskType.ANNOTATOR_REPUBLISH, new AnnotatorRepublishTaskSpecification());
		
		taskMap.put(TaskType.EMBEDDER_EXECUTE, new EmbedderExecuteTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_CLEAR_LAST_EXECUTION, new EmbedderClearLastExecutionTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_PUBLISH, new EmbedderPublishTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_UNPUBLISH, new EmbedderUnpublishTaskSpecification());
		taskMap.put(TaskType.EMBEDDER_REPUBLISH, new EmbedderRepublishTaskSpecification());

		taskMap.put(TaskType.CLUSTERER_EXECUTE, new ClustererExecuteTaskSpecification());
		taskMap.put(TaskType.CLUSTERER_CLEAR_LAST_EXECUTION, new ClustererClearLastExecutionTaskSpecification());
		taskMap.put(TaskType.CLUSTERER_PUBLISH, new ClustererPublishTaskSpecification());
		taskMap.put(TaskType.CLUSTERER_UNPUBLISH, new ClustererUnpublishTaskSpecification());
		taskMap.put(TaskType.CLUSTERER_REPUBLISH, new ClustererRepublishTaskSpecification());

		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, new PagedAnnotationValidationExecuteTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, new PagedAnnotationValidationClearLastExecutionTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, new PagedAnnotationValidationResumeTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, new PagedAnnotationValidationPublishTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, new PagedAnnotationValidationUnpublishTaskSpecification());
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_REPUBLISH, new PagedAnnotationValidationRepublishTaskSpecification());
		
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, new FilterAnnotationValidationExecuteTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, new FilterAnnotationValidationClearLastExecutionTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, new FilterAnnotationValidationPublishTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, new FilterAnnotationValidationUnpublishTaskSpecification());
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH, new FilterAnnotationValidationRepublishTaskSpecification());
		
		taskMap.put(TaskType.INDEX_CREATE, new IndexCreateTaskSpecification());
		taskMap.put(TaskType.INDEX_DESTROY, new IndexDestroyTaskSpecification());
		taskMap.put(TaskType.INDEX_RECREATE, new IndexRecreateTaskSpecification());

		taskMap.put(TaskType.DISTRIBUTION_CREATE, new DistributionCreateTaskSpecification());
		taskMap.put(TaskType.DISTRIBUTION_DESTROY, new DistributionDestroyTaskSpecification());
		taskMap.put(TaskType.DISTRIBUTION_RECREATE, new DistributionRecreateTaskSpecification());

		taskMap.put(TaskType.USER_TASK_DATASET_RUN, new UserTaskDatasetCustomTaskSpecification());
		
		
		containerTasksMap = new HashMap<>();
		
		for (Map.Entry<TaskType, TaskSpecification> entry : taskMap.entrySet()) {
			TaskType type = entry.getKey();
			Class<? extends EnclosedObjectContainer<?,?,?>> clazz = entry.getValue().objectClass;
			
			List<TaskType> tasks = containerTasksMap.get(clazz);
			if (tasks == null) {
				tasks = new ArrayList<>();
				containerTasksMap.put(clazz, tasks);
			}
			
			tasks.add(type);
		}

	}
	
	public static List<TaskType> getTaskTypesForContainerClass(Class<? extends ObjectContainer> clazz) {
		List<TaskType> res = containerTasksMap.get(clazz);
		if (res == null) {
			res = new ArrayList<>();
		}
		
		return res;
	}

	public static String getConflictMessage(TaskType type) {
		return TaskType.currentyActiveTaskMessage(type);
	}
	
	public static String getConflictMessage(TaskType type, String name) {
		String s = TaskType.currentyActiveTaskMessage(type);
		if (s != null) {
			s = s.replaceAll("@@", name);
		}
		return s ;
	}

	public static TaskSpecification getTaskSpecification(TaskType type) {
//		System.out.println(type);
		return taskMap.get(type);
	}
	
	public class Conflicts {
		protected Map<Class<? extends ObjectContainer>, List<TaskType>> any;
		protected Map<Class<? extends ObjectContainer>, List<TaskType>> one;
		
		Conflicts() {
			any = new LinkedHashMap<>();
			one = new LinkedHashMap<>();
		}
			
		private List<TaskType> emptyListIfNull(List<TaskType> res) {
			if (res == null) {
				return new ArrayList<TaskType>();
			} else {
				return res;
			}
		}
		
		public List<TaskType> putAny(Class<? extends ObjectContainer> clazz, List<TaskType> types) {
			return emptyListIfNull(any.put(clazz, types));
		}

		public List<TaskType> putOne(Class<? extends ObjectContainer> clazz, List<TaskType> types) {
			return emptyListIfNull(one.put(clazz, types));
		}
		
		public List<TaskType> getAny(Class<? extends ObjectContainer> clazz) {
			return emptyListIfNull(any.get(clazz));
		}

		public List<TaskType> getOne(Class<? extends ObjectContainer> clazz) {
			return emptyListIfNull(one.get(clazz));
		}
		
		public Set<Class<? extends ObjectContainer>> getAnyContainers() {
			return any.keySet();
		}

		public Class<? extends ObjectContainer> getOneContainer() {
			Set<Class<? extends ObjectContainer>> res = new HashSet<>();
			res.addAll(one.keySet());
			res.remove(DatasetContainer.class);
			
			if (res.isEmpty()) {
				return null;
			} else if (res.size() > 1) {
				throw new Error("Multiple one container conflict definitions");
			} else {
				return res.iterator().next();
			}
		}


	}

	public class Fork<D extends SpecificationDocument> {
		private Class<? extends EnclosedObjectContainer<D,?,?>> containerClass;
		private TaskType[] types;
		private Condition<D> condition;
		
		public Fork(TaskType[] types) {
			this(null, types, null);
		}

		public Fork(TaskType[] types, Condition<D> condition) {
			this(null, types, condition);
		}

		public Fork(Class<? extends EnclosedObjectContainer<D,?,?>> containerClass, TaskType[] types) {
			this(containerClass, types, null);
		}

		public Fork(Class<? extends EnclosedObjectContainer<D,?,?>> containerClass, TaskType[] types, Condition<D> condition) {
			this.containerClass = containerClass;
			this.types = types;
			this.condition = condition;
		}

		public Class<? extends ObjectContainer> getContainerClass() {
			return containerClass;
		}

		public TaskType[] getTypes() {
			return types;
		}
		
		public Condition<D> getCondition() {
			return condition;
		}

	}
	
	
	
	protected Class<? extends EnclosedObjectContainer<?,?,?>> objectClass;
	
	protected TaskType taskType;
	protected NotificationType notificationType;
	protected NotificationChannel notificationChannel;
	protected boolean stoppable;
	
//	protected TaskType[] forkTypes;
//	protected Class<? extends EnclosedObjectContainer<?,?>> forkContainer;
	
	protected Fork<?>[] forkTypes;
	
	protected Conflicts conflictingTasks;
	
	protected String cancelledStateString; 
	protected String queuedStateString;
	
//	protected ContainerService<?> service;

	public TaskSpecification() {
		conflictingTasks = new Conflicts();
	}
	
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
		} else if (notificationChannel == NotificationChannel.index) {
			return new IndexMonitor(notificationChannel, tdescr.getContainer(), wsService, tdescr.getCreateTime());
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

	public TaskDescription createTask(ObjectContainer<?,?> oc) throws TaskConflictException, Exception  {
		return createTask(oc, null, null);
	}

	public TaskDescription createTask(ObjectContainer<?,?> oc, Properties props) throws TaskConflictException, Exception {
		return createTask(oc, props, null);
	}

	public TaskDescription createTask(ObjectContainer<?,?> oc, Properties props, TaskDescription parent) throws TaskConflictException, Exception {
		return staticTaskService.createNewTask(this,  oc, props, parent);
	}

	public String toString() {
		return taskType.toString();
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
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			this.forkTypes = new Fork[] { 
					new Fork<Dataset>(new TaskType [] { TaskType.DATASET_UNPUBLISH_METADATA }, 
							(props, container) -> { 
								DatasetContainer dc = (DatasetContainer)container;
						                  			
								return props.get(ServiceProperties.REPUBLISH) == Boolean.FALSE && (int)props.get(ServiceProperties.DATASET_GROUP) != -1 && dc.isPublished();
							}),
						                  
					new Fork<MappingDocument>(MappingContainer.class, new TaskType [] { TaskType.MAPPING_PUBLISH }, 
							(props, container) -> {
								MappingContainer mc = (MappingContainer)container;
					                    			  
					            Integer group = (Integer)props.get(ServiceProperties.DATASET_GROUP);
					            if (group != -1 && mc.getObject().getGroup() != group) {
						            return false;
					            }
					                      			  
					            if (!mc.isExecuted()) {
					            	return false;
					            }
					                      			  
					            if (props.get(ServiceProperties.CONTENT) == ServiceProperties.ALL) {
					            	return true;
					            } else if (props.get(ServiceProperties.CONTENT) == ServiceProperties.ONLY_NEW) {
					            	return !mc.isPublished() || mc.getObject().getType() == MappingType.HEADER;
					            } else if (props.get(ServiceProperties.CONTENT) == ServiceProperties.NONE) {
					            	return mc.getObject().getType() == MappingType.HEADER;
					            } else {
					            	return false;
					            }
							}),
					
					new Fork<FileDocument>(FileContainer.class, new TaskType [] { TaskType.FILE_PUBLISH },       
							(props, container) -> { 
								FileContainer fc =  (FileContainer)container;
					                    			  
								Integer group = (Integer)props.get(ServiceProperties.DATASET_GROUP);
					            if (group != -1 && fc.getObject().getGroup() != group) {
					            	return false;
					            }
					                      			  
					            if (!fc.isExecuted()) {
					            	return false;
					            }
					                    			  
					            if (props.get(ServiceProperties.CONTENT) == ServiceProperties.ALL) {
					            	return true;
					            } else if (props.get(ServiceProperties.CONTENT) == ServiceProperties.ONLY_NEW) {
					            	return !fc.isPublished();
					            } else {
					            	return false;
					            } 
							}),

					new Fork<Dataset>(new TaskType [] { TaskType.DATASET_PUBLISH_METADATA }, 
							(props, container) -> { 
								return props.get(ServiceProperties.METADATA) == ServiceProperties.ALL;  
							}) 
            };
			

			conflictingTasks.putOne(DatasetContainer.class, 
					             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					            		                        TaskType.DATASET_PUBLISH_METADATA,
								 				                TaskType.DATASET_UNPUBLISH,
					            		                        TaskType.DATASET_UNPUBLISH_METADATA,
					            		                        TaskType.DATASET_UNPUBLISH_CONTENT,
												                TaskType.DATASET_REPUBLISH, 
												                TaskType.DATASET_REPUBLISH_METADATA, 
												                TaskType.DATASET_EXECUTE_MAPPINGS, 
					                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
				                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
													  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
													  	        TaskType.MAPPING_PUBLISH,
						                       				  } ));
			conflictingTasks.putAny(IndexContainer.class,
                                 Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
				   			  	                                TaskType.INDEX_RECREATE, 
                      				                          } ));			
			conflictingTasks.putAny(DistributionContainer.class,
	                             Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
										  	                    TaskType.DISTRIBUTION_RECREATE, 
			                       				              } ));			
		}
	}
	
	static class DatasetUnpublishTaskSpecification extends TaskSpecification {

		DatasetUnpublishTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.forkTypes = new Fork[] { 
                    new Fork<Dataset>(new TaskType [] { TaskType.DATASET_UNPUBLISH_CONTENT  }, 
                    		(props, container) -> { 
                    			return props.get(ServiceProperties.CONTENT) == ServiceProperties.ALL;  
                    		}),
                    
                    new Fork<Dataset>(new TaskType [] { TaskType.DATASET_UNPUBLISH_METADATA }, 
                    		(props, container) -> { 
                    			return props.get(ServiceProperties.METADATA) == ServiceProperties.ALL; 
                    		}),
                    
					new Fork<Dataset>(new TaskType [] { TaskType.DATASET_PUBLISH_METADATA }, 
							(props, container) -> { 
								DatasetContainer dc = (DatasetContainer)container;
						                  			
								return props.get(ServiceProperties.REPUBLISH) == Boolean.FALSE && (int)props.get(ServiceProperties.DATASET_GROUP) != -1 && dc.isPublished();  
							}),                    
                    };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
		            		                        TaskType.DATASET_PUBLISH_METADATA,
					 				                TaskType.DATASET_UNPUBLISH,
		            		                        TaskType.DATASET_UNPUBLISH_METADATA,
		            		                        TaskType.DATASET_UNPUBLISH_CONTENT,
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
										  	        TaskType.MAPPING_PUBLISH,
			                       				  } ));
			conflictingTasks.putAny(IndexContainer.class,
                    Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
	   		   	                                   TaskType.INDEX_RECREATE, 
         				                          } ));			
			conflictingTasks.putAny(DistributionContainer.class,
                    Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
							  	                   TaskType.DISTRIBUTION_RECREATE, 
                      				              } ));	
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putAny(EmbedderContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, 
										  	        TaskType.EMBEDDER_PUBLISH, 
										  	        TaskType.EMBEDDER_UNPUBLISH,
										  	        TaskType.EMBEDDER_REPUBLISH,
			                       				  } ));

		}
	}
	
	static class DatasetRepublishTaskSpecification extends TaskSpecification {

		DatasetRepublishTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.forkTypes = new Fork[] { 
                    new Fork<Dataset>(new TaskType [] { TaskType.DATASET_UNPUBLISH, TaskType.DATASET_PUBLISH }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
		            		                        TaskType.DATASET_PUBLISH_METADATA,
					 				                TaskType.DATASET_UNPUBLISH,
		            		                        TaskType.DATASET_UNPUBLISH_METADATA,
		            		                        TaskType.DATASET_UNPUBLISH_CONTENT,
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
										  	        TaskType.MAPPING_PUBLISH,
			                       				  } ));
			conflictingTasks.putAny(IndexContainer.class,
                    Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
	   			  	                                TaskType.INDEX_RECREATE, 
         				                          } ));			
			conflictingTasks.putAny(DistributionContainer.class,
                    Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
							  	                    TaskType.DISTRIBUTION_RECREATE, 
                      				              } ));	
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putAny(EmbedderContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, 
										  	        TaskType.EMBEDDER_PUBLISH, 
										  	        TaskType.EMBEDDER_UNPUBLISH,
										  	        TaskType.EMBEDDER_REPUBLISH,
			                       				  } ));
		}
	}	
	

	static class DatasetRepublishMetadataTaskSpecification extends TaskSpecification {

		DatasetRepublishMetadataTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_REPUBLISH_METADATA;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = true;
			
//			this.forkTypes = new TaskType[] { TaskType.DATASET_UNPUBLISH, TaskType.DATASET_PUBLISH };
			this.forkTypes = new Fork[] { new Fork(new TaskType[] { TaskType.DATASET_UNPUBLISH_METADATA, TaskType.DATASET_PUBLISH_METADATA }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
//									                TaskType.DATASET_CREATE_DISTRIBUTION, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION, 
			                       				  } ));
			conflictingTasks.putAny(DistributionContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
										  	        TaskType.DISTRIBUTION_RECREATE, 
			                       				  } ));			
			
		}
	}
	
	static class DatasetPublishMetadataTaskSpecification extends TaskSpecification {

		DatasetPublishMetadataTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_PUBLISH_METADATA;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
									                TaskType.DATASET_UNPUBLISH_METADATA,
//									                TaskType.DATASET_CREATE_DISTRIBUTION, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
										  	        TaskType.MAPPING_PUBLISH,
			                       				  } ));
			conflictingTasks.putAny(DistributionContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
										  	        TaskType.DISTRIBUTION_RECREATE, 
			                       				  } ));			
			
		}
	}
	
	static class DatasetUnpublishMetadataTaskSpecification extends TaskSpecification {

		DatasetUnpublishMetadataTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_UNPUBLISH_METADATA;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
									                TaskType.DATASET_UNPUBLISH_METADATA,
//									                TaskType.DATASET_CREATE_DISTRIBUTION, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
										  	        TaskType.MAPPING_PUBLISH,
			                       				  } ));
			conflictingTasks.putAny(DistributionContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
										  	        TaskType.DISTRIBUTION_RECREATE, 
			                       				  } ));			
			
		}
	}
	
	static class DatasetUnpublishContentTaskSpecification extends TaskSpecification {

		DatasetUnpublishContentTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_UNPUBLISH_CONTENT;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
									                TaskType.DATASET_UNPUBLISH_METADATA,
//									                TaskType.DATASET_CREATE_DISTRIBUTION, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
										  	        TaskType.MAPPING_PUBLISH,
			                       				  } ));
			conflictingTasks.putAny(DistributionContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
										  	        TaskType.DISTRIBUTION_RECREATE, 
			                       				  } ));			
			
		}
	}
		
//	static class DatasetCreateDistributionTaskSpecification extends TaskSpecification {
//
//		DatasetCreateDistributionTaskSpecification() {
//			this.objectClass = DatasetContainer.class;
//			
//			this.taskType = TaskType.DATASET_CREATE_DISTRIBUTION;
//			this.notificationType = NotificationType.createDistribution;
//			this.notificationChannel = NotificationChannel.dataset;
//			this.stoppable = true;
//			
//			conflictingTasks.putOne(DatasetContainer.class, 
//		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
//					 				                TaskType.DATASET_UNPUBLISH, 
//									                TaskType.DATASET_REPUBLISH, 
//									                TaskType.DATASET_REPUBLISH_METADATA, 
//									                TaskType.DATASET_CREATE_DISTRIBUTION, 
//									                TaskType.DATASET_MAPPINGS_EXECUTE, 
//		                            	          } ));
//		}
//
//	}
	
	static class DatasetMappingsExecuteTaskSpecification extends TaskSpecification {

		DatasetMappingsExecuteTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_EXECUTE_MAPPINGS;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
//			this.forkTypes = new Fork[] { new Fork(MappingContainer.class, new TaskType[] { TaskType.MAPPING_EXECUTE }) };
			this.forkTypes = new Fork[] {new Fork<MappingDocument>(MappingContainer.class, new TaskType [] { TaskType.MAPPING_EXECUTE }, 
          		  (props, container) -> {
          			  int group = (int)props.get(ServiceProperties.DATASET_GROUP);
          			  int mappingGroup = ((MappingContainer)container).getObject().getGroup();
          		     
          			  if (group == -1 || mappingGroup == group) {
          				  return true;
          			  } else {
          				  return false;
          			  }
          		  })
			};
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
//									                TaskType.DATASET_CREATE_DISTRIBUTION, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION, 
			                       				  } ));
		}
	}
	
	static class DatasetAnnotatorsExecuteTaskSpecification extends TaskSpecification {

		DatasetAnnotatorsExecuteTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_EXECUTE_ANNOTATORS;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.forkTypes = new Fork[] {new Fork<AnnotatorDocument>(AnnotatorContainer.class, new TaskType [] { TaskType.ANNOTATOR_EXECUTE }, 
          		  (props, container) -> {
          			  List<String> tags = (List<String>)props.get(ServiceProperties.ANNOTATOR_TAG);
          			  List<String> annotatorTags = ((AnnotatorContainer)container).getObject().getTags();
          		     
          			  for (String t : tags) {
          				for (String at : annotatorTags) {
          					if (t.equals(at)) {
          						return true;
          					}
          				}
          			  }

       				  return false;
          		  })
			};
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
//									                TaskType.DATASET_CREATE_DISTRIBUTION, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
		                            	          } ));
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION,
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
		}
	}
	
	static class DatasetAnnotatorsPublishTaskSpecification extends TaskSpecification {

		DatasetAnnotatorsPublishTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_PUBLISH_ANNOTATORS;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			this.forkTypes = new Fork[] { 
						                  
					new Fork<AnnotatorDocument>(AnnotatorContainer.class, new TaskType [] { TaskType.ANNOTATOR_PUBLISH }, 
			          		  (props, container) -> {
			          			  List<String> tags = (List<String>)props.get(ServiceProperties.ANNOTATOR_TAG);
			          			  List<String> annotatorTags = ((AnnotatorContainer)container).getObject().getTags();
			          		     
			          			  for (String t : tags) {
			          				for (String at : annotatorTags) {
			          					if (t.equals(at)) {

											AnnotatorContainer ac = (AnnotatorContainer)container;
											
								            if (!ac.isExecuted()) {
								            	return false;
								            } else {
								            	return true;
								            }
			          					}
			          				}
			          			  }

			       				  return false;
			          		  }),
								
            };
			

			conflictingTasks.putOne(DatasetContainer.class, 
					             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					            		                        TaskType.DATASET_PUBLISH_METADATA,
								 				                TaskType.DATASET_UNPUBLISH,
					            		                        TaskType.DATASET_UNPUBLISH_METADATA,
					            		                        TaskType.DATASET_UNPUBLISH_CONTENT,
												                TaskType.DATASET_REPUBLISH, 
												                TaskType.DATASET_REPUBLISH_METADATA, 
												                TaskType.DATASET_EXECUTE_MAPPINGS, 
					                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
				                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
													  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
													  	        TaskType.MAPPING_PUBLISH,
						                       				  } ));
			conflictingTasks.putAny(IndexContainer.class,
                                 Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
				   			  	                                TaskType.INDEX_RECREATE, 
                      				                          } ));			
			conflictingTasks.putAny(DistributionContainer.class,
	                             Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
										  	                    TaskType.DISTRIBUTION_RECREATE, 
			                       				              } ));			
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION,
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));			
		}
	}
	
	static class DatasetAnnotatorsUnpublishTaskSpecification extends TaskSpecification {

		DatasetAnnotatorsUnpublishTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_UNPUBLISH_ANNOTATORS;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.forkTypes = new Fork[] { 
						                  
					new Fork<AnnotatorDocument>(AnnotatorContainer.class, new TaskType [] { TaskType.ANNOTATOR_UNPUBLISH }, 
			          		  (props, container) -> {
			          			  List<String> tags = (List<String>)props.get(ServiceProperties.ANNOTATOR_TAG);
			          			  List<String> annotatorTags = ((AnnotatorContainer)container).getObject().getTags();
			          		     
			          			  for (String t : tags) {
			          				for (String at : annotatorTags) {
			          					if (t.equals(at)) {

											AnnotatorContainer ac = (AnnotatorContainer)container;
											
								            if (!ac.isPublished()) {
								            	return false;
								            } else {
								            	return true;
								            }
			          					}
			          				}
			          			  }

			       				  return false;
			          		  }),
								
            };

			conflictingTasks.putOne(DatasetContainer.class, 
					             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					            		                        TaskType.DATASET_PUBLISH_METADATA,
								 				                TaskType.DATASET_UNPUBLISH,
					            		                        TaskType.DATASET_UNPUBLISH_METADATA,
					            		                        TaskType.DATASET_UNPUBLISH_CONTENT,
												                TaskType.DATASET_REPUBLISH, 
												                TaskType.DATASET_REPUBLISH_METADATA, 
												                TaskType.DATASET_EXECUTE_MAPPINGS, 
					                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
				                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
													  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
													  	        TaskType.MAPPING_PUBLISH,
						                       				  } ));
			conflictingTasks.putAny(IndexContainer.class,
                                 Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
				   			  	                                TaskType.INDEX_RECREATE, 
                      				                          } ));			
			conflictingTasks.putAny(DistributionContainer.class,
	                             Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
										  	                    TaskType.DISTRIBUTION_RECREATE, 
			                       				              } ));			
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION,
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));			
		}
	}

	static class DatasetAnnotatorsRepublishTaskSpecification extends TaskSpecification {

		DatasetAnnotatorsRepublishTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_REPUBLISH_ANNOTATORS;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
			this.forkTypes = new Fork[] { 
                    new Fork<Dataset>(new TaskType [] { TaskType.DATASET_UNPUBLISH_ANNOTATORS, TaskType.DATASET_PUBLISH_ANNOTATORS }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
		            		                        TaskType.DATASET_PUBLISH_METADATA,
					 				                TaskType.DATASET_UNPUBLISH,
		            		                        TaskType.DATASET_UNPUBLISH_METADATA,
		            		                        TaskType.DATASET_UNPUBLISH_CONTENT,
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
									                TaskType.DATASET_EXECUTE_MAPPINGS, 
									                TaskType.DATASET_EXECUTE_ANNOTATORS,
									                TaskType.DATASET_REPUBLISH_ANNOTATORS,
									                TaskType.DATASET_UNPUBLISH_ANNOTATORS,
									                TaskType.DATASET_PUBLISH_ANNOTATORS,
		                            	          } ));
			conflictingTasks.putAny(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION,
										  	        TaskType.MAPPING_PUBLISH,
			                       				  } ));
			conflictingTasks.putAny(IndexContainer.class,
                    Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
	   			  	                                TaskType.INDEX_RECREATE, 
         				                          } ));			
			conflictingTasks.putAny(DistributionContainer.class,
                    Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
							  	                    TaskType.DISTRIBUTION_RECREATE, 
                      				              } ));	
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putAny(EmbedderContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, 
										  	        TaskType.EMBEDDER_PUBLISH, 
										  	        TaskType.EMBEDDER_UNPUBLISH,
										  	        TaskType.EMBEDDER_REPUBLISH,
			                       				  } ));
		}
	}	
	
//	static class DatasetMappingsPublishTaskSpecification extends TaskSpecification {
//
//		DatasetMappingsPublishTaskSpecification() {
//			this.objectClass = DatasetContainer.class;
//			
//			this.taskType = TaskType.DATASET_MAPPINGS_PUBLISH;
//			this.notificationType = null;
//			this.notificationChannel = NotificationChannel.dataset;
//			this.stoppable = false;
//			
////			this.forkContainer = MappingContainer.class;
////			this.forkTypes = new TaskType [] { TaskType.MAPPING_EXECUTE } ;
//			this.forkTypes = new Fork[] { new Fork(MappingContainer.class, new TaskType[] { TaskType.MAPPING_PUBLISH }) };
//			
//			conflictingTasks.putOne(DatasetContainer.class, 
//		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
//					 				                TaskType.DATASET_UNPUBLISH, 
//									                TaskType.DATASET_REPUBLISH, 
//									                TaskType.DATASET_REPUBLISH_METADATA, 
////									                TaskType.DATASET_CREATE_DISTRIBUTION, 
//									                TaskType.DATASET_MAPPINGS_EXECUTE, 
//		                            	          } ));
//			conflictingTasks.putAny(MappingContainer.class,
//	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
//										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION, 
//			                       				  } ));
//		}
//	}
//	
//	static class DatasetFilesPublishTaskSpecification extends TaskSpecification {
//
//		DatasetFilesPublishTaskSpecification() {
//			this.objectClass = DatasetContainer.class;
//			
//			this.taskType = TaskType.DATASET_FILES_PUBLISH;
//			this.notificationType = null;
//			this.notificationChannel = NotificationChannel.dataset;
//			this.stoppable = false;
//			
////			this.forkContainer = MappingContainer.class;
////			this.forkTypes = new TaskType [] { TaskType.MAPPING_EXECUTE } ;
//			this.forkTypes = new Fork[] { new Fork(MappingContainer.class, new TaskType[] { TaskType.FILE_PUBLISH }) };
//			
//			conflictingTasks.putOne(DatasetContainer.class, 
//		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
//					 				                TaskType.DATASET_UNPUBLISH, 
//									                TaskType.DATASET_REPUBLISH, 
//									                TaskType.DATASET_REPUBLISH_METADATA, 
////									                TaskType.DATASET_CREATE_DISTRIBUTION, 
//									                TaskType.DATASET_MAPPINGS_EXECUTE, 
//		                            	          } ));
//			conflictingTasks.putAny(MappingContainer.class,
//	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
//										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION, 
//			                       				  } ));
//		}
//	}
	
	static class DatasetRecreateIndexesTaskSpecification extends TaskSpecification {

		DatasetRecreateIndexesTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_RECREATE_INDEXES;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
//			this.forkContainer = IndexContainer.class;
//			this.forkTypes = new TaskType [] { TaskType.INDEX_RECREATE } ;
			this.forkTypes = new Fork[] { new Fork(IndexContainer.class, new TaskType[] { TaskType.INDEX_RECREATE }) };

			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(IndexContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
					 				                TaskType.INDEX_DESTROY, 
									                TaskType.INDEX_RECREATE, 
		                            	          } ));
		}
	}
	
	static class DatasetRecreateDistributionsTaskSpecification extends TaskSpecification {

		DatasetRecreateDistributionsTaskSpecification() {
			this.objectClass = DatasetContainer.class;
			
			this.taskType = TaskType.DATASET_RECREATE_DISTRIBUTIONS;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.dataset;
			this.stoppable = false;
			
//			this.forkContainer = DistributionContainer.class;
//			this.forkTypes = new TaskType [] { TaskType.DISTRIBUTION_RECREATE } ;
			this.forkTypes = new Fork[] { new Fork(DistributionContainer.class, new TaskType[] { TaskType.DISTRIBUTION_RECREATE }) };

			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(DistributionContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
					 				                TaskType.DISTRIBUTION_DESTROY, 
									                TaskType.DISTRIBUTION_RECREATE, 
		                            	          } ));
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
			this.objectClass = MappingContainer.class;
			
			this.taskType = TaskType.MAPPING_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.mapping;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(MappingContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
					 				                TaskType.MAPPING_CLEAR_LAST_EXECUTION 
		                            	          } ));
		}
	}
	
	static class MappingPublishTaskSpecification extends TaskSpecification {

		MappingPublishTaskSpecification() {
			this.objectClass = MappingContainer.class;
			
			this.taskType = TaskType.MAPPING_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.mapping;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION, 
			                       				  } ));
		}

	}
	
	static class MappingClearLastExecutionTaskSpecification extends TaskSpecification {

		MappingClearLastExecutionTaskSpecification() {
			this.objectClass = MappingContainer.class;
			
			this.taskType = TaskType.MAPPING_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.mapping;
			this.stoppable = false;
		}
	}
	
	static class MappingShaclValidateLastExecutionTaskSpecification extends TaskSpecification {

		MappingShaclValidateLastExecutionTaskSpecification() {
			this.objectClass = MappingContainer.class;
			
			this.taskType = TaskType.MAPPING_SHACL_VALIDATE_LAST_EXECUTION;
			this.notificationType = NotificationType.validate;
			this.notificationChannel = NotificationChannel.mapping;
			this.stoppable = false;
			
//			conflictingTasks.putOne(DatasetContainer.class, 
//		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
//									                TaskType.DATASET_REPUBLISH, 
//									                TaskType.DATASET_REPUBLISH_METADATA, 
//		                            	          } ));
			conflictingTasks.putOne(MappingContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
					 				                TaskType.MAPPING_CLEAR_LAST_EXECUTION 
		                            	          } ));
		}
	}
	
	static class FilePublishTaskSpecification extends TaskSpecification {

		FilePublishTaskSpecification() {
			this.objectClass = FileContainer.class;
			
			this.taskType = TaskType.FILE_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.file;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(MappingContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.MAPPING_EXECUTE, 
										  	        TaskType.MAPPING_CLEAR_LAST_EXECUTION, 
			                       				  } ));
		}

	}
	
	static class AnnotatorExecuteTaskSpecification extends TaskSpecification {

		AnnotatorExecuteTaskSpecification() {
			this.objectClass = AnnotatorContainer.class;
			
			this.taskType = TaskType.ANNOTATOR_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
		}
	}
	
	static class AnnotatorPublishTaskSpecification extends TaskSpecification {

		AnnotatorPublishTaskSpecification() {
			this.objectClass = AnnotatorContainer.class;
			
			this.taskType = TaskType.ANNOTATOR_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
		}

	}
	
	static class AnnotatorUnpublishTaskSpecification extends TaskSpecification {

		AnnotatorUnpublishTaskSpecification() {
			this.objectClass = AnnotatorContainer.class;
			
			this.taskType = TaskType.ANNOTATOR_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
		}

	}
	
	static class AnnotatorRepublishTaskSpecification extends TaskSpecification {

		AnnotatorRepublishTaskSpecification() {
			this.objectClass = AnnotatorContainer.class;
			
			this.taskType = TaskType.ANNOTATOR_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
			
//			this.forkTypes = new TaskType[] { TaskType.ANNOTATOR_UNPUBLISH, TaskType.ANNOTATOR_PUBLISH };
			this.forkTypes = new Fork[] { new Fork(new TaskType[] { TaskType.ANNOTATOR_UNPUBLISH, TaskType.ANNOTATOR_PUBLISH }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_EXECUTE, 
										  	        TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));

		}

	}
	
	static class AnnotatorClearLastExecutionTaskSpecification extends TaskSpecification {

		AnnotatorClearLastExecutionTaskSpecification() {
			this.objectClass = AnnotatorContainer.class;
			
			this.taskType = TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.annotator;
			this.stoppable = false;
		}
		
		
	}
	
	static class EmbedderExecuteTaskSpecification extends TaskSpecification {

		EmbedderExecuteTaskSpecification() {
			this.objectClass = EmbedderContainer.class;
			
			this.taskType = TaskType.EMBEDDER_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(EmbedderContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, 
										  	        TaskType.EMBEDDER_PUBLISH, 
										  	        TaskType.EMBEDDER_UNPUBLISH,
										  	        TaskType.EMBEDDER_REPUBLISH,
			                       				  } ));
		}

	}
	
	static class EmbedderPublishTaskSpecification extends TaskSpecification {

		EmbedderPublishTaskSpecification() {
			this.objectClass = EmbedderContainer.class;
			
			this.taskType = TaskType.EMBEDDER_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(EmbedderContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, 
										  	        TaskType.EMBEDDER_PUBLISH, 
										  	        TaskType.EMBEDDER_UNPUBLISH,
										  	        TaskType.EMBEDDER_REPUBLISH,
			                       				  } ));
		}
	}
	
	static class EmbedderUnpublishTaskSpecification extends TaskSpecification {

		EmbedderUnpublishTaskSpecification() {
			this.objectClass = EmbedderContainer.class;
			
			this.taskType = TaskType.EMBEDDER_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(EmbedderContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, 
										  	        TaskType.EMBEDDER_PUBLISH, 
										  	        TaskType.EMBEDDER_UNPUBLISH,
										  	        TaskType.EMBEDDER_REPUBLISH,
			                       				  } ));
		}
	}
	
	static class EmbedderRepublishTaskSpecification extends TaskSpecification {

		EmbedderRepublishTaskSpecification() {
			this.objectClass = EmbedderContainer.class;
			
			this.taskType = TaskType.EMBEDDER_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
			
//			this.forkTypes = new TaskType[] { TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_PUBLISH };
			this.forkTypes = new Fork[] { new Fork(new TaskType[] { TaskType.EMBEDDER_UNPUBLISH, TaskType.EMBEDDER_PUBLISH }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(EmbedderContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.EMBEDDER_EXECUTE, 
										  	        TaskType.EMBEDDER_PUBLISH, 
										  	        TaskType.EMBEDDER_UNPUBLISH,
										  	        TaskType.EMBEDDER_REPUBLISH,
			                       				  } ));
		}
	}
	
	static class EmbedderClearLastExecutionTaskSpecification extends TaskSpecification {

		EmbedderClearLastExecutionTaskSpecification() {
			this.objectClass = EmbedderContainer.class;
			
			this.taskType = TaskType.EMBEDDER_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.embedder;
			this.stoppable = false;
		}
	}
	
	
	static class ClustererExecuteTaskSpecification extends TaskSpecification {

		ClustererExecuteTaskSpecification() {
			this.objectClass = ClustererContainer.class;
			
			this.taskType = TaskType.CLUSTERER_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.clusterer;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(ClustererContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.CLUSTERER_EXECUTE, 
										  	        TaskType.CLUSTERER_PUBLISH, 
										  	        TaskType.CLUSTERER_UNPUBLISH,
										  	        TaskType.CLUSTERER_REPUBLISH,
			                       				  } ));
		}
	}
	
	static class ClustererPublishTaskSpecification extends TaskSpecification {

		ClustererPublishTaskSpecification() {
			this.objectClass = ClustererContainer.class;
			
			this.taskType = TaskType.CLUSTERER_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.clusterer;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(ClustererContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.CLUSTERER_EXECUTE, 
										  	        TaskType.CLUSTERER_PUBLISH, 
										  	        TaskType.CLUSTERER_UNPUBLISH,
										  	        TaskType.CLUSTERER_REPUBLISH,
			                       				  } ));
		}

	}
	
	static class ClustererUnpublishTaskSpecification extends TaskSpecification {

		ClustererUnpublishTaskSpecification() {
			this.objectClass = ClustererContainer.class;
			
			this.taskType = TaskType.CLUSTERER_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.clusterer;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(ClustererContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.CLUSTERER_PUBLISH, 
										  	        TaskType.CLUSTERER_UNPUBLISH,
										  	        TaskType.CLUSTERER_REPUBLISH,
			                       				  } ));
		}

	}
	
	static class ClustererRepublishTaskSpecification extends TaskSpecification {

		ClustererRepublishTaskSpecification() {
			this.objectClass = ClustererContainer.class;
			
			this.taskType = TaskType.CLUSTERER_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.clusterer;
			this.stoppable = false;
			
//			this.forkTypes = new TaskType[] { TaskType.ANNOTATOR_UNPUBLISH, TaskType.ANNOTATOR_PUBLISH };
			this.forkTypes = new Fork[] { new Fork(new TaskType[] { TaskType.CLUSTERER_UNPUBLISH, TaskType.CLUSTERER_PUBLISH }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(ClustererContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.CLUSTERER_EXECUTE, 
										  	        TaskType.CLUSTERER_PUBLISH, 
										  	        TaskType.CLUSTERER_UNPUBLISH,
										  	        TaskType.CLUSTERER_REPUBLISH,
			                       				  } ));

		}

	}
	
	static class ClustererClearLastExecutionTaskSpecification extends TaskSpecification {

		ClustererClearLastExecutionTaskSpecification() {
			this.objectClass = ClustererContainer.class;
			
			this.taskType = TaskType.CLUSTERER_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.clusterer;
			this.stoppable = false;
		}
		
		
	}
	
	static class PagedAnnotationValidationResumeTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationResumeTaskSpecification() {
			this.objectClass = PagedAnnotationValidationContainer.class;
			
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_RESUME;
			this.notificationType = NotificationType.lifecycle;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putOne(PagedAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));

		}

	}
	
	static class PagedAnnotationValidationExecuteTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationExecuteTaskSpecification() {
			this.objectClass = PagedAnnotationValidationContainer.class;
			
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(PagedAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
	}
	
	static class PagedAnnotationValidationPublishTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationPublishTaskSpecification() {
			this.objectClass = PagedAnnotationValidationContainer.class;
			
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putOne(PagedAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}

	}
	
	static class PagedAnnotationValidationUnpublishTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationUnpublishTaskSpecification() {
			this.objectClass = PagedAnnotationValidationContainer.class;
			
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(PagedAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
	}
	
	static class PagedAnnotationValidationRepublishTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationRepublishTaskSpecification() {
			this.objectClass = PagedAnnotationValidationContainer.class;
			
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
			
//			this.forkTypes = new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH };
			this.forkTypes = new Fork[] { new Fork(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putOne(PagedAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
	}
	
	static class PagedAnnotationValidationClearLastExecutionTaskSpecification extends TaskSpecification {

		PagedAnnotationValidationClearLastExecutionTaskSpecification() {
			this.objectClass = PagedAnnotationValidationContainer.class;
			
			this.taskType = TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.paged_annotation_validation;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(PagedAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, 
	                		                        TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
		
		
	}
	
	static class FilterAnnotationValidationExecuteTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationExecuteTaskSpecification() {
			this.objectClass = FilterAnnotationValidationContainer.class;
			
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE;
			this.notificationType = NotificationType.execute;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(FilterAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
	}
	
	static class FilterAnnotationValidationPublishTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationPublishTaskSpecification() {
			this.objectClass = FilterAnnotationValidationContainer.class;
			
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
			
			this.cancelledStateString = DatasetState.PUBLISHING_CANCELED.toString();
			this.queuedStateString = DatasetState.WAITING_TO_PUBLISH.toString();
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putOne(FilterAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
	}
	
	static class FilterAnnotationValidationUnpublishTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationUnpublishTaskSpecification() {
			this.objectClass = FilterAnnotationValidationContainer.class;
			
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH;
			this.notificationType = NotificationType.publish;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putOne(FilterAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
	}
	
	static class FilterAnnotationValidationRepublishTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationRepublishTaskSpecification() {
			this.objectClass = FilterAnnotationValidationContainer.class;
			
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
			
//			this.forkTypes = new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH };
			this.forkTypes = new Fork[] { new Fork( new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putAny(AnnotatorContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.ANNOTATOR_PUBLISH, 
										  	        TaskType.ANNOTATOR_UNPUBLISH,
										  	        TaskType.ANNOTATOR_REPUBLISH,
			                       				  } ));
			conflictingTasks.putOne(FilterAnnotationValidationContainer.class,
	                 Arrays.asList(new TaskType[] { TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, 
	                		                        TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION,
			                       				  } ));
		}
	}
	
	static class FilterAnnotationValidationClearLastExecutionTaskSpecification extends TaskSpecification {

		FilterAnnotationValidationClearLastExecutionTaskSpecification() {
			this.objectClass = FilterAnnotationValidationContainer.class;
			
			this.taskType = TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.filter_annotation_validation;
			this.stoppable = false;
		}
	}
	
	static class IndexCreateTaskSpecification extends TaskSpecification {

		IndexCreateTaskSpecification() {
			this.objectClass = IndexContainer.class;
			
			this.taskType = TaskType.INDEX_CREATE;
			this.notificationType = NotificationType.create;
			this.notificationChannel = NotificationChannel.index;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(IndexContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
					 				                TaskType.INDEX_DESTROY, 
									                TaskType.INDEX_RECREATE, 
		                            	          } ));
		}
	}
	
	static class IndexDestroyTaskSpecification extends TaskSpecification {

		IndexDestroyTaskSpecification() {
			this.objectClass = IndexContainer.class;
			
			this.taskType = TaskType.INDEX_DESTROY;
			this.notificationType = NotificationType.create;
			this.notificationChannel = NotificationChannel.index;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(IndexContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
					 				                TaskType.INDEX_DESTROY, 
									                TaskType.INDEX_RECREATE, 
		                            	          } ));
		}
	}
	
	static class IndexRecreateTaskSpecification extends TaskSpecification {

		IndexRecreateTaskSpecification() {
			this.objectClass = IndexContainer.class;
			
			this.taskType = TaskType.INDEX_RECREATE;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.index;
			this.stoppable = false;
			
//			this.forkTypes = new TaskType[] { TaskType.INDEX_DESTROY, TaskType.INDEX_CREATE } ;
			this.forkTypes = new Fork[] { new Fork(new TaskType[] { TaskType.INDEX_DESTROY, TaskType.INDEX_CREATE }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(IndexContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.INDEX_CREATE, 
					 				                TaskType.INDEX_DESTROY, 
									                TaskType.INDEX_RECREATE, 
		                            	          } ));
		}
	}
	
	static class DistributionCreateTaskSpecification extends TaskSpecification {

		DistributionCreateTaskSpecification() {
			this.objectClass = DistributionContainer.class;
			
			this.taskType = TaskType.DISTRIBUTION_CREATE;
			this.notificationType = NotificationType.create;
			this.notificationChannel = NotificationChannel.distribution;
			this.stoppable = true;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(DistributionContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
					 				                TaskType.DISTRIBUTION_DESTROY, 
									                TaskType.DISTRIBUTION_RECREATE, 
		                            	          } ));
		}
	}
	
	static class DistributionDestroyTaskSpecification extends TaskSpecification {

		DistributionDestroyTaskSpecification() {
			this.objectClass = DistributionContainer.class;
			
			this.taskType = TaskType.DISTRIBUTION_DESTROY;
			this.notificationType = NotificationType.create;
			this.notificationChannel = NotificationChannel.distribution;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(DistributionContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
				                                    TaskType.DISTRIBUTION_DESTROY, 
				                                    TaskType.DISTRIBUTION_RECREATE,  
		                            	          } ));
		}
	}
	
	static class DistributionRecreateTaskSpecification extends TaskSpecification {

		DistributionRecreateTaskSpecification() {
			this.objectClass = DistributionContainer.class;
			
			this.taskType = TaskType.DISTRIBUTION_RECREATE;
			this.notificationType = null;
			this.notificationChannel = NotificationChannel.distribution;
			this.stoppable = false;
			
//			this.forkTypes = new TaskType[] { TaskType.DISTRIBUTION_DESTROY, TaskType.DISTRIBUTION_CREATE } ;
			this.forkTypes = new Fork[] { new Fork(new TaskType[] { TaskType.DISTRIBUTION_DESTROY, TaskType.DISTRIBUTION_CREATE }) };
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(DistributionContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DISTRIBUTION_CREATE, 
                                                    TaskType.DISTRIBUTION_DESTROY, 
                                                    TaskType.DISTRIBUTION_RECREATE,
		                            	          } ));
		}
	}
	
	static class UserTaskDatasetCustomTaskSpecification extends TaskSpecification {

		UserTaskDatasetCustomTaskSpecification() {
			this.objectClass = UserTaskContainer.class;
			
			this.taskType = TaskType.USER_TASK_DATASET_RUN;
			this.notificationType = NotificationType.run;
			this.notificationChannel = NotificationChannel.user_task;
			this.stoppable = false;
			
			conflictingTasks.putOne(DatasetContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.DATASET_PUBLISH, 
					 				                TaskType.DATASET_UNPUBLISH, 
									                TaskType.DATASET_REPUBLISH, 
									                TaskType.DATASET_REPUBLISH_METADATA, 
		                            	          } ));
			conflictingTasks.putOne(UserTaskContainer.class, 
		             Arrays.asList(new TaskType[] { TaskType.USER_TASK_DATASET_RUN, 
		                            	          } ));		
		}

		@Override
		public TaskDescription createTask(ObjectContainer<?,?> oc, Properties props, TaskDescription parent) throws TaskConflictException, Exception {
			return staticTaskService.newUserTaskDatasetCustomTask(this.conflictingTasks, (UserTaskContainer)oc, props, parent);
		}
	
	}

}
