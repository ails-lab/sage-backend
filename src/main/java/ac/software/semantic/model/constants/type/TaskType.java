package ac.software.semantic.model.constants.type;

import java.util.HashMap;
import java.util.Map;

import ac.software.semantic.service.TaskDetail;

public enum TaskType {

	CATALOG_IMPORT_BY_TEMPLATE, // was IMPORT_CATALOG,
		IMPORT_CATALOG,
	DATASET_IMPORT_BY_TEMPLATE, // was IMPORT_DATASET,
		IMPORT_DATASET,
	DATASET_TEMPLATE_UPDATE, // was UPDATE_TEMPLATE_DATASET,
		UPDATE_TEMPLATE_DATASET,

	DATASET_PUBLISH,
	DATASET_PUBLISH_METADATA,
	DATASET_UNPUBLISH,
	DATASET_UNPUBLISH_METADATA,
	DATASET_UNPUBLISH_CONTENT,
	DATASET_REPUBLISH,
	DATASET_EXECUTE_MAPPINGS, // was DATASET_MAPPINGS_EXECUTE
	DATASET_RECREATE_INDEXES,
	DATASET_RECREATE_DISTRIBUTIONS,

	DATASET_EXECUTE_ANNOTATORS, 
	DATASET_PUBLISH_ANNOTATORS,
	DATASET_UNPUBLISH_ANNOTATORS,
	DATASET_REPUBLISH_ANNOTATORS, 
	
	DATASET_FLIP_VISIBILITY, //kept for mongo compatibility
	DATASET_CREATE_DISTRIBUTION, //kept for mongo compatibility
	DATASET_INDEX,           //kept for mongo compatibility
	DATASET_UNINDEX,         //kept for mongo compatibility
//	DATASET_UNINDEX_INDEX,   //kept for mongo compatibility
	DATASET_MAPPINGS_EXECUTE, //kept for mongo compatibility replaced by above
	DATASET_MAPPINGS_EXECUTE_AND_REPUBLISH, //kept for mongo compatibility
	DATASET_REPUBLISH_METADATA, //kept for mongo compatibility
		
	MAPPING_EXECUTE,
	MAPPING_CLEAR_LAST_EXECUTION, // not async task
	MAPPING_PUBLISH,
	MAPPING_SHACL_VALIDATE_LAST_EXECUTION,
	
	FILE_EXECUTE,              // not async task
	FILE_CLEAR_LAST_EXECUTION, // not async task // not used
	FILE_PUBLISH,
	FILE_SHACL_VALIDATE_LAST_EXECUTION,
	
	ANNOTATOR_EXECUTE,
	ANNOTATOR_CLEAR_LAST_EXECUTION, // not async task
	ANNOTATOR_PUBLISH,
	ANNOTATOR_UNPUBLISH,
	ANNOTATOR_REPUBLISH,

	CLUSTERER_EXECUTE,
	CLUSTERER_CLEAR_LAST_EXECUTION, // not async task
	CLUSTERER_PUBLISH,
	CLUSTERER_UNPUBLISH,
	CLUSTERER_REPUBLISH,

	EMBEDDER_EXECUTE,
	EMBEDDER_CLEAR_LAST_EXECUTION, // not async task
	EMBEDDER_PUBLISH,
	EMBEDDER_UNPUBLISH,
	EMBEDDER_REPUBLISH,
	
	PAGED_ANNOTATION_VALIDATION_EXECUTE,
	PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, // not async task
	PAGED_ANNOTATION_VALIDATION_PUBLISH,
	PAGED_ANNOTATION_VALIDATION_UNPUBLISH,
	PAGED_ANNOTATION_VALIDATION_REPUBLISH,
	PAGED_ANNOTATION_VALIDATION_RESUME,
	
	FILTER_ANNOTATION_VALIDATION_EXECUTE,
	FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, // not async task
	FILTER_ANNOTATION_VALIDATION_PUBLISH,
	FILTER_ANNOTATION_VALIDATION_UNPUBLISH,
	FILTER_ANNOTATION_VALIDATION_REPUBLISH,
	
	INDEX_CREATE,
	INDEX_DESTROY,
	INDEX_RECREATE,

	DISTRIBUTION_CREATE,
	DISTRIBUTION_DESTROY,
	DISTRIBUTION_RECREATE,

	USER_TASK_DATASET_RUN,

	;
	
	private static Map<TaskType, TaskDetail> taskMap = new HashMap<>();
	
	static {
		taskMap.put(TaskType.DATASET_PUBLISH, new TaskDetail(DocumentType.DATASET, OperationType.PUBLISH));
		taskMap.put(TaskType.DATASET_PUBLISH_METADATA, new TaskDetail(DocumentType.DATASET, OperationType.PUBLISH, TargetType.METADATA));
		taskMap.put(TaskType.DATASET_UNPUBLISH, new TaskDetail(DocumentType.DATASET, OperationType.UNPUBLISH));
		taskMap.put(TaskType.DATASET_UNPUBLISH_METADATA, new TaskDetail(DocumentType.DATASET, OperationType.UNPUBLISH, TargetType.METADATA));
		taskMap.put(TaskType.DATASET_UNPUBLISH_CONTENT, new TaskDetail(DocumentType.DATASET, OperationType.UNPUBLISH, TargetType.CONTENT));
		taskMap.put(TaskType.DATASET_REPUBLISH, new TaskDetail(DocumentType.DATASET, OperationType.REPUBLISH));
//		taskMap.put(TaskType.DATASET_REPUBLISH_METADATA, new TaskDetail(DocumentType.DATASET, OperationType.REPUBLISH, TargetType.METADATA));
		taskMap.put(TaskType.DATASET_EXECUTE_MAPPINGS, new TaskDetail(DocumentType.DATASET, OperationType.EXECUTE, TargetType.MAPPINGS));
		taskMap.put(TaskType.DATASET_RECREATE_INDEXES, new TaskDetail(DocumentType.DATASET, OperationType.RECREATE, TargetType.INDEXES));
		taskMap.put(TaskType.DATASET_RECREATE_DISTRIBUTIONS, new TaskDetail(DocumentType.DATASET, OperationType.RECREATE, TargetType.DISTRIBUTIONS));

		taskMap.put(TaskType.DATASET_EXECUTE_ANNOTATORS, new TaskDetail(DocumentType.DATASET, OperationType.EXECUTE, TargetType.ANNOTATORS));
		taskMap.put(TaskType.DATASET_PUBLISH_ANNOTATORS, new TaskDetail(DocumentType.DATASET, OperationType.EXECUTE, TargetType.ANNOTATORS));
		taskMap.put(TaskType.DATASET_REPUBLISH_ANNOTATORS, new TaskDetail(DocumentType.DATASET, OperationType.EXECUTE, TargetType.ANNOTATORS));
		taskMap.put(TaskType.DATASET_UNPUBLISH_ANNOTATORS, new TaskDetail(DocumentType.DATASET, OperationType.EXECUTE, TargetType.ANNOTATORS));

		taskMap.put(TaskType.MAPPING_EXECUTE, new TaskDetail(DocumentType.MAPPING, OperationType.EXECUTE));
		taskMap.put(TaskType.MAPPING_CLEAR_LAST_EXECUTION, new TaskDetail(DocumentType.MAPPING, OperationType.CLEAR, TargetType.LAST_EXECUTION));
		taskMap.put(TaskType.MAPPING_PUBLISH, new TaskDetail(DocumentType.MAPPING, OperationType.PUBLISH));
		taskMap.put(TaskType.MAPPING_SHACL_VALIDATE_LAST_EXECUTION, new TaskDetail(DocumentType.MAPPING, OperationType.SHACL_VALIDATE, TargetType.LAST_EXECUTION));

		taskMap.put(TaskType.FILE_EXECUTE, new TaskDetail(DocumentType.FILE, OperationType.EXECUTE));
		taskMap.put(TaskType.FILE_CLEAR_LAST_EXECUTION, new TaskDetail(DocumentType.FILE, OperationType.CLEAR, TargetType.LAST_EXECUTION));
		taskMap.put(TaskType.FILE_PUBLISH, new TaskDetail(DocumentType.FILE, OperationType.PUBLISH));
		taskMap.put(TaskType.FILE_SHACL_VALIDATE_LAST_EXECUTION, new TaskDetail(DocumentType.FILE, OperationType.SHACL_VALIDATE, TargetType.LAST_EXECUTION));

		taskMap.put(TaskType.ANNOTATOR_EXECUTE, new TaskDetail(DocumentType.ANNOTATOR, OperationType.EXECUTE));
		taskMap.put(TaskType.ANNOTATOR_CLEAR_LAST_EXECUTION, new TaskDetail(DocumentType.ANNOTATOR, OperationType.CLEAR, TargetType.LAST_EXECUTION));
		taskMap.put(TaskType.ANNOTATOR_PUBLISH, new TaskDetail(DocumentType.ANNOTATOR, OperationType.PUBLISH));
		taskMap.put(TaskType.ANNOTATOR_UNPUBLISH, new TaskDetail(DocumentType.ANNOTATOR, OperationType.UNPUBLISH));
		taskMap.put(TaskType.ANNOTATOR_REPUBLISH, new TaskDetail(DocumentType.ANNOTATOR, OperationType.REPUBLISH));
		
		taskMap.put(TaskType.EMBEDDER_EXECUTE, new TaskDetail(DocumentType.EMBEDDER, OperationType.EXECUTE));
		taskMap.put(TaskType.EMBEDDER_CLEAR_LAST_EXECUTION, new TaskDetail(DocumentType.EMBEDDER, OperationType.CLEAR, TargetType.LAST_EXECUTION));
		taskMap.put(TaskType.EMBEDDER_PUBLISH, new TaskDetail(DocumentType.EMBEDDER, OperationType.PUBLISH));
		taskMap.put(TaskType.EMBEDDER_UNPUBLISH, new TaskDetail(DocumentType.EMBEDDER, OperationType.UNPUBLISH));
		taskMap.put(TaskType.EMBEDDER_REPUBLISH, new TaskDetail(DocumentType.EMBEDDER, OperationType.REPUBLISH));
		
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_EXECUTE, new TaskDetail(DocumentType.PAGED_ANNOTATION_VALIDATION, OperationType.EXECUTE));
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, new TaskDetail(DocumentType.PAGED_ANNOTATION_VALIDATION, OperationType.CLEAR, TargetType.LAST_EXECUTION));
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_PUBLISH, new TaskDetail(DocumentType.PAGED_ANNOTATION_VALIDATION, OperationType.PUBLISH));
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_UNPUBLISH, new TaskDetail(DocumentType.PAGED_ANNOTATION_VALIDATION, OperationType.UNPUBLISH));
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_REPUBLISH, new TaskDetail(DocumentType.PAGED_ANNOTATION_VALIDATION, OperationType.REPUBLISH));
		taskMap.put(TaskType.PAGED_ANNOTATION_VALIDATION_RESUME, new TaskDetail(DocumentType.PAGED_ANNOTATION_VALIDATION, OperationType.RESUME));

		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_EXECUTE, new TaskDetail(DocumentType.FILTER_ANNOTATION_VALIDATION, OperationType.EXECUTE));
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, new TaskDetail(DocumentType.FILTER_ANNOTATION_VALIDATION, OperationType.CLEAR, TargetType.LAST_EXECUTION));
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_PUBLISH, new TaskDetail(DocumentType.FILTER_ANNOTATION_VALIDATION, OperationType.PUBLISH));
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_UNPUBLISH, new TaskDetail(DocumentType.FILTER_ANNOTATION_VALIDATION, OperationType.UNPUBLISH));
		taskMap.put(TaskType.FILTER_ANNOTATION_VALIDATION_REPUBLISH, new TaskDetail(DocumentType.FILTER_ANNOTATION_VALIDATION, OperationType.REPUBLISH));
		
		taskMap.put(TaskType.INDEX_CREATE, new TaskDetail(DocumentType.INDEX, OperationType.CREATE));
		taskMap.put(TaskType.INDEX_DESTROY, new TaskDetail(DocumentType.INDEX, OperationType.DESTROY));
		taskMap.put(TaskType.INDEX_RECREATE, new TaskDetail(DocumentType.INDEX, OperationType.RECREATE));

		taskMap.put(TaskType.DISTRIBUTION_CREATE, new TaskDetail(DocumentType.DISTRIBUTION, OperationType.CREATE));
		taskMap.put(TaskType.DISTRIBUTION_DESTROY, new TaskDetail(DocumentType.DISTRIBUTION, OperationType.DESTROY));
		taskMap.put(TaskType.DISTRIBUTION_RECREATE, new TaskDetail(DocumentType.DISTRIBUTION, OperationType.RECREATE));
		
		taskMap.put(TaskType.USER_TASK_DATASET_RUN, new TaskDetail(DocumentType.USER_TASK, OperationType.RUN));
	}
	
	public static boolean isExecute(TaskType t) {
		TaskDetail td = taskMap.get(t);
		return td.getOperationType() == OperationType.EXECUTE && td.getTargetType() == null; 
	}

	public static boolean isCreate(TaskType t) {
		TaskDetail td = taskMap.get(t);
		return td.getOperationType() == OperationType.CREATE && td.getTargetType() == null; 
	}
	
	public static boolean isDestroy(TaskType t) {
		TaskDetail td = taskMap.get(t);
		return td.getOperationType() == OperationType.DESTROY && td.getTargetType() == null; 
	}

	public static boolean isPublish(TaskType t) {
		TaskDetail td = taskMap.get(t);
		return td.getOperationType() == OperationType.PUBLISH && td.getTargetType() == null; 
	}

	public static boolean isUnpublish(TaskType t) {
		TaskDetail td = taskMap.get(t);
		return td.getOperationType() == OperationType.UNPUBLISH && td.getTargetType() == null; 
	}

	public static boolean isRun(TaskType t) {
		TaskDetail td = taskMap.get(t);
		return td.getOperationType() == OperationType.RUN && td.getTargetType() == null; 
	}
	
	public static DocumentType getDocumentType(TaskType type) {
		
		TaskDetail td = taskMap.get(type);
		
		if (td == null) {
			return null;
		}
		
		return td.getDocumentType();
	}
	
	public static OperationType getOperationType(TaskType type) {
		
		TaskDetail td = taskMap.get(type);
		
		if (td == null) {
			return null;
		}
		
		return td.getOperationType();
	}

	public static String currentyActiveTaskMessage(TaskType type) {
		
		TaskDetail td = taskMap.get(type);
		
		DocumentType dt = td.getDocumentType();
		OperationType ot = td.getOperationType();
		TargetType tt = td.getTargetType();
		
		return "There is a currently active " + OperationType.toPrettyString(ot) + (tt != null ? " " + TargetType.toPrettyString(tt) + " " : "") + " task for " + DocumentType.toPrettyString(dt) + " @@ ";
	}


}
