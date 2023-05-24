package ac.software.semantic.model.constants;

public enum TaskType {

	CATALOG_IMPORT_BY_TEMPLATE, // was IMPORT_CATALOG,
		IMPORT_CATALOG,

	DATASET_PUBLISH,
	DATASET_UNPUBLISH,
	DATASET_REPUBLISH,
	DATASET_FLIP_VISIBILITY,
	DATASET_REPUBLISH_METADATA,
	DATASET_CREATE_DISTRIBUTION,
	DATASET_INDEX,
	DATASET_UNINDEX,
	DATASET_MAPPINGS_EXECUTE,
	DATASET_MAPPINGS_EXECUTE_AND_REPUBLISH,
	DATASET_IMPORT_BY_TEMPLATE, // was IMPORT_DATASET,
		IMPORT_DATASET,
	DATASET_TEMPLATE_UPDATE, // was UPDATE_TEMPLATE_DATASET,
		UPDATE_TEMPLATE_DATASET,

	MAPPING_EXECUTE,
	
	MAPPING_CLEAR_LAST_EXECUTION, // not async task
	
	ANNOTATOR_EXECUTE,
	ANNOTATOR_PUBLISH,
	ANNOTATOR_UNPUBLISH,
	ANNOTATOR_REPUBLISH,
	
	ANNOTATOR_CLEAR_LAST_EXECUTION, // not async task
	
	EMBEDDER_EXECUTE,
	EMBEDDER_PUBLISH,
	EMBEDDER_UNPUBLISH,
	EMBEDDER_REPUBLISH,
	
	EMBEDDER_CLEAR_LAST_EXECUTION, // not async task
	
	PAGED_ANNOTATION_VALIDATION_RESUME,
	PAGED_ANNOTATION_VALIDATION_EXECUTE,
	PAGED_ANNOTATION_VALIDATION_PUBLISH,
	PAGED_ANNOTATION_VALIDATION_UNPUBLISH,
	PAGED_ANNOTATION_VALIDATION_REPUBLISH,
	
	PAGED_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, // not async task

	FILTER_ANNOTATION_VALIDATION_EXECUTE,
	FILTER_ANNOTATION_VALIDATION_PUBLISH,
	FILTER_ANNOTATION_VALIDATION_UNPUBLISH,
	FILTER_ANNOTATION_VALIDATION_REPUBLISH,
	
	FILTER_ANNOTATION_VALIDATION_CLEAR_LAST_EXECUTION, // not async task

	;
}
