package ac.software.semantic.model.constants.type;

public enum DocumentType {
	
	DATASET, 
	MAPPING,
	ANNOTATOR,
	EMBEDDER,
	PAGED_ANNOTATION_VALIDATION,
	FILTER_ANNOTATION_VALIDATION,
	INDEX,
	USER_TASK,
	FILE,
	DISTRIBUTION,
	;

	public static String toPrettyString(DocumentType type) {
		if (type == DATASET) {
			return "dataset";
		} else if (type == MAPPING) {
			return "mapping";
		} else if (type == ANNOTATOR) {
			return "annotator";
		} else if (type == EMBEDDER) {
			return "embedder";
		} else if (type == PAGED_ANNOTATION_VALIDATION) {
			return "paged annotation validation";
		} else if (type == FILTER_ANNOTATION_VALIDATION) {
			return "filter annotation validatation";
		} else if (type == INDEX) {
			return "index";
		} else if (type == USER_TASK) {
			return "user task";
		} else if (type == FILE) {
			return "file";
		} else if (type == DISTRIBUTION) {
			return "distribution";
		}
		
		return null;
	}
}
