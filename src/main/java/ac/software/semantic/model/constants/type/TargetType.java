package ac.software.semantic.model.constants.type;

public enum TargetType {
	
	LAST_EXECUTION, 
	METADATA,
	CONTENT,
	MAPPINGS,
	ANNOTATORS,
	INDEXES,
	DISTRIBUTIONS,
	;

	public static String toPrettyString(TargetType type) {
		if (type == LAST_EXECUTION) {
			return "last execution";
		} else if (type == METADATA) {
			return "metadata";
		} else if (type == CONTENT) {
			return "content";
		} else if (type == MAPPINGS) {
			return "mappings";
		} else if (type == ANNOTATORS) {
			return "annotators";
		} else if (type == INDEXES) {
			return "indices";
		} else if (type == DISTRIBUTIONS) {
			return "distributions";
		}
		
		return null;
	}
}
