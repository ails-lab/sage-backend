package ac.software.semantic.model.constants;

public enum DatasetScope {
	
	COLLECTION, // should be removed
	VOCABULARY,
	ANNOTATION,
	ALIGNMENT,
	;
	
	public static DatasetScope get(String type) {
		if (type.equals("COLLECTION")) {
			return COLLECTION;
		} else if (type.equals("VOCABULARY")) {
			return VOCABULARY;
		} else if (type.equals("ANNOTATION")) {
			return ANNOTATION;
		} else if (type.equals("ALIGNMENT")) {
			return ALIGNMENT;
		}
		
		return null;
	}
}
