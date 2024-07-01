package ac.software.semantic.model.constants.type;

public enum DatasetScope {
	
	COLLECTION, // should be removed
	VOCABULARY,
	ANNOTATION,
	ALIGNMENT,
	SHACL,
	D2RML,
	ANNOTATOR,
	COMPARATOR,
	INDEX,
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
		} else if (type.equals("SHACL")) {
			return SHACL;
		} else if (type.equals("D2RML")) {
			return D2RML;
		} else if (type.equals("ANNOTATOR")) {
			return ANNOTATOR;
		} else if (type.equals("COMPARATOR")) {
			return COMPARATOR;
		} else if (type.equals("INDEX")) {
			return INDEX;
		}
		
		return null;
	}
}
