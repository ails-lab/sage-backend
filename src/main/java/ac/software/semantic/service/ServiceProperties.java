package ac.software.semantic.service;

public enum ServiceProperties {

//	PUBLISH_MODE,
//	PUBLISH_MODE_PRIVATE,
//	PUBLISH_MODE_PUBLIC,
//	PUBLISH_MODE_CURRENT,

	METADATA,
	CONTENT,
	
	REPUBLISH,
	
	DATASET_GROUP,
	ANNOTATOR_TAG,
	
	IS_FIRST,
	IS_LAST,
	
//	PUBLISH_ONLY_NEW_CONTENT,
	
	TRIPLE_STORE,

	ALL,
	NONE,
	ONLY_NEW,
	
	;
	
	public static ServiceProperties valueFromString(String v) {
		if (v.equalsIgnoreCase("ALL")) {
			return ALL;
		} else if (v.equalsIgnoreCase("NONE")) {
			return NONE;
		} else if (v.equalsIgnoreCase("ONLY_NEW")) {
			return ONLY_NEW;
		} 
		
		return null;
	}
	

}

