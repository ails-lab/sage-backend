package ac.software.semantic.model.constants;

public enum DatasetType {
	
	
	DATASET, // should be removed
	CATALOG,
	;
	
	public static DatasetType get(String type) {
		if (type.equals("DATASET")) {
			return DATASET;
		} else if (type.equals("CATALOG")) {
			return CATALOG;
		} 
		
		return null;
	}

}
