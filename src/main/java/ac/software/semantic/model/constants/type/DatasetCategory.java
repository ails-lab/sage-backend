package ac.software.semantic.model.constants.type;

public enum DatasetCategory {
	
	
	DATA, 
	PROTOTYPE,
	;
	
	public static DatasetCategory get(String type) {
		if (type.equals("DATA")) {
			return DATA;
		} else if (type.equals("PROTOTYPE")) {
			return PROTOTYPE;
		} 
		
		return null;
	}

}
