package ac.software.semantic.model.constants.type;

public enum MappingType {
	
	PREFIX,
	HEADER,
	CONTENT,
	CATALOG,
	;
	
	public static MappingType get(String type) {
		if (type.equals("HEADER")) {
			return HEADER;
		} else if (type.equals("PREFIX")) {
			return PREFIX;
		} else if (type.equals("CONTENT")) {
			return CONTENT;
		} else if (type.equals("CATALOG")) {
			return CATALOG;
		}
		
		return null;
	}
}
