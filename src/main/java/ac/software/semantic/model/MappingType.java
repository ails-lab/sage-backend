package ac.software.semantic.model;

public enum MappingType {
	
	PREFIX,
	HEADER,
	CONTENT;
	
	public static MappingType get(String type) {
		if (type.equals("HEADER")) {
			return HEADER;
		} else if (type.equals("PREFIX")) {
			return PREFIX;
		} else {
			return CONTENT;
		}
	}
}
