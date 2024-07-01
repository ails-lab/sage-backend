package ac.software.semantic.model.constants.type;

public enum PathElementType {
	
	CLASS,
	PROPERTY,
	;
	
	public static PathElementType get(String type) {
		if (type.equals("CLASS")) {
			return CLASS;
		} else if (type.equals("PROPERTY")) {
			return PROPERTY;
		} 
		
		return null;
	}
}
