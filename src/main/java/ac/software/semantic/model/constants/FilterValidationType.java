package ac.software.semantic.model.constants;

public enum FilterValidationType {
	
	DELETE,
	REPLACE;
	
	public static FilterValidationType get(String type) {
		if (type.equals("DELETE")) {
			return DELETE;
		} else if (type.equals("REPLACE")) {
			return REPLACE;
		} 
		
		return null;
	}
}
