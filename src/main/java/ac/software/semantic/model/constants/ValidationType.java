package ac.software.semantic.model.constants;

public enum ValidationType {
	
	APPROVE,
	DELETE;
	
	public static ValidationType get(String type) {
		if (type.equals("DELETE")) {
			return DELETE;
		} else if (type.equals("APPROVE")) {
			return APPROVE;
		} 
		
		return null;
	}
}
