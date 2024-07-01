package ac.software.semantic.model.constants.type;

public enum AnnotationEditType {

	ACCEPT,
	REJECT,
	CHANGE_TARGET,
//	DELETE,
	ADD;
	
	public static AnnotationEditType get(String type) {
//		if (type.equals("DELETE")) {
//			return DELETE;
//		} else 
		if (type.equals("ACCEPT")) {
			return ACCEPT;
		} else if (type.equals("REJECT")) {
			return REJECT;
		} else if (type.equals("ADD")) {
			return ADD;
		} else if (type.equals("CHANGE_TARGET")) {
			return CHANGE_TARGET;
		}
		
		return null;
	}
}
