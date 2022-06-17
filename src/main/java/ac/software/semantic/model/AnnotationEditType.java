package ac.software.semantic.model;

public enum AnnotationEditType {

	ACCEPT,
	REJECT,
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
		}
		
		return null;
	}
}
