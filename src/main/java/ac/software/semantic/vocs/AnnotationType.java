package ac.software.semantic.vocs;

public enum AnnotationType {
	
	GENERIC,
	SPATIAL,
	TEMPORAL;
	
	public static AnnotationType get(String type) {
		if (type.equals("SPATIAL")) {
			return SPATIAL;
		} else if (type.equals("GENERIC")) {
			return GENERIC;
		} else if (type.equals("TEMPORAL")) {
			return TEMPORAL;
		}
		
		return null;
	}
}
