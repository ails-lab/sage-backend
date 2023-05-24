package ac.software.semantic.model.state;

public enum PagedAnnotationValidationState {
	
	STARTED,
	STOPPED, 
	RESUMING,
	RESUMING_FAILED;
	
	public static PagedAnnotationValidationState get(String type) {
		if (type.equals("STARTED")) {
			return STARTED;
		} else if (type.equals("STOPPED")) {
			return STOPPED;
		} else if (type.equals("RESUMING")) {
				return RESUMING;
		} else if (type.equals("RESUMING_FAILED")) {
			return RESUMING_FAILED;
		} 
		
		return null;
	}
}
