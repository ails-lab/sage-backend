package ac.software.semantic.model;

public enum DatasetState {
	
	UNPUBLISHED,
	PUBLISHED_PUBLIC,
	PUBLISHED_PRIVATE,
	UNPUBLISHING,
	PUBLISHING,
	PUBLISHING_FAILED,
	PUBLISHED;
	
	public static DatasetState get(String type) {
		if (type.equals("UNPUBLISHED")) {
			return UNPUBLISHED;
		} else if (type.equals("PUBLISHED_PUBLIC")) {
			return PUBLISHED_PUBLIC;
		} else if (type.equals("PUBLISHED_PRIVATE")) {
			return PUBLISHED_PRIVATE;
		} else if (type.equals("UNPUBLISHING")) {
			return UNPUBLISHING;
		} else if (type.equals("PUBLISHING")) {
			return PUBLISHING;
		} else if (type.equals("PUBLISHING_FAILED")) {
			return PUBLISHING_FAILED;
		} else if (type.equals("PUBLISHED")) {
			return PUBLISHED;
		}
		
		return null;
	}
}
