package ac.software.semantic.model.constants.state;

public enum DatasetState {

	WAITING_TO_PUBLISH,
	UNPUBLISHED, // should be changed to NOT_PUBLISHED but Mongo currently has UNPUBLISHED
	PUBLISHED_PUBLIC,
	PUBLISHED_PRIVATE,
	UNPUBLISHING,
	UNPUBLISHING_FAILED,
	PUBLISHING,
	PUBLISHING_FAILED,
	PUBLISHING_CANCELED,
	PUBLISHED;
	
	public static boolean isPublishedState(DatasetState state) {
		return state == PUBLISHED || state == PUBLISHED_PUBLIC || state == PUBLISHED_PRIVATE;
	}
}
