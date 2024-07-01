package ac.software.semantic.model.constants.type;

public enum OperationType {
	
	EXECUTE, 
	CREATE,
	DESTROY,
	RECREATE,
	PUBLISH,
	UNPUBLISH,
	REPUBLISH,
	SHACL_VALIDATE,
	CLEAR,
	RESUME,
	RUN,
	;

	public static String toPrettyString(OperationType type) {
		if (type == EXECUTE) {
			return "execute";
		} else if (type == CREATE) {
			return "create";
		} else if (type == DESTROY) {
			return "destroy";
		} else if (type == RECREATE) {
			return "recreate";
		} else if (type == PUBLISH) {
			return "publish";
		} else if (type == UNPUBLISH) {
			return "unpublish";
		} else if (type == REPUBLISH) {
			return "republish";
		} else if (type == SHACL_VALIDATE) {
			return "SHACL validate";
		} else if (type == CLEAR) {
			return "clear";
		} else if (type == RESUME) {
			return "resume";
		} else if (type == RUN) {
			return "run";
		}
		
		return null;
	}
}
