package ac.software.semantic.model.constants;

public enum AnnotatorPrepareStatus {
    PREPARED,
    PREPARING,
    NOT_PREPARED,
    UNKNOWN,
    ERROR,
    ;

    public static AnnotatorPrepareStatus get(String type) {
		if (type.equals("PREPARED")) {
			return PREPARED;
		} else if (type.equals("PREPARING")) {
			return PREPARING;
		} else if (type.equals("NOT_PREPARED")) {
			return PREPARING;
		} else if (type.equals("UNKNOWN")) {
			return UNKNOWN;
		} else if (type.equals("ERROR")) {
			return ERROR;
		} 
			
		return null;
	}
}