package ac.software.semantic.model.constants.state;

public enum ThesaurusLoadState {
    LOADED,
    LOADING,
    NOT_LOADED,
    UNKNOWN,
    ERROR,
    ;

    public static ThesaurusLoadState get(String type) {
		if (type.equals("LOADED")) {
			return LOADED;
		} else if (type.equals("LOADING")) {
			return LOADING;
		} else if (type.equals("NOT_LOADED")) {
			return NOT_LOADED;
		} else if (type.equals("UNKNOWN")) {
			return UNKNOWN;
		} else if (type.equals("ERROR")) {
			return ERROR;
		} 
			
		return null;
	}
    
    
}