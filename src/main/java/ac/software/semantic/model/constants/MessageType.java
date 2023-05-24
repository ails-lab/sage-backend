package ac.software.semantic.model.constants;

public enum MessageType {
    ERROR,
    INFO;

    public static MessageType get(String type) {
		if (type.equals("ERROR")) {
			return ERROR;
		} else if (type.equals("INFO")) {
			return INFO;
		} 

		return null;
	}
}