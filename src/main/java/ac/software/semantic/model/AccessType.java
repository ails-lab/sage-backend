package ac.software.semantic.model;

public enum AccessType {
    SUPER,
    EDITOR,
    VALIDATOR;

    public static AccessType get(String type) {
		if (type.equals("SUPER")) {
			return SUPER;
		} else if (type.equals("EDITOR")) {
				return EDITOR;
		} else {
			return VALIDATOR;
		}
	}
}