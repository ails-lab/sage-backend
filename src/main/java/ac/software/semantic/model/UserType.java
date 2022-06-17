package ac.software.semantic.model;

public enum UserType {
	
	// ADMIN,
	// SUPER,
	// NORMAL;
	SUPER,
	EDITOR,
	VALIDATOR,
	PENDING;
	
	public static UserType get(String type) {
		if (type.equals("SUPER")) {
			return SUPER;
		} else if (type.equals("EDITOR")) {
				return EDITOR;
		} else if (type.equals("VALIDATOR")){
			return VALIDATOR;
		} else {
			return PENDING;
		}
	}
}
