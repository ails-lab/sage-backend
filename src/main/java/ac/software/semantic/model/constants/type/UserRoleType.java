package ac.software.semantic.model.constants.type;

public enum UserRoleType {
	
	ADMINISTRATOR,
    EDITOR,
    VALIDATOR,
    ;

    public static UserRoleType get(String type) {
		if (type.equals("EDITOR")) {
			return EDITOR;
		} else if (type.equals("ADMINISTRATOR")) {
				return ADMINISTRATOR;
		} else if (type.equals("VALIDATOR")) {
			return VALIDATOR;
		}
		
		return null;
	}
}