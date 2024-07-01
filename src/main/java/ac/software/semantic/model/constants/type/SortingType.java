package ac.software.semantic.model.constants.type;

public enum SortingType {
    ASC,
    DESC;

    public static SortingType get(String type) {
		if (type.equals("ASC")) {
			return ASC;
		} else if (type.equals("DESC")) {
			return DESC;
		} 

		return null;
	}
}