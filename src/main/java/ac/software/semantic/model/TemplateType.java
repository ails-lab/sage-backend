package ac.software.semantic.model;

public enum TemplateType {
    ANNOTATOR,
    FILTER,
    PREDEFINED_IMPORT,
    API_KEY;

    public static TemplateType get(String type) {
        if (type.equals("ANNOTATOR")) {
            return ANNOTATOR;
        } else if (type.equals("FILTER")) {
            return FILTER;
        } else if (type.equals("PREDEFINED_IMPORT")) {
            return PREDEFINED_IMPORT;
        } else if (type.equals("API_KEY")) {
            return API_KEY;
        }
        return null;
    }

}
