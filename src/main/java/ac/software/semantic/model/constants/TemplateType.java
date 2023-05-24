package ac.software.semantic.model.constants;

public enum TemplateType {
    ANNOTATOR,
    FILTER,
    DATASET_IMPORT, //PREDEFINED_IMPORT,
    DATASET_IMPORT_HEADER, //PREDEFINED_IMPORT_HEADER,
    DATASET_IMPORT_CONTENT, //PREDEFINED_IMPORT_CONTENT,
    CATALOG_IMPORT, //PREDEFINED_IMPORT,
    MAPPING_SAMPLE, //PREDEFINED_IMPORT,
    API_KEY;

    public static TemplateType get(String type) {
        if (type.equals("ANNOTATOR")) {
            return ANNOTATOR;
        } else if (type.equals("FILTER")) {
            return FILTER;
        } else if (type.equals("DATASET_IMPORT")) {
            return DATASET_IMPORT;
        } else if (type.equals("DATASET_IMPORT_HEADER")) {
            return DATASET_IMPORT_HEADER;
        } else if (type.equals("DATASET_IMPORT_CONTENT")) {
            return DATASET_IMPORT_CONTENT;
        } else if (type.equals("CATALOG_IMPORT")) {
            return CATALOG_IMPORT;
        } else if (type.equals("MAPPING_SAMPLE")) {
            return MAPPING_SAMPLE;
        } else if (type.equals("API_KEY")) {
            return API_KEY;
        }
        return null;
    }

}
