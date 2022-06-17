package ac.software.semantic.model;

public enum ImportType {
    CUSTOM,
    EUROPEANA;

    public static ImportType get(String type) {
        if (type.equals("CUSTOM")) {
            return CUSTOM;
        } else if (type.equalsIgnoreCase("EUROPEANA")) {
            return EUROPEANA;
        }
        return null;
    }
}
