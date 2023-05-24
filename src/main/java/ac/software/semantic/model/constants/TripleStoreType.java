package ac.software.semantic.model.constants;

public enum TripleStoreType {
	
    BLAZEGRAPH,
	OPENLINK_VIRTUOSO,
    ;

    public static TripleStoreType get(String type) {
		if (type.equals("BLAZEGRAPH")) {
			return BLAZEGRAPH;
		} else if (type.equals("OPENLINK_VIRTUOSO")) {
			return OPENLINK_VIRTUOSO;
		}
		
		return null;
	}
}