package ac.software.semantic.model.constants.type;

public enum TripleStoreType {
	
    BLAZEGRAPH,
	OPENLINK_VIRTUOSO,
	GRAPHDB,
    ;

    public static TripleStoreType get(String type) {
		if (type.equals("BLAZEGRAPH")) {
			return BLAZEGRAPH;
		} else if (type.equals("OPENLINK_VIRTUOSO")) {
			return OPENLINK_VIRTUOSO;
		} else if (type.equals("GRAPHDB")) {
			return GRAPHDB;
		}
		
		return null;
	}
}