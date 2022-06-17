package ac.software.semantic.model;

public enum IndexingState {
	
	NOT_INDEXED,
	INDEXED,
	INDEXING,
	INDEXING_FAILED,
	UNINDEXING;
	
	public static IndexingState get(String type) {
		if (type.equals("NOT_INDEXED")) {
			return NOT_INDEXED;
		} else if (type.equals("INDEXED")) {
			return INDEXED;
		} else if (type.equals("INDEXING")) {
			return INDEXING;
		} else if (type.equals("INDEXING_FAILED")) {
			return INDEXING_FAILED;
		} else if (type.equals("UNINDEXING")) {
			return UNINDEXING;
		} 
		
		return null;
	}
}
