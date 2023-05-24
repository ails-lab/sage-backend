package ac.software.semantic.model.constants;

public enum RDFTermType {
	
	LITERAL,
	IRI,
	;
	
	public static RDFTermType get(String type) {
		if (type.equals("LITERAL")) {
			return LITERAL;
		} else if (type.equals("IRI")) {
			return IRI;
		} 
		
		return null;
	}
	
}
