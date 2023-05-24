package ac.software.semantic.model.constants;

public enum VocabularyType {
	
	RDFS,
	OWL2,
	KNOWLEDGE_BASE;
	
	public static VocabularyType get(String type) {
		if (type.equals("RDFS")) {
			return RDFS;
		} else if (type.equals("OWL2")) {
			return OWL2;
		} else if (type.equals("KNOWLEDGE_BASE")) {
			return KNOWLEDGE_BASE;
		} 
		
		return null;
	}
}
