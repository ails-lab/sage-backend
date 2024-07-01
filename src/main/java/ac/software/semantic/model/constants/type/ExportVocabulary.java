package ac.software.semantic.model.constants.type;

public enum ExportVocabulary {

	eu_contolled_vocabularies,
	w3c;

	public static ExportVocabulary get(String type) {
		if (type.equalsIgnoreCase("EU Controlled Vocabularies")) {
			return eu_contolled_vocabularies;
		} else if (type.equalsIgnoreCase("W3C")) {
			return w3c;
		} 
		
		return null;
	}
	
	public static String get(ExportVocabulary ev) {
		if (ev == eu_contolled_vocabularies) {
			return "EU Controlled Vocabularies";
		} else if (ev == w3c) {
			return "W3C";
		} 
		
		return null;
	}
}
