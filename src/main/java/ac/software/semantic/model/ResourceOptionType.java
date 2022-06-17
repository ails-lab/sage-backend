package ac.software.semantic.model;

import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;

public enum ResourceOptionType {
	
	BIDIRECTIONAL,
	ABOX_FOR,
	SOURCE,
	TARGET;
	
	public static ResourceOptionType get(String type) {
		if (type.equals("ABOX_FOR")) {
			return ABOX_FOR;
		} else if (type.equals("SOURCE")) {
			return SOURCE;
		} else if (type.equals("TARGET")) {
			return TARGET;
		} else if (type.equals("BIDIRECTIONAL")) {
			return BIDIRECTIONAL;
		} else {
			return null;
		}
	}
	
	public static String getProperty(ResourceOptionType type) {
		if (type == ABOX_FOR) {
			return SEMAVocabulary.aboxOf.toString();
		} else if (type == SOURCE) {
			return SEMAVocabulary.source.toString();
		} else if (type == TARGET) {
			return SEMAVocabulary.target.toString();
		} else {
			return null;
		}
	}
}
