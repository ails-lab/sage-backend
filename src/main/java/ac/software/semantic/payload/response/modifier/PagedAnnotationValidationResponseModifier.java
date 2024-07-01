package ac.software.semantic.payload.response.modifier;

import java.util.List;

import ac.software.semantic.payload.response.ResponseFieldType;

public class PagedAnnotationValidationResponseModifier implements ResponseModifier {

	private ResponseFieldType states = ResponseFieldType.IGNORE;
	private ResponseFieldType dates = ResponseFieldType.IGNORE;
	private ResponseFieldType vocabularies = ResponseFieldType.EXPAND;
	private ResponseFieldType progress = ResponseFieldType.IGNORE;
	
	public PagedAnnotationValidationResponseModifier() {
	}
	
	public static PagedAnnotationValidationResponseModifier baseModifier() {
		PagedAnnotationValidationResponseModifier modifier = new PagedAnnotationValidationResponseModifier(); 
		modifier.setStates(ResponseFieldType.IGNORE);
		modifier.setDates(ResponseFieldType.IGNORE);
		modifier.setVocabularies(ResponseFieldType.IGNORE);
		modifier.setProgress(ResponseFieldType.IGNORE);
		
		return modifier;
	}
	
	public static PagedAnnotationValidationResponseModifier fullModifier() {
		PagedAnnotationValidationResponseModifier modifier = new PagedAnnotationValidationResponseModifier(); 
		modifier.setStates(ResponseFieldType.EXPAND);
		modifier.setDates(ResponseFieldType.EXPAND);
		modifier.setVocabularies(ResponseFieldType.EXPAND);
		modifier.setProgress(ResponseFieldType.EXPAND);
		
		return modifier;
	}
	
	public void customizeModifier(List<String> options) {
		if (options.contains("progress")) {
			setProgress(ResponseFieldType.EXPAND);
		}
		
		if (options.contains("vocabularies")) {
			setVocabularies(ResponseFieldType.EXPAND);
		} 
		
		if (options.contains("states")) {
			setStates(ResponseFieldType.EXPAND);
		} 

		if (options.contains("dates")) {
			setDates(ResponseFieldType.EXPAND);
		}
	}

	public ResponseFieldType getStates() {
		return states;
	}

	public void setStates(ResponseFieldType states) {
		this.states = states;
	}

	public ResponseFieldType getDates() {
		return dates;
	}

	public void setDates(ResponseFieldType dates) {
		this.dates = dates;
	}

	public ResponseFieldType getVocabularies() {
		return vocabularies;
	}

	public void setVocabularies(ResponseFieldType vocabularies) {
		this.vocabularies = vocabularies;
	}

	public ResponseFieldType getProgress() {
		return progress;
	}

	public void setProgress(ResponseFieldType progress) {
		this.progress = progress;
	}
	
}
