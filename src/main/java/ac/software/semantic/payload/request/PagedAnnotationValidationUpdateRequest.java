package ac.software.semantic.payload.request;

import java.util.List;

public class PagedAnnotationValidationUpdateRequest implements UpdateRequest {

	private String name;
	
	private String mode;
	
	private List<String> systemVocabularies;
	private List<String> userVocabularies;

	public PagedAnnotationValidationUpdateRequest() { 
		
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public List<String> getSystemVocabularies() {
		return systemVocabularies;
	}

	public void setSystemVocabularies(List<String> systemVocabularies) {
		this.systemVocabularies = systemVocabularies;
	}

	public List<String> getUserVocabularies() {
		return userVocabularies;
	}

	public void setUserVocabularies(List<String> userVocabularies) {
		this.userVocabularies = userVocabularies;
	}
	
}
