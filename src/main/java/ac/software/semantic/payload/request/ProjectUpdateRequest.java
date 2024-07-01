package ac.software.semantic.payload.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProjectUpdateRequest implements UpdateRequest {

	private String name;
	
	private String identifier;

   	@JsonProperty("public")
    private boolean publik;
   	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public boolean isPublik() {
		return publik;
	}

	public void setPublik(boolean publik) {
		this.publik = publik;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	
}
