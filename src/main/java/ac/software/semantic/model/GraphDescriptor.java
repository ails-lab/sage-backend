package ac.software.semantic.model;

public class GraphDescriptor {

	private String identifier;
	private String url;
	
	private boolean isSKOS;
	private boolean isInstance;
	
	public GraphDescriptor(String url, String identifier, boolean isSKOS, boolean isInstance) {
		this.url = url;
		this.identifier = identifier;
		
		this.isSKOS = isSKOS;
		this.isInstance = isInstance;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public boolean isSKOS() {
		return isSKOS;
	}

	public void setSKOS(boolean isSKOS) {
		this.isSKOS = isSKOS;
	}

	public boolean isInstance() {
		return isInstance;
	}

	public void setInstance(boolean isInstance) {
		this.isInstance = isInstance;
	}

	
}
