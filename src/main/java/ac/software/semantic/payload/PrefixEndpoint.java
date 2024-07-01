package ac.software.semantic.payload;

public class PrefixEndpoint {

	private String prefix;
	private String endpoint;
	
	public PrefixEndpoint(String prefix, String endpoint) {
		this.prefix = prefix;
		this.endpoint = endpoint;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	
	
	public int hasCode() {
		return prefix.hashCode();
	}
	
	public boolean equals(Object obj) {
		if (!(obj instanceof PrefixEndpoint)) {
			return false;
		} else {
			return prefix.equals(((PrefixEndpoint)obj).prefix);
		}
	}
}
