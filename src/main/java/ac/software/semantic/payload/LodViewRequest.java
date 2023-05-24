package ac.software.semantic.payload;

import java.util.Collection;
import java.util.List;

public class LodViewRequest {

	private String database;
	private Collection<PrefixEndpoint> prefixes;
	private List<String> hosts;
	
	public LodViewRequest(String database, Collection<PrefixEndpoint> prefixes, List<String> hosts) {
		this.database = database;
		this.prefixes = prefixes;
		this.hosts = hosts;
	}

	public String getDatabase() {
		return database;
	}
	
	public void setDatabase(String database) {
		this.database = database;
	}
	
	public Collection<PrefixEndpoint> getPrefixes() {
		return prefixes;
	}
	
	public void setPrefixes(Collection<PrefixEndpoint> prefixes) {
		this.prefixes = prefixes;
	}

	public List<String> getHosts() {
		return hosts;
	}

	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}
}
