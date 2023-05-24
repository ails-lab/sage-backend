package ac.software.semantic.config;

import ac.software.semantic.model.TripleStoreConfiguration;

public class VocabularyInfo {

	private String graph;
	private String endpoint;
	
	private TripleStoreConfiguration virtuoso;
	
	public VocabularyInfo(String graph) {
		this(graph, null);
	}
	
	public VocabularyInfo(String graph, String endpoint) {
		this.graph = graph;
		this.endpoint = endpoint;
	}
	
	public boolean isRemote() {
		return endpoint != null;
	}
	
	public String getGraph() {
		return graph;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
	
	public TripleStoreConfiguration getVirtuoso() {
		return virtuoso;
	}
	
	public void setVirtuoso(TripleStoreConfiguration virtuoso) {
		this.virtuoso = virtuoso;
	}
	
}
