package ac.software.semantic.config;

import ac.software.semantic.model.VirtuosoConfiguration;

public class VocabularyInfo {

	private String graph;
	private String endpoint;
	
	private VirtuosoConfiguration virtuoso;
	
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
	
	public VirtuosoConfiguration getVirtuoso() {
		return virtuoso;
	}
	
	public void setVirtuoso(VirtuosoConfiguration virtuoso) {
		this.virtuoso = virtuoso;
	}
	
}
