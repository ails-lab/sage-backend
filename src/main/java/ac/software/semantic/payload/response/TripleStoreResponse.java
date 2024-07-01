package ac.software.semantic.payload.response;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.TripleStoreType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TripleStoreResponse {
	
	private String id;
   
	private String name;
	private String sparqlEndpoint;
	private String fileServer;
	
	private String uploadFolder;
	
	private TripleStoreType type; 
	private int triplesCount;
	private int graphCount;
	
	public TripleStoreResponse() { 
	}

   	public String getId() {
   		return id;
   	}
   	
	public void setId(String id) {
		this.id = id;
	}
	   
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSparqlEndpoint() {
		return sparqlEndpoint;
	}

	public void setSparqlEndpoint(String sparqlEndpoint) {
		this.sparqlEndpoint = sparqlEndpoint;
	}

	public String getFileServer() {
		return fileServer;
	}

	public void setFileServer(String fileServer) {
		this.fileServer = fileServer;
	}

	public String getUploadFolder() {
		return uploadFolder;
	}

	public void setUploadFolder(String uploadFolder) {
		this.uploadFolder = uploadFolder;
	}

	public TripleStoreType getType() {
		return type;
	}

	public void setType(TripleStoreType type) {
		this.type = type;
	}

	public int getTriplesCount() {
		return triplesCount;
	}

	public void setTriplesCount(int triplesCount) {
		this.triplesCount = triplesCount;
	}

	public int getGraphCount() {
		return graphCount;
	}

	public void setGraphCount(int graphCount) {
		this.graphCount = graphCount;
	}

	

}
