package ac.software.semantic.payload.response;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.TripleStoreType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ElasticResponse {
	
	private String id;
   
	private String name;
	private String location;
	
	private String version; 
	
	public ElasticResponse() { 
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

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}


}
