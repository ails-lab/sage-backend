package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProjectDocumentResponse implements Response {
   
   private String id;
   private String uuid;

   private String name;
   
   private String identifier;
   
  	@JsonProperty("public")
   private boolean publik;
  	
   private List<UserResponse> joinedUsers; 

   private Date createdAt;
   private Date updatedAt;

   
   public ProjectDocumentResponse() {
   }
   
   public String getId() {
       return id;
   }
   
	public void setId(String id) {
		this.id = id;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

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

	public List<UserResponse> getJoinedUsers() {
		return joinedUsers;
	}

	public void setJoinedUsers(List<UserResponse> joinedUsers) {
		this.joinedUsers =  joinedUsers != null && joinedUsers.size() > 0 ? joinedUsers : null;;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

}