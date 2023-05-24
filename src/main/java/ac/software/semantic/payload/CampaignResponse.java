package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.CampaignState;
import ac.software.semantic.model.constants.CampaignType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CampaignResponse {
   
   private String id;
   
   private CampaignType type;
   
   private String name;
   
	private CampaignState state;
   
   private String uuid;
   
   private List<NewUserSummary> users;
   
   
   public CampaignResponse() {  }


	public CampaignType getType() {
		return type;
	}
	
	
	public void setType(CampaignType type) {
		this.type = type;
	}
	
	
	public String getName() {
		return name;
	}
	
	
	public void setName(String name) {
		this.name = name;
	}


	public String getUuid() {
		return uuid;
	}


	public void setUuid(String uuid) {
		this.uuid = uuid;
	}


	public String getId() {
		return id;
	}


	public void setId(String id) {
		this.id = id;
	}


	public CampaignState getState() {
		return state;
	}


	public void setState(CampaignState state) {
		this.state = state;
	}


	public List<NewUserSummary> getUsers() {
		return users;
	}


	public void setUsers(List<NewUserSummary> users) {
		this.users = users;
	}
   

}