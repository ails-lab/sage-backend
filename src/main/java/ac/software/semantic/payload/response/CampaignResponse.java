package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.state.CampaignState;
import ac.software.semantic.model.constants.type.CampaignType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CampaignResponse implements Response {
   
   private String id;
   private String uuid;
   
   private CampaignType type;
   
   private String name;
   
	private CampaignState state;
   
   private List<UserResponse> validators;
   private List<DatasetResponse> datasets;
   
   private Date createdAt;
   private Date updatedAt;
   
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


	public List<UserResponse> getValidators() {
		return validators;
	}


	public void setValidators(List<UserResponse> users) {
		this.validators = users != null && users.size() > 0 ? users : null;
	}


	public List<DatasetResponse> getDatasets() {
		return datasets;
	}


	public void setDatasets(List<DatasetResponse> datasets) {
		this.datasets = datasets;
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