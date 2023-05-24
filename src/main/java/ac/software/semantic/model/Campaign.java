package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.CampaignState;
import ac.software.semantic.model.constants.CampaignType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Campaigns")
public class Campaign {
   
   @Id
   private ObjectId id;
   
   private ObjectId userId;
   
   private ObjectId databaseId;

   private CampaignType type;
   
   private String uuid;
   
   private String name;
   
   private List<ObjectId> validatorId;
   
   private CampaignState state;
   
   private Date createdAt;
   
   private List<ObjectId> datasetId;
   
   public Campaign() {  }
   
//   public Campaign(ObjectId userId, ObjectId databaseId, CampaignType type) { 
//	   this.userId = userId;
//	   this.databaseId = databaseId;
//	   this.type = type;
//   }

	public ObjectId getId() {
		return id;
	}
	
	public ObjectId getUserId() {
		return userId;
	}
	
	
	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}
	
	
	public ObjectId getDatabaseId() {
		return databaseId;
	}
	
	
	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}
	
	
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


	public List<ObjectId> getValidatorId() {
		return validatorId;
	}


	public void setValidatorId(List<ObjectId> validatorId) {
		this.validatorId = validatorId;
	}
	
	public boolean addValidatorId(ObjectId id) {
		if (validatorId == null) {
			validatorId = new ArrayList<>();
		}
		
		if (validatorId.contains(id)) {
			return false;
		} else {
			validatorId.add(id);
			return true;
		}
		
	}
	
	public boolean removeValidatorId(ObjectId id) {
		boolean removed = false;
		if (validatorId != null) {
			removed = validatorId.remove(id);
		} 

		if (validatorId.size() == 0) {
			validatorId = null;
		}
		
		return removed;
		
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public CampaignState getState() {
		return state;
	}

	public void setState(CampaignState state) {
		this.state = state;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public List<ObjectId> getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(List<ObjectId> datasetId) {
		this.datasetId = datasetId;
	}
   

}