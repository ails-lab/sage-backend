package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.UserRoleType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Access")
public class Access {
    //should we put creatorId;
    
	@Id
    private ObjectId id;
    
	private ObjectId creatorId;
    
	private ObjectId userId;
    
	// this is datasetId
	private ObjectId collectionId;
    
	//this is datasetUuid
	private String collectionUuid;
    
    private ObjectId campaignId;
    
    private UserRoleType accessType;

    private ObjectId databaseId;

    public Access() {}

    public ObjectId getId() {
        return id;
    }

    public ObjectId getUserId() {
        return userId;
    }

    public ObjectId getCollectionId() {
        return collectionId;
    }

    public ObjectId getCreatorId() {
        return creatorId;
    }

    public UserRoleType getAccessType() {
        return accessType;
    }

    public void setUserId(ObjectId userId) {
        this.userId =  userId;
    }

    public void setCollectionId(ObjectId collectionId) {
        this.collectionId = collectionId;
    }

    public void setCreatorId(ObjectId creatorId) {
        this.creatorId = creatorId;
    }

    public String getCollectionUuid() {
        return collectionUuid;
    }

    public void setCollectionUuid(String collectionUuid) {
        this.collectionUuid = collectionUuid;
    }

    public void setAccessType(UserRoleType accessType) {
        this.accessType = accessType;
    }

	public ObjectId getCampaignId() {
		return campaignId;
	}

	public void setCampaignId(ObjectId campaignId) {
		this.campaignId = campaignId;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}
}