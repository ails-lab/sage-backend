package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.AccessType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Access")
public class Access {
    //should we put creatorId;
    @Id
    private ObjectId id;
    private ObjectId creatorId;
    private ObjectId userId;
    private ObjectId collectionId;
    private String collectionUuid;
    
    private ObjectId campaignId;
    
    private AccessType accessType;

    public Access() {}

    public Access(String creatorId, String userId, String collectionId, String collectionUuid, AccessType accessType) {
        this.userId = new ObjectId(userId);
        this.creatorId = new ObjectId(creatorId);
        this.collectionId = new ObjectId(collectionId);
        this.collectionUuid = collectionUuid;
        this.accessType = accessType;
    }

    public Access(ObjectId userId,  ObjectId creatorId, ObjectId collectionId, String collectionUuid, AccessType accessType) {
        this.userId = userId;
        this.creatorId = creatorId;
        this.collectionId = collectionId;
        this.collectionUuid = collectionUuid;
        this.accessType = accessType;
    }

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

    public AccessType getAccessType() {
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

    public void setAccessType(AccessType accessType) {
        this.accessType = accessType;
    }

	public ObjectId getCampaignId() {
		return campaignId;
	}

	public void setCampaignId(ObjectId campaignId) {
		this.campaignId = campaignId;
	}
}