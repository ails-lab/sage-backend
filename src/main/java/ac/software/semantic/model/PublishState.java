package ac.software.semantic.model;

import java.util.Date;

import org.bson.types.ObjectId;

public class PublishState {

	private ObjectId databaseConfigurationId;
	   
	private DatasetState publishState;
   
	private Date publishStartedAt;
	private Date publishCompletedAt;

	public PublishState() { }
	
	public DatasetState getPublishState() {
		return publishState;
	}

	public void setPublishState(DatasetState publishState) {
		this.publishState = publishState;
	}

	public Date getPublishStartedAt() {
		return publishStartedAt;
	}

	public void setPublishStartedAt(Date publishStartedAt) {
		this.publishStartedAt = publishStartedAt;
	}

	public Date getPublishCompletedAt() {
		return publishCompletedAt;
	}

	public void setPublishCompletedAt(Date publishCompletedAt) {
		this.publishCompletedAt = publishCompletedAt;
	}

	public ObjectId getDatabaseConfigurationId() {
		return databaseConfigurationId;
	}

	public void setDatabaseConfigurationId(ObjectId databaseConfigurationId) {
		this.databaseConfigurationId = databaseConfigurationId;
	}
	   
}	   
