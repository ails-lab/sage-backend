package ac.software.semantic.model;

import java.util.Date;

import org.bson.types.ObjectId;

public class IndexState {

	private ObjectId databaseConfigurationId;
	   
	private IndexingState indexState;
   
	private Date indexStartedAt;
	private Date indexCompletedAt;

	public IndexState() { }
	

	public ObjectId getDatabaseConfigurationId() {
		return databaseConfigurationId;
	}

	public void setDatabaseConfigurationId(ObjectId databaseConfigurationId) {
		this.databaseConfigurationId = databaseConfigurationId;
	}


	public IndexingState getIndexState() {
		return indexState;
	}


	public void setIndexState(IndexingState indexState) {
		this.indexState = indexState;
	}


	public Date getIndexStartedAt() {
		return indexStartedAt;
	}


	public void setIndexStartedAt(Date indexStartedAt) {
		this.indexStartedAt = indexStartedAt;
	}


	public Date getIndexCompletedAt() {
		return indexCompletedAt;
	}


	public void setIndexCompletedAt(Date indexCompletedAt) {
		this.indexCompletedAt = indexCompletedAt;
	}
	   
}	   
