package ac.software.semantic.model;

import java.util.Date;

import org.bson.types.ObjectId;

public class ExecuteState {

	private ObjectId databaseConfigurationId;
	   
	private MappingState executeState;
   
	private Date executeStartedAt;
	private Date executeCompletedAt;
	
	private int count;
	private int executeShards;

	public ExecuteState() { 
		this.count = -1;
	}
	
	public MappingState getExecuteState() {
		return executeState;
	}

	public void setExecuteState(MappingState executeState) {
		this.executeState = executeState;
	}

	public Date getExecuteStartedAt() {
		return executeStartedAt;
	}

	public void setExecuteStartedAt(Date executeStartedAt) {
		this.executeStartedAt = executeStartedAt;
	}

	public Date getExecuteCompletedAt() {
		return executeCompletedAt;
	}

	public void setExecuteCompletedAt(Date executeCompletedAt) {
		this.executeCompletedAt = executeCompletedAt;
	}

	public ObjectId getDatabaseConfigurationId() {
		return databaseConfigurationId;
	}

	public void setDatabaseConfigurationId(ObjectId databaseConfigurationId) {
		this.databaseConfigurationId = databaseConfigurationId;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getExecuteShards() {
		return executeShards;
	}

	public void setExecuteShards(int executeShards) {
		this.executeShards = executeShards;
	}
	   
}	   
