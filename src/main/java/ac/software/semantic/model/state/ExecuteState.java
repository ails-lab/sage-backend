package ac.software.semantic.model.state;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ExecutionInfo;

public abstract class ExecuteState extends ProcessState {

	protected ObjectId databaseConfigurationId;
	   
	protected MappingState executeState;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private List<ExecutionInfo> d2rmlExecution;
	
	protected Date executeStartedAt;
	protected Date executeCompletedAt;
	
	protected ExecuteState() { 
	}
	
	public MappingState getExecuteState() {
		return executeState;
	}

	public void setExecuteState(MappingState executeState) {
		this.executeState = executeState;
	}

	public String getExecuteStartStamp() {
		return CurrentState.stampFormat.format(executeStartedAt);
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

	public List<ExecutionInfo> getD2rmlExecution() {
		return d2rmlExecution;
	}

	public void setD2rmlExecution(List<ExecutionInfo> d2rmlExecution) {
		this.d2rmlExecution = d2rmlExecution;
	}
   
}	   
