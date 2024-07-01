package ac.software.semantic.model.state;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.state.RunningState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.response.ResponseTaskObject;

public class RunState extends ProcessState { // should be abstract but spring instantiation fails

	protected ObjectId fileSystemConfigurationId;
	   
	protected RunningState runState;

	protected Date runStartedAt;
	protected Date runCompletedAt;
	
	public RunState() { 
	}
	
	public RunningState getRunState() {
		return runState;
	}

	public void setRunState(RunningState runState) {
		this.runState = runState;
	}

	public String getRunStartStamp() {
		return CurrentState.stampFormat.format(runStartedAt);
	}

	public Date getRunStartedAt() {
		return runStartedAt;
	}

	public void setRunStartedAt(Date runStartedAt) {
		this.runStartedAt = runStartedAt;
	}

	public Date getRunCompletedAt() {
		return runCompletedAt;
	}

	public void setRunCompletedAt(Date runCompletedAt) {
		this.runCompletedAt = runCompletedAt;
	}

	@Override
	public void startDo(TaskMonitor tm) {
		setRunState(RunningState.RUNNING);
		setRunStartedAt(new Date());
		clearMessages();
	}

	@Override
	public void completeDo(TaskMonitor tm) {
		setRunState(RunningState.RUN);
		setRunCompletedAt(tm.getCompletedAt());
	}

	@Override
	public void failDo(TaskMonitor tm) {
		setRunState(RunningState.RUNNING_FAILED);
		setRunStartedAt(tm.getCompletedAt());
		setMessage(tm.getFailureMessage());
	}

	@Override
	public void fail(Date completedAt, String error) {
		setRunState(RunningState.RUNNING_FAILED);
        setRunCompletedAt(completedAt);
        setMessage(new NotificationMessage(MessageType.ERROR, error));
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}
	
	@Override
	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = new ResponseTaskObject();
		
    	res.setState(getRunState().toString());
    	res.setStartedAt(getRunStartedAt());
    	res.setCompletedAt(getRunCompletedAt());
    	res.setMessages(getMessages());
		
		return res;
	}
   
}	   
