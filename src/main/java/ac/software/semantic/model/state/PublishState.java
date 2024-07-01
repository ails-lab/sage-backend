package ac.software.semantic.model.state;

import java.util.Date;
import java.util.Properties;

import org.bson.types.ObjectId;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.response.ResponseTaskObject;

public class PublishState<E extends ExecuteState> extends ProcessState implements UndoableState { // E is e.g. FileExecuteState, MappingExecuteState

	protected ObjectId databaseConfigurationId;
	   
	protected DatasetState publishState;
   
	protected Date publishStartedAt;
	protected Date publishCompletedAt;
	
	private E execute;

	public PublishState() { }
	
	public String getPublishStartStamp() {
		return CurrentState.stampFormat.format(publishStartedAt);
	}

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

	@Override
	public void startDo(TaskMonitor tm) {
		setPublishState(DatasetState.PUBLISHING);
		setPublishStartedAt(new Date());
		setPublishCompletedAt(null);
		clearMessages();	
	}
	
	@Override
	public void completeDo(TaskMonitor tm) {
		setPublishState(DatasetState.PUBLISHED);
		setPublishCompletedAt(tm.getCompletedAt());
	}

	@Override
	public void failDo(TaskMonitor tm) {
		setPublishState(DatasetState.PUBLISHING_FAILED);
		setPublishCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}

	
	@Override
	public void startUndo(TaskMonitor tm) {
		setPublishState(DatasetState.UNPUBLISHING);
		setPublishStartedAt(new Date());
		setPublishCompletedAt(null);
		clearMessages();
	}

	
	@Override
	public void failUndo(TaskMonitor tm) {
		setPublishState(DatasetState.UNPUBLISHING_FAILED);
		setPublishCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}


	@Override
	public void fail(Date completedAt, String error) {
		if (publishState == DatasetState.PUBLISHING) {
			setPublishState(DatasetState.PUBLISHING_FAILED);
		} else if (publishState == DatasetState.UNPUBLISHING) {
			setPublishState(DatasetState.UNPUBLISHING_FAILED);
		}
        setPublishCompletedAt(completedAt);
        setMessage(new NotificationMessage(MessageType.ERROR, error));
	}
	
	public E getExecute() {
		return execute;
	}

	public void setExecute(E execute) {
		this.execute = execute;
	}

	@Override
	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = new ResponseTaskObject();
		
		if (getPublishState() == DatasetState.PUBLISHED_PRIVATE || getPublishState() == DatasetState.PUBLISHED_PUBLIC) {
			res.setState(DatasetState.PUBLISHED.toString());
		} else {
			res.setState(getPublishState().toString());
		}
    	
   		res.setStartedAt(getPublishStartedAt());
   		res.setCompletedAt(getPublishCompletedAt());
   		res.setMessages(getMessages());
    	
    	return res;
	}
	
}	   
