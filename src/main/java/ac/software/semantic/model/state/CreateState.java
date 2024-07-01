package ac.software.semantic.model.state;

import java.util.Date;
import java.util.Properties;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.response.ResponseTaskObject;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateState extends ProcessState implements UndoableState { // should be abstract but spring instantiation fails

	protected ObjectId elasticConfigurationId;
	private ObjectId fileSystemConfigurationId;
	   
	protected CreatingState createState;

	protected Date createStartedAt;
	protected Date createCompletedAt;
	
	public CreateState() { 
	}
	
	public CreatingState getCreateState() {
		return createState;
	}

	public void setCreateState(CreatingState createState) {
		this.createState = createState;
	}

	public String getCreateStartStamp() {
		return CurrentState.stampFormat.format(createStartedAt);
	}

	public Date getCreateStartedAt() {
		return createStartedAt;
	}

	public void setCreateStartedAt(Date createStartedAt) {
		this.createStartedAt = createStartedAt;
	}

	public Date getCreateCompletedAt() {
		return createCompletedAt;
	}

	public void setCreateCompletedAt(Date createCompletedAt) {
		this.createCompletedAt = createCompletedAt;
	}

	public ObjectId getElasticConfigurationId() {
		return elasticConfigurationId;
	}

	public void setElasticConfigurationId(ObjectId elasticConfigurationId) {
		this.elasticConfigurationId = elasticConfigurationId;
	}

	@Override
	public void startDo(TaskMonitor tm) {
        setCreateState(CreatingState.CREATING);
        setCreateStartedAt(new Date());
        setCreateCompletedAt(null);
        clearMessages();
	}
	
	@Override
	public void startUndo(TaskMonitor tm) {
        setCreateState(CreatingState.DESTROYING);
        setCreateStartedAt(new Date());
        setCreateCompletedAt(null);
        clearMessages();
	}

	@Override
	public void completeDo(TaskMonitor tm) {
	    setCreateState(CreatingState.CREATED);
	    setCreateCompletedAt(tm.getCompletedAt());
	}	

	@Override
	public void failDo(TaskMonitor tm) {
        setCreateState(CreatingState.CREATING_FAILED);
        setCreateCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}
	
	@Override
	public void failUndo(TaskMonitor tm) {
        setCreateState(CreatingState.DESTROYING_FAILED);
        setCreateCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}


	@Override
	public void fail(Date completedAt, String error) {
		if (createState == CreatingState.CREATING) {
			setCreateState(CreatingState.CREATING_FAILED);
		} else if (createState == CreatingState.DESTROYING) {
			setCreateState(CreatingState.DESTROYING_FAILED);
		}
        setCreateCompletedAt(completedAt);
        setMessage(new NotificationMessage(MessageType.ERROR, error));
	}
	
	@Override
	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = new ResponseTaskObject();
		
    	res.setState(getCreateState().toString());
    	res.setStartedAt(getCreateStartedAt());
    	res.setCompletedAt(getCreateCompletedAt());
    	res.setMessages(getMessages());
		
		return res;
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}

	
}	   
