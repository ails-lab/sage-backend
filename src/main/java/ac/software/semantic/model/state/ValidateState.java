package ac.software.semantic.model.state;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.bson.types.ObjectId;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.ValidationResult;
import ac.software.semantic.model.constants.state.ValidatingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.response.ResponseTaskObject;

public class ValidateState extends ProcessState { 

	protected ObjectId fileSystemConfigurationId;
	   
	protected ValidatingState validateState;

	protected Date validateStartedAt;
	protected Date validateCompletedAt;
	
	protected ValidationResult result;
	
	private List<ObjectId> validatorDocumentId;
	
	public ValidateState() { 
	}
	
	public ValidatingState getValidateState() {
		return validateState;
	}

	public void setValidateState(ValidatingState validateState) {
		this.validateState = validateState;
	}

	public String getValidateStartStamp() {
		return CurrentState.stampFormat.format(validateStartedAt);
	}

	public Date getValidateStartedAt() {
		return validateStartedAt;
	}

	public void setValidateStartedAt(Date validateStartedAt) {
		this.validateStartedAt = validateStartedAt;
	}

	public Date getValidateCompletedAt() {
		return validateCompletedAt;
	}

	public void setValidateCompletedAt(Date validateCompletedAt) {
		this.validateCompletedAt = validateCompletedAt;
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}

	@Override
	public void startDo(TaskMonitor tm) {
        setValidateState(ValidatingState.VALIDATING);
        setValidateStartedAt(new Date());
        setValidateCompletedAt(null);
        setResult(null);
        clearMessages();
	
	}

	@Override
	public void completeDo(TaskMonitor tm) {
	    setValidateState(ValidatingState.VALIDATED);
	    setValidateCompletedAt(tm.getCompletedAt());
	}	

	@Override
	public void failDo(TaskMonitor tm) {
        setValidateState(ValidatingState.VALIDATING_FAILED);
        setValidateCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}
	
	@Override
	public void fail(Date completedAt, String error) {
		if (validateState == ValidatingState.VALIDATING) {
			setValidateState(ValidatingState.VALIDATING_FAILED);
		}
        setValidateCompletedAt(completedAt);
        setMessage(new NotificationMessage(MessageType.ERROR, error));
	}

	public ValidationResult getResult() {
		return result;
	}

	public void setResult(ValidationResult result) {
		this.result = result;
	}

	public List<ObjectId> getValidatorDocumentId() {
		return validatorDocumentId;
	}

	public void setValidatorDocumentId(List<ObjectId> validatorDocumentId) {
		this.validatorDocumentId = validatorDocumentId;
	}
   
	@Override
	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = new ResponseTaskObject();
		
    	res.setState(getValidateState().toString());
    	res.setStartedAt(getValidateStartedAt());
    	res.setCompletedAt(getValidateCompletedAt());
    	res.setMessages(getMessages());
	    	
    	res.setConforms(getResult().isConforms());
    	res.setReport(getResult().getReport());
		
		return res;
	}
}	   
