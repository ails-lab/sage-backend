package ac.software.semantic.model.state;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.service.GenericMonitor;

public class PublishState extends ProcessState {

	protected ObjectId databaseConfigurationId;
	   
	protected DatasetState publishState;
   
	protected Date publishStartedAt;
	protected Date publishCompletedAt;

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
	public void startUndo(TaskMonitor tm) {
		setPublishState(DatasetState.UNPUBLISHING);
		setPublishStartedAt(new Date());
		setPublishCompletedAt(null);
		clearMessages();
	}

	@Override
	public void failDo(TaskMonitor tm) {
		setPublishState(DatasetState.PUBLISHING_FAILED);
		setPublishCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}
	
	@Override
	public void failUndo(TaskMonitor tm) {
		setPublishState(DatasetState.UNPUBLISHING_FAILED);
		setPublishCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}

	@Override
	public void completeDo(TaskMonitor tm) {
		setPublishState(DatasetState.PUBLISHED);
		setPublishCompletedAt(tm.getCompletedAt());
	}

	@Override
	public void fail(Date completedAt, String error) {
		// TODO Auto-generated method stub
		
	}

}	   
