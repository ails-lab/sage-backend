package ac.software.semantic.model.state;

import java.util.Date;
import java.util.Properties;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.state.IndexingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.response.ResponseTaskObject;
import ac.software.semantic.service.monitor.GenericMonitor;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IndexStateOld extends ProcessState implements UndoableState {

//	private ObjectId databaseConfigurationId; // legacy
	
	private ObjectId elasticConfigurationId;
	   
	private IndexingState indexState;
   
	private Date indexStartedAt;
	private Date indexCompletedAt;

	private ObjectId indexStructureId;
	
	private ObjectId indexId;
	
	private PublishState publish;

	public IndexStateOld() { }

//	public ObjectId getDatabaseConfigurationId() {
//		return databaseConfigurationId;
//	}
//
//	public void setDatabaseConfigurationId(ObjectId databaseConfigurationId) {
//		this.databaseConfigurationId = databaseConfigurationId;
//	}

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

	public ObjectId getIndexStructureId() {
		return indexStructureId;
	}

	public void setIndexStructureId(ObjectId indexStructureId) {
		this.indexStructureId = indexStructureId;
	}
	
	@Override
	public void startDo(TaskMonitor tm) {
        setIndexState(IndexingState.INDEXING);
        setIndexStartedAt(new Date());
        setIndexCompletedAt(null);
        clearMessages();
	}

	@Override
	public void startUndo(TaskMonitor tm) {
        setIndexState(IndexingState.UNINDEXING);
        setIndexStartedAt(new Date());
        setIndexCompletedAt(null);
        clearMessages();
	}
	
	@Override
	public void completeDo(TaskMonitor tm) {
	    setIndexState(IndexingState.INDEXED);
	    setIndexCompletedAt(tm.getCompletedAt());
	}	

	@Override
	public void failDo(TaskMonitor tm) {
        setIndexState(IndexingState.INDEXING_FAILED);
        setIndexCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}
	   
	@Override
	public void failUndo(TaskMonitor tm) {
        setIndexState(IndexingState.UNINDEXING_FAILED);
        setIndexCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}

	@Override
	public void fail(Date completedAt, String error) {
		if (indexState == IndexingState.INDEXING) {
			setIndexState(IndexingState.INDEXING_FAILED);
		} else if (indexState == IndexingState.UNINDEXING) {
			setIndexState(IndexingState.UNINDEXING_FAILED);
		}
        setIndexCompletedAt(completedAt);
        setMessage(new NotificationMessage(MessageType.ERROR, error));
	}
	
	public PublishState getPublish() {
		return publish;
	}

	public void setPublish(PublishState publish) {
		this.publish = publish;
	}

	public ObjectId getElasticConfigurationId() {
//		if (elasticConfigurationId == null) {
//			return databaseConfigurationId; // for legacy entries
//		}
		
		return elasticConfigurationId;
	}

	public void setElasticConfigurationId(ObjectId elasticConfigurationId) {
		this.elasticConfigurationId = elasticConfigurationId;
	}

	public ObjectId getIndexId() {
		return indexId;
	}

	public void setIndexId(ObjectId indexId) {
		this.indexId = indexId;
	}

	@Override
	public ResponseTaskObject createResponseState() {
		// TODO Auto-generated method stub
		return null;
	}
}	   
