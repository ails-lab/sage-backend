package ac.software.semantic.model.state;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.SerializationType;

public class CreateDistributionState extends ProcessState {

	private ObjectId fileSystemConfigurationId;
	private ObjectId tripleStoreConfigurationId;
	   
	private MappingState createDistrubitionState;
	
	private Date createDistributionStartedAt;
	private Date createDistributionCompletedAt;
	
	private List<SerializationType> serialization; 
	
	private String compression;
	
	public CreateDistributionState() { 
	}
	
	public MappingState getCreateDistributionState() {
		return createDistrubitionState;
	}

	public void setCreateDistributionState(MappingState createDistributionState) {
		this.createDistrubitionState = createDistributionState;
	}

	public String getCreateDistributionStartStamp() {
		return CurrentState.stampFormat.format(createDistributionStartedAt);
	}

	public Date getCreateDistributionStartedAt() {
		return createDistributionStartedAt;
	}

	public void setCreateDistributionStartedAt(Date createDistributionStartedAt) {
		this.createDistributionStartedAt = createDistributionStartedAt;
	}

	public Date getCreateDistributionCompletedAt() {
		return createDistributionCompletedAt;
	}

	public void setCreateDistributionCompletedAt(Date createDistributionCompletedAt) {
		this.createDistributionCompletedAt = createDistributionCompletedAt;
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}

	public ObjectId getTripleStoreConfigurationId() {
		return tripleStoreConfigurationId;
	}

	public void setTripleStoreConfigurationId(ObjectId tripleStoreConfigurationId) {
		this.tripleStoreConfigurationId = tripleStoreConfigurationId;
	}

	public String getCompression() {
		return compression;
	}

	public void setCompression(String compression) {
		this.compression = compression;
	}

	public List<SerializationType> getSerialization() {
		return serialization;
	}

	public void setSerialization(List<SerializationType> serialization) {
		this.serialization = serialization;
	}

	@Override
	public void startDo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void completeDo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void failDo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startUndo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void failUndo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Date completedAt, String error) {
		// TODO Auto-generated method stub
		
	}
   
}	   
