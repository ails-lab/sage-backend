package ac.software.semantic.service;

import java.util.Arrays;
import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.payload.Response;
import ac.software.semantic.security.UserPrincipal;
import io.jsonwebtoken.lang.Collections;

public abstract class ObjectContainer implements BaseContainer {
	
	protected UserPrincipal currentUser;

	protected Dataset dataset;

	public UserPrincipal getCurrentUser() {
		return currentUser;
	}

	public void setCurrentUser(UserPrincipal currentUser) {
		this.currentUser = currentUser;
	}
	
	public Dataset getDataset() {
		if (dataset == null) {
			loadDataset();
		}
		
		return dataset;
	}

	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}
	
	public abstract String syncString();
	
	public String containerString() {
		return getClass().getName();
	}
	
	public abstract String saveSyncString();
	
	public abstract Response asResponse();
	
	public abstract ObjectId getPrimaryId();
	
	public ObjectId getSecondaryId() {
		return null;
	}
	
	// TODO should check different synchronization strings ( use of file system / triples store ) depending on task type
	public String synchronizationString(List<TaskType> taskType) {
		return synchronizationString(taskType.toArray(new TaskType[] {}));
	}
	
	public String synchronizationString(TaskType... taskType) {
		String res = "SYNC:" + containerString() + ":";
		
		if (taskType.length > 0) {
			
			String[] sTaskType = new String[taskType.length];
			for (int i = 0; i < taskType.length; i++) {
				sTaskType[i] = taskType[i].toString();
			}
			
			Arrays.sort(sTaskType);
			
			String taskString = sTaskType[0]; 
			for (int i = 1; i < sTaskType.length; i++) {
				taskString += "/" + sTaskType[1];
			}
			
			res += taskString + ":";
		}
		
		return (res + ":" + localSynchronizationString()).intern();
	}
	
	protected abstract String localSynchronizationString();
	
	protected abstract void load();
	
	protected abstract void loadDataset();
	



}
