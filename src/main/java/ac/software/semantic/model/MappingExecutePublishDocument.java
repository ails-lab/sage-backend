package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;

public abstract class MappingExecutePublishDocument<P extends PublishState> extends PublishDocument<P> implements ExecuteDocument {

	private List<MappingExecuteState> execute;
	
	public MappingExecutePublishDocument() {
	   super();
	   execute = new ArrayList<>();
	}
	
	public List<MappingExecuteState> getExecute() {
		return execute;
	}

	public void setExecute(List<MappingExecuteState> execute) {
		this.execute = execute;
	}	
	
	public MappingExecuteState getExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {
			for (MappingExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			execute = new ArrayList<>();
		}
		
		MappingExecuteState s = new MappingExecuteState();
		s.setExecuteState(MappingState.NOT_EXECUTED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		execute.add(s);
		
		return s;
	}
	
	public MappingExecuteState checkExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {		
			for (MappingExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}		
	
	public void deleteExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {
			for (int i = 0; i < execute.size(); i++) {
				if (execute.get(i).getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					execute.remove(i);
					break;
				}
			}
		} else {
			execute = new ArrayList<>();
		}
	}

}
