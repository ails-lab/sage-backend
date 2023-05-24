package ac.software.semantic.model;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.MappingExecuteState;

public interface ExecuteDocument {

	public List<MappingExecuteState> getExecute();

	public void setExecute(List<MappingExecuteState> execute);
	
	public MappingExecuteState getExecuteState(ObjectId databaseConfigurationId);
	
	public MappingExecuteState checkExecuteState(ObjectId databaseConfigurationId);
	
	public void deleteExecuteState(ObjectId databaseConfigurationId);
}
