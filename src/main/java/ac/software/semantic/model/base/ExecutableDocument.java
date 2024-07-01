package ac.software.semantic.model.base;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;

public interface ExecutableDocument<M extends ExecuteState> extends SpecificationDocument {

	public List<M> getExecute();

	public void setExecute(List<M> execute);
	
	public M getExecuteState(ObjectId databaseConfigurationId);
	
	public M checkExecuteState(ObjectId databaseConfigurationId);
	
	public void deleteExecuteState(ObjectId databaseConfigurationId);
}
