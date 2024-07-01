package ac.software.semantic.model.base;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.RunState;

public interface RunnableDocument {

	public List<RunState> getRun();

	public void setRun(List<RunState> run);
	
	public RunState getRunState(ObjectId fileSystemConfigurationId);
	
	public RunState checkRunState(ObjectId fileSystemConfigurationId);
	
	public void deleteRunState(ObjectId fileSystemConfigurationId);
}
