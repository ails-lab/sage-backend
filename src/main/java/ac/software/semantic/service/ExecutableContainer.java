package ac.software.semantic.service;

import ac.software.semantic.model.ExecuteDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.state.MappingExecuteState;

public interface ExecutableContainer extends BaseContainer {
	
	public FileSystemConfiguration getContainerFileSystemConfiguration();
	
	public ExecuteDocument getExecuteDocument();
	
	default public MappingExecuteState getExecuteState() {
		return getExecuteDocument().getExecuteState(getContainerFileSystemConfiguration().getId());
	}

	default MappingExecuteState checkExecuteState() {
		return getExecuteDocument().checkExecuteState(getContainerFileSystemConfiguration().getId());
	}

	default void deleteExecuteState() {
		getExecuteDocument().deleteExecuteState(getContainerFileSystemConfiguration().getId());
	}
	
	public boolean clearExecution() throws Exception;  

	public boolean clearExecution(MappingExecuteState es) throws Exception;
	
	public TaskType getExecuteTask();
	
	public TaskType getClearLastExecutionTask();
	
}
