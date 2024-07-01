package ac.software.semantic.service.container;

import java.util.Date;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.ExecutableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.payload.response.Response;

public interface ExecutableContainer<D extends SpecificationDocument, F extends Response, M extends ExecuteState, I extends EnclosingDocument> extends EnclosedBaseContainer<D,F,I> {
	
	public D getObject();
		
	public FileSystemConfiguration getContainerFileSystemConfiguration();
	
	public default ExecutableDocument<M> getExecuteDocument() {
		return (ExecutableDocument<M>)getObject();
	}
	
	default public M getExecuteState() {
		return getExecuteDocument().getExecuteState(getContainerFileSystemConfiguration().getId());
	}

	default public M checkExecuteState() {
		return getExecuteDocument().checkExecuteState(getContainerFileSystemConfiguration().getId());
	}

	default public void deleteExecuteState() {
		getExecuteDocument().deleteExecuteState(getContainerFileSystemConfiguration().getId());
	}
	
	public boolean clearExecution() throws Exception;  

	public boolean clearExecution(M es) throws Exception;
	
	public TaskType getExecuteTask();
	
	public TaskType getClearLastExecutionTask();
	
	default public void failExecution() throws Exception {			
		update(iec -> {			
			M ies = ((ExecutableContainer<D,F,M,I>)iec).getExecuteDocument().checkExecuteState(getContainerFileSystemConfiguration().getId());
	
			if (ies != null) {
				ies.setExecuteState(MappingState.EXECUTION_FAILED);
				ies.setExecuteCompletedAt(new Date());
				ies.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
				ies.setExecuteMessage(null);
			}
		});
	}
	
	default boolean isExecuting() {
		M ec = getExecuteDocument().checkExecuteState(getContainerFileSystemConfiguration().getId());
		if (ec != null) {
			if (ec.getExecuteState() == MappingState.EXECUTING) {
				return true;
			}
		} 
			
		return false;
	}
	
	default boolean isExecuted() {
		M ec = getExecuteDocument().checkExecuteState(getContainerFileSystemConfiguration().getId());
		if (ec != null) {
			if (ec.getExecuteState() == MappingState.EXECUTED) {
				return true;
			}
		} 
			
		return false;
	}
	
}
