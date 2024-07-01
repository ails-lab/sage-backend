package ac.software.semantic.service.container;

import java.util.Date;

import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.base.CreatableDocument;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.payload.response.Response;

public interface CreatableContainer<D extends SpecificationDocument, F extends Response, C extends CreateState, I extends EnclosingDocument> extends EnclosedBaseContainer<D,F,I> {

	public default ElasticConfiguration getContainerElasticConfiguration() {
		return null;
	}
	
	public default FileSystemConfiguration getContainerFileSystemConfiguration() {
		return null;
	}
	
	public CreatableDocument<C> getCreateDocument();
	
	default public C getCreateState() {
		return getCreateDocument().getCreateState(getContainerElasticConfiguration() != null ? getContainerElasticConfiguration().getId() : null, 
				                                  getContainerFileSystemConfiguration() != null ? getContainerFileSystemConfiguration().getId() : null);
	}

	default public C checkCreateState() {
		return getCreateDocument().checkCreateState(getContainerElasticConfiguration() != null ? getContainerElasticConfiguration().getId() : null, 
                                                    getContainerFileSystemConfiguration() != null ? getContainerFileSystemConfiguration().getId() : null);
	}

	default public void deleteCreateState() {
		getCreateDocument().deleteCreateState(getContainerElasticConfiguration() != null ? getContainerElasticConfiguration().getId() : null, 
                                              getContainerFileSystemConfiguration() != null ? getContainerFileSystemConfiguration().getId() : null);
	}
	
	public TaskType getCreateTask();
	
	public TaskType getDestroyTask();
	
	public TaskType getRecreateTask();
	
	default public void failCreating() throws Exception {			
		update(iec -> {			
			C ies = ((CreatableContainer<D,F,C,I>)iec).getCreateDocument().checkCreateState(getContainerElasticConfiguration() != null ? getContainerElasticConfiguration().getId() : null, 
                    getContainerFileSystemConfiguration() != null ? getContainerFileSystemConfiguration().getId() : null);
	
			if (ies != null) {
				ies.setCreateState(CreatingState.CREATING_FAILED);
				ies.setCreateCompletedAt(new Date());
				ies.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
			}
		});
	}
	
	default boolean isCreated() {
		C cs = checkCreateState();
		
		if (cs != null && cs.getCreateState() == CreatingState.CREATED) {
			return true;
		} 
			
		return false;
	}
	

}
