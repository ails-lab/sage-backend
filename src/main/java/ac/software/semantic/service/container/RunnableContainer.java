package ac.software.semantic.service.container;

import java.util.Date;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.base.RunnableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.RunningState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.model.state.RunState;
import ac.software.semantic.payload.response.Response;

public interface RunnableContainer<D extends SpecificationDocument, F extends Response> extends BaseContainer<D,F> {
	
	public FileSystemConfiguration getContainerFileSystemConfiguration();
	
	public RunnableDocument getRunDocument();
	
	default public RunState getRunState() {
		return getRunDocument().getRunState(getContainerFileSystemConfiguration().getId());
	}

	default public RunState checkRunState() {
		return getRunDocument().checkRunState(getContainerFileSystemConfiguration().getId());
	}

	default public void deleteRunState() {
		getRunDocument().deleteRunState(getContainerFileSystemConfiguration().getId());
	}
	
	default public void failRunning() throws Exception {			
		update(iec -> {			
			RunState ies = ((RunnableContainer)iec).getRunDocument().checkRunState(getContainerFileSystemConfiguration().getId());
	
			if (ies != null) {
				ies.fail(new Date(), "Unknown error.");
			}
		});
	}
	
}
