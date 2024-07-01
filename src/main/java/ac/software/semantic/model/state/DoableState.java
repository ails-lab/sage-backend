package ac.software.semantic.model.state;

import java.util.Properties;

import ac.software.semantic.model.TaskMonitor;

public interface DoableState {

	public void startDo(TaskMonitor tm);
		
	public void completeDo(TaskMonitor tm);
	
	public void failDo(TaskMonitor tm);
}
