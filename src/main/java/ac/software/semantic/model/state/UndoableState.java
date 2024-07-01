package ac.software.semantic.model.state;

import java.util.Properties;

import ac.software.semantic.model.TaskMonitor;

public interface UndoableState {

	public void startUndo(TaskMonitor tm);
		
//	public void completeUndo(TaskMonitor tm);
	
	public void failUndo(TaskMonitor tm);
}
