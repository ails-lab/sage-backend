package ac.software.semantic.service.monitor;

import java.util.List;

import ac.software.semantic.model.ExecutionInfo;
import edu.ntua.isci.ac.d2rml.monitor.Failure;

public interface MonitorEntry {

	public boolean isStarted();

	public void setStarted(boolean started);

	public boolean isCompleted();

	public void setCompleted(boolean completed);

	public boolean isFailed();

	public void setFailed(boolean failed);

	public List<Failure> getFailures();

	public void addFailure(Failure failure);
	
	public ExecutionInfo createFromMonitorEntry();
}
