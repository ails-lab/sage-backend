package ac.software.semantic.service.monitor;

import java.util.ArrayList;
import java.util.List;

import edu.ntua.isci.ac.d2rml.monitor.Failure;

public abstract class MonitorEntryBase implements MonitorEntry {

	protected boolean started;
	protected boolean completed;
	protected boolean failed;
	
	protected List<Failure> failures;
	
	protected MonitorEntryBase() {
		this.failures = new ArrayList<>();
	}
	
	public boolean isStarted() {
		return started;
	}

	public void setStarted(boolean started) {
		this.started = started;
	}

	public boolean isCompleted() {
		return completed;
	}

	public void setCompleted(boolean completed) {
		this.completed = completed;
	}

	public boolean isFailed() {
		return failed;
	}

	public void setFailed(boolean failed) {
		this.failed = failed;
	}

	public List<Failure> getFailures() {
		return failures;
	}

	public void addFailure(Failure failure) {
		this.failures.add(failure);
	}
}
