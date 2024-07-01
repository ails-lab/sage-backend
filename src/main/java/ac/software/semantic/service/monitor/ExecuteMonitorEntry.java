package ac.software.semantic.service.monitor;

import ac.software.semantic.model.ExecutionInfo;
import edu.ntua.isci.ac.d2rml.model.dataset.MappingDataset;
import edu.ntua.isci.ac.d2rml.model.informationsource.D2RMLSource;
import edu.ntua.isci.ac.d2rml.model.logicalinput.LogicalInput;
import edu.ntua.isci.ac.d2rml.monitor.MonitorWrap;
import edu.ntua.isci.ac.d2rml.stream.D2RMLStream;

public class ExecuteMonitorEntry extends MonitorWrap  implements MonitorEntry {

	public ExecuteMonitorEntry(MappingDataset tm, LogicalInput logicalInput, D2RMLSource dataSource, D2RMLStream stream) {
		super(tm, logicalInput, dataSource, stream);
	}

	@Override
	public ExecutionInfo createFromMonitorEntry() {
		ExecutionInfo ei = new ExecutionInfo(getTriplesMap().getName().toString(), getDataSource().getSourceURI() != null ? getDataSource().getSourceURI().toString() : "");
		if (getStream() != null) {
			ei.setKey(getStream().getCurrentKey());
			ei.setPartialCount(getStream().getPartialCount());
			ei.setTotalCount(getStream().getCurrentCount());
		}
		ei.setFailures(getFailures().size());
		ei.setStarted(isStarted());
		ei.setCompleted(isCompleted());
		ei.setFailed(isFailed());
		ei.buildMessages(this);
		
		return ei;
	}
}
