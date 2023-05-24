package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.service.ExecuteMonitor;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.dataset.LogicalDataset;
import edu.ntua.isci.ac.d2rml.model.informationsource.D2RMLSource;
import edu.ntua.isci.ac.d2rml.monitor.Failure;
import edu.ntua.isci.ac.d2rml.monitor.MonitorWrap;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExecutionInfo {
	private String triplesMap;
	private String dataSource;
	private String key;
	
	private int partialCount;
	private int totalCount;
	private int failures;
	
	private boolean failed;
	private boolean completed;
	private boolean started;
	
	private List<NotificationMessage> messages;
	
	public ExecutionInfo(String triplesMap, String dataSource) {
		this.triplesMap = triplesMap;
		this.dataSource = dataSource;
		
		this.partialCount = 0;
		this.totalCount = 0;
		
		this.key = "";
	}

	public String getTriplesMap() {
		return triplesMap;
	}

	public void setTriplesMap(String triplesMap) {
		this.triplesMap = triplesMap;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public int getPartialCount() {
		return partialCount;
	}

	public void setPartialCount(int partialCount) {
		this.partialCount = partialCount;
	}

	public boolean isCompleted() {
		return completed;
	}

	public void setCompleted(boolean completed) {
		this.completed = completed;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public boolean isStarted() {
		return started;
	}

	public void setStarted(boolean started) {
		this.started = started;
	}
	
	public static List<ExecutionInfo> createStructure(D2RMLModel d2rml) {
		List<ExecutionInfo> tms = new ArrayList<>();
        List<LogicalDataset> t;
//        if (d2rml.getTriplesMapOrdering() != null) {
//        	t = d2rml.getTriplesMapOrdering().getTriplesMaps();
//        } else {
        	t = d2rml.getLogicalDatasets();
//        }
        
        for (LogicalDataset tm : t) {
        	
        	if (tm.getName().isURIResource()) {
//        		System.out.println(tm);
        		
        		List<D2RMLSource> datasources = tm.getLogicalInput().getSources();
        		for (D2RMLSource ds : datasources) {
//        			System.out.println(tm.getName());
//        			System.out.println(ds.getSourceURI());
        			tms.add(new ExecutionInfo(tm.getName().toString(), ds.getSourceURI() != null ? ds.getSourceURI().toString() : null));
        		}
        	}
        }
        
//      System.out.println("TMS " + tms);
        
        return tms;
	}

	public int getFailures() {
		return failures;
	}

	public void setFailures(int failures) {
		this.failures = failures;
	}

	public boolean isFailed() {
		return failed;
	}

	public void setFailed(boolean failed) {
		this.failed = failed;
	}

	public List<NotificationMessage> getMessages() {
		return messages;
	}

	public void setMessages(List<NotificationMessage> messages) {
		this.messages = messages;
	}
	
	public void buildMessages(MonitorWrap currentWrap) {
		this.messages = null;
		
		if (currentWrap.getFailures() != null && currentWrap.getFailures().size() > 0) {
			List<NotificationMessage> res = new ArrayList<>();
			
			int count = 0;
			for (Failure f : currentWrap.getFailures()) {
				res.add(new NotificationMessage(MessageType.ERROR, f.getException().getMessage()));
				
				if (count++ > ExecuteMonitor.MAX_MESSAGES) {
					break;
				}
			}
			this.messages = res;
		}

	}
	
	public static ExecutionInfo createFromMonitorWrap(MonitorWrap mw) {
		ExecutionInfo ei = new ExecutionInfo(mw.getTriplesMap().getName().toString(), mw.getDataSource().getSourceURI() != null ? mw.getDataSource().getSourceURI().toString() : "");
		if (mw.getStream() != null) {
			ei.setKey(mw.getStream().getCurrentKey());
			ei.setPartialCount(mw.getStream().getPartialCount());
			ei.setTotalCount(mw.getStream().getCurrentCount());
		}
		ei.setFailures(mw.getFailures().size());
		ei.setStarted(mw.isStarted());
		ei.setCompleted(mw.isCompleted());
		ei.setFailed(mw.isFailed());
		ei.buildMessages(mw);
		
		return ei;
	}

}
