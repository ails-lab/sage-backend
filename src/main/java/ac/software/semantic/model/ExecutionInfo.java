package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.service.monitor.ExecuteMonitor;
import ac.software.semantic.service.monitor.MonitorEntry;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.dataset.LogicalDataset;
import edu.ntua.isci.ac.d2rml.model.informationsource.D2RMLSource;
import edu.ntua.isci.ac.d2rml.model.logicalinput.LogicalInput;
import edu.ntua.isci.ac.d2rml.model.map.ConditionalMap;
import edu.ntua.isci.ac.d2rml.monitor.Failure;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExecutionInfo {
	private String triplesMap;
	private String dataSource;
	private String key;
	
	private Integer partialCount;
	private Integer totalCount;
	private Integer failures;
	
	private Integer totalItems;
	
	private boolean failed;
	private boolean completed;
	private boolean started;
	
	private List<NotificationMessage> messages;

	public ExecutionInfo() {
		
	}
	
	public ExecutionInfo(String triplesMap) {
		this(triplesMap, null);
	}
	
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

	public Integer getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Integer totalCount) {
		this.totalCount = totalCount;
	}

	public int getPartialCount() {
		return partialCount;
	}

	public void setPartialCount(Integer partialCount) {
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
//        		System.out.println("TM " + tm);
        		
        		for (ConditionalMap<? extends LogicalInput> cli : tm.getLogicalInput()) {
	        		List<D2RMLSource> datasources = cli.getTermMap().getSources();
	        		for (D2RMLSource ds : datasources) {
//	        			System.out.println("A " + tm.getName());
//	        			System.out.println("B " + ds.getSourceURI());
	        			tms.add(new ExecutionInfo(tm.getName().toString(), ds.getSourceURI() != null ? ds.getSourceURI().toString() : null));
	        		}
        		}
        	}
        }
        
//      System.out.println("TMS " + tms);
        
        return tms;
	}

	public Integer getFailures() {
		return failures;
	}

	public void setFailures(Integer failures) {
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
	
	public void buildMessages(MonitorEntry currentWrap) {
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
	
	public Integer getTotalItems() {
		return totalItems;
	}

	public void setTotalItems(Integer totalItems) {
		this.totalItems = totalItems;
	}

}
