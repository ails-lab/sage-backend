package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.dataset.LogicalDataset;
import edu.ntua.isci.ac.d2rml.model.informationsource.D2RMLSource;

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
	
}
