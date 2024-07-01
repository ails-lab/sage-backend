package ac.software.semantic.service.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.shaded.com.google.common.base.Objects;

import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.notification.NotificationChannel;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.notification.ExecuteNotificationObject;
import ac.software.semantic.payload.notification.NotificationObject;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.WebSocketService;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.dataset.LogicalDataset;
import edu.ntua.isci.ac.d2rml.model.dataset.MappingDataset;
import edu.ntua.isci.ac.d2rml.model.informationsource.D2RMLSource;
import edu.ntua.isci.ac.d2rml.model.logicalinput.LogicalInput;
import edu.ntua.isci.ac.d2rml.model.map.ConditionalMap;
import edu.ntua.isci.ac.d2rml.monitor.Failure;
import edu.ntua.isci.ac.d2rml.monitor.Monitor;
import edu.ntua.isci.ac.d2rml.monitor.MonitorWrap;
import edu.ntua.isci.ac.d2rml.output.RDFOutputHandler;
import edu.ntua.isci.ac.d2rml.stream.D2RMLStream;

public class ExecuteMonitor implements Monitor, AutoCloseable, TaskMonitor {

	Logger logger = LoggerFactory.getLogger(ExecuteMonitor.class);
	
	private List<ExecuteMonitorEntry> triplesMaps;
	private Map<MonitorWrapKey, ExecuteMonitorEntry> map;
	
	private ExecuteMonitorEntry currentWrap;

	private Timer timer;
	
    private WebSocketService wsService;

    private NotificationChannel channel;
    private UserPrincipal currentUser;
    
	private String id;
	private String instanceId;
	
	private String key;
	
	private Throwable failureException;
	
	private int order;
	
	private Date startedAt;
	private Date completedAt;
	
	private Integer totalCount;
	
	private NotificationObject lastSentNotification;
	
	public static int MAX_MESSAGES = 3;
	
	private RDFOutputHandler outHandler;
	
	private String stateMessage;

	private class MonitorWrapKey {
		private MappingDataset tm;
		private LogicalInput logicalInput;
		private D2RMLSource dataSource;
		
		public MonitorWrapKey(MappingDataset tm, LogicalInput logicalInput, D2RMLSource dataSource) {
			this.tm = tm;
			this.logicalInput = logicalInput;
			this.dataSource = dataSource;
		}
		
		public int hashCode() {
			return Objects.hashCode(tm.getName().toString(),  logicalInput.getSourceURI() != null ? logicalInput.getSourceURI().toString() : "", dataSource.getSourceURI() != null ? dataSource.getSourceURI().toString() : "");
		}
		
		public boolean equals(Object obj) {
			if (!(obj instanceof MonitorWrapKey)) {
				return false;
			}
			
			MappingDataset tm2 = ((MonitorWrapKey)obj).tm;
			LogicalInput logicalInput2 = ((MonitorWrapKey)obj).logicalInput;
			D2RMLSource dataSource2 = ((MonitorWrapKey)obj).dataSource;

			if (!tm.getName().toString().equals(tm2.getName().toString())) {
				return false;
			}
			
			Resource li1 = logicalInput.getSourceURI();  
			Resource li2 = logicalInput2.getSourceURI();
			
			if (li1 != null && li2 != null) {
				if (!li1.equals(li2)) {
					return false;
				}
			}
			
			if (li1 != li2) {
				return false;
			}
			
			Resource ds1 = dataSource.getSourceURI();  
			Resource ds2 = dataSource2.getSourceURI();
			
			if (ds1 != null && ds2 != null) {
				if (!ds1.equals(ds2)) {
					return false;
				} else {
					return true;
				}
			}
			
			if (ds1 != ds2) {
				return false;
			}
			
			return true;
			
		}
	}
	
	public ExecuteMonitor(NotificationChannel channel, String id, UserPrincipal currentUser, WebSocketService wsService, Date startedAt) {
		this.channel = channel;
		this.id = id;
		
		this.currentUser = currentUser;
		
		triplesMaps = new ArrayList<>();
		this.wsService = wsService;
		
		key = "";
		
		order = 0;
		this.startedAt = startedAt;
		
		map = new HashMap<>();
	}
	
	public ExecuteMonitor(NotificationChannel channel, EnclosedObjectContainer oc, WebSocketService wsService, Date startedAt) {
		this.channel = channel;
	
		this.id = oc.getPrimaryId().toString();
		if (oc.getSecondaryId() != null) {
			this.instanceId = oc.getSecondaryId().toString();	
		}
		
		this.currentUser = oc.getCurrentUser();
		
		triplesMaps = new ArrayList<>();
		this.wsService = wsService;
		
		key = "";
		
		order = 0;
		this.startedAt = startedAt;
		
		map = new HashMap<>();
	}
	
	public void createStructure(D2RMLModel d2rml) {
		createStructure(d2rml, null);
	}
	
	public void createStructure(D2RMLModel d2rml, RDFOutputHandler outHandler) {
        for (LogicalDataset tm : d2rml.getLogicalDatasets()) {
//        	System.out.println("TM " + tm);
        	if (tm instanceof MappingDataset && tm.getName().isURIResource()) {
        		for (ConditionalMap<? extends LogicalInput> cli : tm.getLogicalInput()) {
	        		List<D2RMLSource> datasources = cli.getTermMap().getSources();
	        		for (D2RMLSource ds : datasources) {
	        			ExecuteMonitorEntry mw = new ExecuteMonitorEntry((MappingDataset)tm, cli.getTermMap(), ds, null);
	        			triplesMaps.add(mw);
	        			map.put(new MonitorWrapKey((MappingDataset)tm, cli.getTermMap(), ds), mw);
	        			
//	        			System.out.println("CREATING " + tm.getName() + " " + ds.getSourceURI());
	        		}
        		}
        	}
        }
        
        this.outHandler = outHandler;
	}

	public NotificationObject sendMessage(NotificationObject eno) {
		eno.setOrder(order++);
		eno.getContent().setStartedAt(startedAt);
		eno.getContent().setCompletedAt(completedAt);
		
		if (eno instanceof ExecuteNotificationObject) {
			((ExecuteNotificationObject)eno).getContent().setProgress(getMaps());
		}
		
		if (getFailureMessage() != null) {
			eno.getContent().addMessage(getFailureMessage());
		}
		
		if (totalCount == null && outHandler != null) {
			eno.getContent().setCount(outHandler.getTotalItems());
		} else {
			eno.getContent().setCount(totalCount);
		}
		
		if (eno.getContent().getStateMessage() == null) {
			eno.getContent().setStateMessage(stateMessage);
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}

	public NotificationObject sendMessage(NotificationObject eno, int count) {
		eno.setOrder(order++);
		eno.getContent().setStartedAt(startedAt);
		eno.getContent().setCompletedAt(completedAt);
		
		if (eno instanceof ExecuteNotificationObject) {
			((ExecuteNotificationObject)eno).getContent().setProgress(getMaps());
		}

		if (getFailureMessage() != null) {
			eno.getContent().addMessage(getFailureMessage());
		}
		
		eno.getContent().setCount(count);
		if (eno.getContent().getStateMessage() == null) {
			eno.getContent().setStateMessage(stateMessage);
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}

//	public List<NotificationMessage> getFailureMessages() {
//		List<NotificationMessage> res = new ArrayList<>();
//	
//		int count = 0;
//		for (MonitorWrap mw : triplesMaps) {
//			if (mw.getFailures() != null) {
//				for (Failure f : mw.getFailures()) {
//					res.add(new NotificationMessage(MessageType.ERROR,
//							mw.getTriplesMap().getName().toString(), mw.getDataSource().getSourceURI() != null ? mw.getDataSource().getSourceURI().toString() : "", null, null,
//							f.getException().getMessage()));
//					
//					if (count++ > MAX_MESSAGES) {
//						return res;
//					}					
//				}
//			}
//		}
//		
//		return res;
//	}
	
	@Override
	public NotificationMessage getFailureMessage() {
		if (failureException != null) {
			return new NotificationMessage(MessageType.ERROR, failureException.getCause() != null ? failureException.getCause().getMessage() : failureException.getMessage());
		} else {
			return null;
		}
	}
	
	@Override
	public void startExecutingConfiguration(MappingDataset tm, LogicalInput logicalInput, D2RMLSource dataSource, D2RMLStream stream) {
		
		currentWrap = map.get(new MonitorWrapKey(tm, logicalInput, dataSource));
		if (currentWrap == null) {
			currentWrap = new ExecuteMonitorEntry(tm, logicalInput, dataSource, stream);
			
			triplesMaps.add(currentWrap);
			map.put(new MonitorWrapKey(tm, logicalInput, dataSource), currentWrap);
			
		} else {
			currentWrap.setStream(stream);
		}

//		logger.info("Mapping execution started -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI()) ;
//		String key = "";
		
		currentWrap.setStarted(true);
		this.completedAt = null;
		
		if (timer == null) {
			timer = new Timer();
			timer.schedule(new TimerTask() {
				public void run() {
					if (stream.getCurrentKey() != null && !key.equals(stream.getCurrentKey())) {
	//					logger.info("Mapping execution update -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size()) ;
						
						key = stream.getCurrentKey();
					} 
					
					NotificationObject eno = new ExecuteNotificationObject(MappingState.EXECUTING, id, instanceId);
					eno.getContent().setStateMessage(stateMessage);
					
					sendMessage(eno);
				}
			}, 0, 250);
		}			
	}

	@Override
	public void finishExecutingCurrentConfiguration() {
		setStateMessage(null);
		
//		logger.info("Mapping execution completed -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size()) ;
		
		currentWrap.setCompleted(true);
		this.completedAt = new Date();

		if (timer != null) {
			timer.cancel();
			timer = null;
		}
		
		NotificationObject eno = new ExecuteNotificationObject(MappingState.EXECUTING, id, instanceId);
		sendMessage(eno);		

		currentWrap = null;
	
	}
	
	@Override
	public void currentConfigurationFailed(Exception ex) {
		setStateMessage(null);
		
		if (currentWrap != null) {
			
			currentWrap.setFailed(true);
			
			logger.info("Mapping execution failed: " + currentWrap.getTriplesMap().getName().toString() + " / " + currentWrap.getDataSource()) ;
	
			NotificationObject eno = new ExecuteNotificationObject(MappingState.EXECUTING, id, instanceId);
			sendMessage(eno);
	
			if (timer != null) {
				timer.cancel();
				timer = null;
			}
		}
		
		this.completedAt = new Date();
		
		if (failureException == null) { 
			failureException = ex;
		}
		
		currentWrap = null;
	}	

	@Override
	public void currentIterationCompleted() {
//		logger.info("Mapping iteration completed -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI().toString() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size()) ;
		
		currentWrap.setFinished(currentWrap.getStream().getCurrentCount());
		currentWrap.setKey(currentWrap.getStream().getCurrentKey());
		currentWrap.setCompletedCountForKey(currentWrap.getStream().getPartialCount());
	}
	
	@Override
	public void currentIterationFailed(Exception ex) {
		logger.info("Mapping iteration failed -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size() + ", ex: " + ex.toString()) ;

		currentWrap.addFailure(new Failure(currentWrap.getStream().getCurrentKey(), currentWrap.getStream().getPartialCount(), ex));
	}

	@Override
	public void close() throws IOException {
		if (timer != null) {
			timer.cancel();
			timer = null;
		}		
	}

	@Override
	public List<ExecuteMonitorEntry> getConfigurations() {
		return triplesMaps;
	}

	@Override
	public MonitorWrap getCurrentConfiguration() {
		return currentWrap;
	}

	@Override
	public void setFailureException(Throwable failureException) {
		this.failureException = failureException;
	}

	@Override
	public Throwable getFailureException() {
		return failureException;
	}
	
	public List<ExecutionInfo> getMaps() {
		Collection<? extends MonitorEntry> entries = getMonitorEntries(); 
		if (entries != null && entries.size() > 0) {
			List<ExecutionInfo> res = new ArrayList<>();
		
			for (MonitorEntry mw : entries) {
				res.add(mw.createFromMonitorEntry());
			}
        
			return res;
		} else {
			return null;
		}
	}

	public Date getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}

	@Override
	public Date getCompletedAt() {
		return completedAt;
	}

	public void setCompletedAt(Date completedAt) {
		this.completedAt = completedAt;
	}
	
	@Override
	public void complete(Throwable ex) {
		if (failureException == null) { 
			failureException = ex;
		}
		
		complete();
	}
	
	@Override
	public void complete() {
		
		if (timer != null) {
			timer.cancel();
			timer = null;
		}
		
		if (completedAt == null) {
			completedAt = new Date();
		}
	}

	public Integer getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Integer totalCount) {
		this.totalCount = totalCount;
	}

	@Override
	public NotificationObject lastSentNotification() {
		return lastSentNotification;
	}

	@Override
	public Collection<? extends MonitorEntry> getMonitorEntries() {
		return triplesMaps;
	}

	public String getStateMessage() {
		return stateMessage;
	}

	public void setStateMessage(String stateMessage) {
		this.stateMessage = stateMessage;
	}

}
