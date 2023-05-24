package ac.software.semantic.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.dataset.LogicalDataset;
import edu.ntua.isci.ac.d2rml.model.dataset.MappingDataset;
import edu.ntua.isci.ac.d2rml.model.informationsource.D2RMLSource;
import edu.ntua.isci.ac.d2rml.monitor.Failure;
import edu.ntua.isci.ac.d2rml.monitor.Monitor;
import edu.ntua.isci.ac.d2rml.monitor.MonitorWrap;
import edu.ntua.isci.ac.d2rml.output.RDFOutputHandler;
import edu.ntua.isci.ac.d2rml.stream.D2RMLStream;

public class ExecuteMonitor implements Monitor, AutoCloseable, TaskMonitor {

	Logger logger = LoggerFactory.getLogger(ExecuteMonitor.class);
	
	private List<MonitorWrap> triplesMaps;
	private Map<MonitorWrapKey, MonitorWrap> map;
	
	private MonitorWrap currentWrap;

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

	private class MonitorWrapKey {
		private MappingDataset tm;
		private D2RMLSource dataSource;
		
		public MonitorWrapKey(MappingDataset tm, D2RMLSource dataSource) {
			this.tm = tm;
			this.dataSource = dataSource;
		}
		
		public int hashCode() {
			return Objects.hashCode(tm.getName().toString(),  dataSource.getSourceURI() != null ? dataSource.getSourceURI().toString() : "");
		}
		
		public boolean equals(Object obj) {
			if (!(obj instanceof MonitorWrapKey)) {
				return false;
			}
			
			MappingDataset tm2 = ((MonitorWrapKey)obj).tm;
			D2RMLSource dataSource2 = ((MonitorWrapKey)obj).dataSource;
			
			if (!tm.getName().toString().equals(tm2.getName().toString())) {
				return false;
			}
			
			Resource ds1 = dataSource2.getSourceURI();  
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
	
	public ExecuteMonitor(NotificationChannel channel, ObjectContainer oc, WebSocketService wsService, Date startedAt) {
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
        	
        	if (tm instanceof MappingDataset && tm.getName().isURIResource()) {
        		List<D2RMLSource> datasources = tm.getLogicalInput().getSources();
        		for (D2RMLSource ds : datasources) {
        			MonitorWrap mw = new MonitorWrap((MappingDataset)tm, ds, null);
        			triplesMaps.add(mw);
        			map.put(new MonitorWrapKey((MappingDataset)tm, ds), mw);
        			
//        			System.out.println("CREATING " + tm.getName() + " " + ds.getSourceURI());
        		}
        	}
        }
        
        this.outHandler = outHandler;
	}

	public NotificationObject sendMessage(NotificationObject eno) {
		eno.setOrder(order++);
		eno.setStartedAt(startedAt);
		eno.setCompletedAt(completedAt);
		
		if (eno instanceof ExecuteNotificationObject) {
			((ExecuteNotificationObject)eno).setD2rmlExecution(getMaps());
		}
		
		if (getFailureMessage() != null) {
			eno.addMessage(getFailureMessage());
		}
		
		if (totalCount == null && outHandler != null) {
			eno.setCount(outHandler.getTotalItems());
		} else {
			eno.setCount(totalCount);
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}

	public NotificationObject sendMessage(NotificationObject eno, int count) {
		eno.setOrder(order++);
		eno.setStartedAt(startedAt);
		eno.setCompletedAt(completedAt);
		
		if (eno instanceof ExecuteNotificationObject) {
			((ExecuteNotificationObject)eno).setD2rmlExecution(getMaps());
		}

		if (getFailureMessage() != null) {
			eno.addMessage(getFailureMessage());
		}
		
		eno.setCount(count);
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
	public void startExecutingConfiguration(MappingDataset tm, D2RMLSource dataSource, D2RMLStream stream) {
		
		currentWrap = map.get(new MonitorWrapKey(tm, dataSource));
		if (currentWrap == null) {
			currentWrap = new MonitorWrap(tm, dataSource, stream);
			
			triplesMaps.add(currentWrap);
			map.put(new MonitorWrapKey(tm, dataSource), currentWrap);
			
		} else {
			currentWrap.setStream(stream);
		}

		logger.info("Mapping execution started -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI()) ;
//		String key = "";
		
		currentWrap.setStarted(true);
		this.completedAt = null;
		
		timer = new Timer();
		timer.schedule(new TimerTask() {
			public void run() {
				
				if (stream.getCurrentKey() != null && !key.equals(stream.getCurrentKey())) {
					logger.info("Mapping execution update -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size()) ;
					
					key = stream.getCurrentKey();
				} 
				
				sendMessage(new ExecuteNotificationObject(MappingState.EXECUTING, id, instanceId));
			}
		}, 0, 250);
			
	}

	@Override
	public void finishExecutingCurrentConfiguration() {
		logger.info("Mapping execution completed -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size()) ;
		
		currentWrap.setCompleted(true);
		this.completedAt = new Date();

		if (timer != null) {
			timer.cancel();
			timer = null;
		}
		
		sendMessage(new ExecuteNotificationObject(MappingState.EXECUTING, id, instanceId));		

		currentWrap = null;
	
	}
	
	@Override
	public void currentConfigurationFailed(Exception ex) {
		if (currentWrap != null) {
			
			currentWrap.setFailed(true);
			
			logger.info("Mapping execution failed: " + currentWrap.getTriplesMap().getName().toString() + " / " + currentWrap.getDataSource()) ;
	
			ExecuteNotificationObject eno = new ExecuteNotificationObject(MappingState.EXECUTING, id, instanceId);
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
	public List<MonitorWrap> getConfigurations() {
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
		if (triplesMaps != null && triplesMaps.size() > 0) {
			List<ExecutionInfo> res = new ArrayList<>();
		
			for (MonitorWrap mw : triplesMaps) {
				res.add(ExecutionInfo.createFromMonitorWrap(mw));
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

}
