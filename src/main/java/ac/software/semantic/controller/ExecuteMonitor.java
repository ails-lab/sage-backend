package ac.software.semantic.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import ac.software.semantic.model.ExecutingNotificationObject;
import ac.software.semantic.model.ExecutionInfo;
import edu.ntua.isci.ac.d2rml.model.dataset.MultiMap;
import edu.ntua.isci.ac.d2rml.model.dataset.TriplesMap;
import edu.ntua.isci.ac.d2rml.model.informationsource.D2RMLSource;
import edu.ntua.isci.ac.d2rml.monitor.Failure;
import edu.ntua.isci.ac.d2rml.monitor.Monitor;
import edu.ntua.isci.ac.d2rml.monitor.MonitorWrap;
import edu.ntua.isci.ac.d2rml.stream.D2RMLStream;

public class ExecuteMonitor implements Monitor, AutoCloseable {

	Logger logger = LoggerFactory.getLogger(ExecuteMonitor.class);
	
	private List<MonitorWrap> triplesMaps;
	
	private MonitorWrap currentWrap;

	private Timer timer;
	
    private ApplicationEventPublisher applicationEventPublisher;

    private String channel;
    
	private String id;
	private String instanceId;
	
	private String key;
	
//	ExecutionInfo ei;	
    
	public ExecuteMonitor(String channel, String id, String instanceId, ApplicationEventPublisher applicationEventPublisher) {
		this.channel = channel;
		this.id = id;
		this.instanceId = instanceId;
		
		triplesMaps = new ArrayList<>();
		this.applicationEventPublisher = applicationEventPublisher;
		
		key = "";
	}
	
	@Override
	public void startExecutingConfiguration(MultiMap tm, D2RMLSource dataSource, D2RMLStream stream) {
		
		currentWrap = new MonitorWrap(tm, dataSource, stream);
		triplesMaps.add(currentWrap);

//		String key = "";
		
		timer = new Timer();
		timer.schedule(new TimerTask() {
			public void run() {
				
				ExecutionInfo ei = new ExecutionInfo(tm.getName().toString(), dataSource.getSourceURI() != null ? dataSource.getSourceURI().toString() : "");
				ei.setKey(stream.getCurrentKey());
				ei.setPartialCount(currentWrap.getStream().getPartialCount());
				ei.setTotalCount(currentWrap.getStream().getCurrentCount());
				ei.setFailures(currentWrap.getFailures().size());
				ei.setStarted(true);
				
				if (stream.getCurrentKey() != null && !key.equals(stream.getCurrentKey())) {
					logger.info("Mapping execution update -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size()) ;
					
					key = stream.getCurrentKey();
				}
				
				
				SSEController.send(channel, applicationEventPublisher, this, new ExecutingNotificationObject(id, instanceId, ei));
			}
		}, 0, 200);
			
	}

	@Override
	public void finishExecutingCurrentConfiguration() {
		logger.info("Mapping execution completed -- id: " + id + (instanceId != null ? ("_" + instanceId) : "") + ", tm: " + currentWrap.getTriplesMap().getName().toString() + ", ds: " + currentWrap.getDataSource().getSourceURI() + ", ck: " + currentWrap.getStream().getCurrentKey() + ", pk: " + currentWrap.getStream().getPartialCount() + ", cc: " + currentWrap.getStream().getCurrentCount() + ", fl: " + currentWrap.getFailures().size()) ;

		ExecutionInfo ei = new ExecutionInfo(currentWrap.getTriplesMap().getName().toString(), currentWrap.getDataSource().getSourceURI() != null ? currentWrap.getDataSource().getSourceURI().toString() : "");
		ei.setKey(currentWrap.getStream().getCurrentKey());
		ei.setPartialCount(currentWrap.getStream().getPartialCount());
		ei.setTotalCount(currentWrap.getStream().getCurrentCount());
		ei.setFailures(currentWrap.getFailures().size());
		ei.setStarted(true);
		ei.setCompleted(true);
		
		
		SSEController.send(channel, applicationEventPublisher, this, new ExecutingNotificationObject(id, instanceId, ei));

		if (timer != null) {
			timer.cancel();
		}
		// TODO Auto-generated method stub
		
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
	public void currentConfigurationFailed() {
		ExecutionInfo ei = new ExecutionInfo(currentWrap.getTriplesMap().getName().toString(), currentWrap.getDataSource().getSourceURI() != null ? currentWrap.getDataSource().getSourceURI().toString() : "");
//		ei.setKey(currentWrap.getStream().getCurrentKey());
//		ei.setPartialCount(currentWrap.getStream().partialCount());
//		ei.setTotalCount(currentWrap.getStream().currentCount());
//		ei.setFailures(currentWrap.getFailures().size());
		ei.setStarted(true);
		ei.setFailed(true);
		
		logger.info("Mapping execution failed: " + currentWrap.getTriplesMap().getName().toString() + " / " + currentWrap.getDataSource()) ;

		SSEController.send(channel, applicationEventPublisher, this, new ExecutingNotificationObject(id, instanceId, ei));

		if (timer != null) {
			timer.cancel();
		}
	}
	
	@Override
	public void close() throws IOException {
		if (timer != null) {
			timer.cancel();
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

}
