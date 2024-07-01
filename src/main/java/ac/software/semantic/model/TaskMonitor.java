package ac.software.semantic.model;

import java.util.Collection;
import java.util.Date;

import ac.software.semantic.payload.notification.NotificationObject;
import ac.software.semantic.service.monitor.MonitorEntry;

public interface TaskMonitor extends AutoCloseable {

	public NotificationObject sendMessage(NotificationObject eno);
		
	public void setFailureException(Throwable e);
	
	public Throwable getFailureException();
	
	public NotificationMessage getFailureMessage();
	
	public NotificationObject lastSentNotification();
	
	public void complete();
	
	public Date getCompletedAt();
	
//	public void start();
	
	public Collection<? extends MonitorEntry> getMonitorEntries();
	
}
