package ac.software.semantic.model;

import java.util.Date;

import ac.software.semantic.payload.NotificationObject;

public interface TaskMonitor extends AutoCloseable {

	public NotificationObject sendMessage(NotificationObject eno);
		
	public void setFailureException(Throwable e);
	
	public Throwable getFailureException();
	
	public NotificationMessage getFailureMessage();
	
	public NotificationObject lastSentNotification();
	
	public void complete();
	
	public Date getCompletedAt();
	
	
}
