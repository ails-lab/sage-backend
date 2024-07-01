package ac.software.semantic.service.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.notification.NotificationChannel;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.notification.CreateNotificationObject;
import ac.software.semantic.payload.notification.NotificationObject;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.WebSocketService;
import ac.software.semantic.service.container.EnclosedObjectContainer;

public class GenericMonitor implements TaskMonitor {

	protected Logger logger = LoggerFactory.getLogger(GenericMonitor.class);
	
    protected WebSocketService wsService;
    
    protected Timer timer;

    protected NotificationChannel channel;
    protected UserPrincipal currentUser;
    
    protected String id;
	
    protected Throwable failureException;
	
    protected int order;
	
    protected Date startedAt;
    protected Date completedAt;
	
    protected NotificationObject lastSentNotification;
	
	public GenericMonitor(NotificationChannel channel, EnclosedObjectContainer dc, WebSocketService wsService, Date startedAt) {
		this.channel = channel;
		this.id = dc.getPrimaryId().toString();
		
		this.currentUser = dc.getCurrentUser();
		
		this.wsService = wsService;
		
		order = 0;
		this.startedAt = startedAt;
	}
	
	public NotificationObject sendMessage(NotificationObject eno) {
		eno.setOrder(order++);
		eno.getContent().setStartedAt(startedAt);
		eno.getContent().setCompletedAt(completedAt);
		
		if (failureException != null) {
			eno.getContent().addMessage(new NotificationMessage(MessageType.ERROR, failureException.getMessage()));
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}
	
	public NotificationObject sendMessage(NotificationObject eno, Date startedAt) {
		eno.setOrder(order++);
		eno.getContent().setStartedAt(startedAt);
		eno.getContent().setCompletedAt(completedAt);
		
		if (failureException != null) {
			eno.getContent().addMessage(new NotificationMessage(MessageType.ERROR, failureException.getMessage()));
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}
	
	public NotificationObject sendMessage(NotificationObject eno, String info) {
		eno.setOrder(order++);
		eno.getContent().setStartedAt(startedAt);
		eno.getContent().setCompletedAt(completedAt);
		
		eno.getContent().addMessage(new NotificationMessage(MessageType.INFO, info));
		
		if (failureException != null) {
			eno.getContent().addMessage(new NotificationMessage(MessageType.ERROR, failureException.getMessage()));
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}
	
	@Override
	public NotificationMessage getFailureMessage() {
		if (failureException != null) {
			return new NotificationMessage(MessageType.ERROR, failureException.getCause() != null ? failureException.getCause().getMessage() : failureException.getMessage());
		} else {
			return null;
		}
	}
	
	@Override
	public void setFailureException(Throwable failureException) {
		this.failureException = failureException;
	}

	@Override
	public Throwable getFailureException() {
		return failureException;
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
	
	public void complete() {
		if (completedAt == null) {
			completedAt = new Date();
		}
	}
	
	public void complete(Throwable ex) {
		if (failureException == null) { 
			failureException = ex;
		}
		
		if (completedAt == null) {
			completedAt = new Date();
		}

	}

	@Override
	public NotificationObject lastSentNotification() {
		return lastSentNotification;
	}

//	@Override
//	public void start() {
//		
//	}
	
	@Override
	public void close() throws IOException {
		if (timer != null) {
			timer.cancel();
			timer = null;
		}		
	}

	@Override
	public Collection<? extends MonitorEntry> getMonitorEntries() {
		return null;
	}

	public List<ExecutionInfo> buildExecutionInfo() {
		Collection<? extends MonitorEntry> entries = getMonitorEntries();
		
		if (entries != null && entries.size() > 0) {
			List<ExecutionInfo> res = new ArrayList<>();
		
			for (MonitorEntry me : entries) {
				res.add(me.createFromMonitorEntry());
			}
        
			return res;
		} else {
			return null;
		}
	}
}
