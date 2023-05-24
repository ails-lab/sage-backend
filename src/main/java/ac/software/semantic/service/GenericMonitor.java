package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.payload.PublishNotificationObject;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;

public class GenericMonitor implements TaskMonitor {

	Logger logger = LoggerFactory.getLogger(GenericMonitor.class);
	
    private WebSocketService wsService;

    private NotificationChannel channel;
    private UserPrincipal currentUser;
    
	private String id;
	
	private Throwable failureException;
	
	private int order;
	
	private Date startedAt;
	private Date completedAt;
	
	private NotificationObject lastSentNotification;
	
	public GenericMonitor(NotificationChannel channel, ObjectContainer dc, WebSocketService wsService, Date startedAt) {
		this.channel = channel;
		this.id = dc.getPrimaryId().toString();
		
		this.currentUser = dc.getCurrentUser();
		
		this.wsService = wsService;
		
		order = 0;
		this.startedAt = startedAt;
	}
	
	public NotificationObject sendMessage(NotificationObject eno) {
		eno.setOrder(order++);
		eno.setStartedAt(startedAt);
		eno.setCompletedAt(completedAt);
		
		if (failureException != null) {
			eno.addMessage(new NotificationMessage(MessageType.ERROR, failureException.getMessage()));
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}
	
	public NotificationObject sendMessage(NotificationObject eno, Date startedAt) {
		eno.setOrder(order++);
		eno.setStartedAt(startedAt);
		eno.setCompletedAt(completedAt);
		
		if (failureException != null) {
			eno.addMessage(new NotificationMessage(MessageType.ERROR, failureException.getMessage()));
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

	@Override
	public void close() throws Exception {
		
	}

}
