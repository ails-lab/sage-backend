package ac.software.semantic.model.state;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.service.GenericMonitor;

public abstract class ProcessState {

	@JsonInclude(JsonInclude.Include.NON_NULL)
	public List<NotificationMessage> messages;
	
	public List<NotificationMessage> getMessages() {
		return messages;
	}

	public void setMessages(List<NotificationMessage> messages) {
		this.messages = messages;
	}
	
	public void setMessage(NotificationMessage message) {
		messages = new ArrayList<>();
		messages.add(message);
	}
	
	public void clearMessages() {
		this.messages = null;
	}
	
	public void addMessage(NotificationMessage message) {
		if (this.messages == null) {
			this.messages = new ArrayList<>();
		}
		
		if (message != null) {
			this.messages.add(message);
		}
	}
	
	public abstract void startDo(TaskMonitor tm);
	
	public abstract void completeDo(TaskMonitor tm);
	
	public abstract void failDo(TaskMonitor tm);
	
	public abstract void startUndo(TaskMonitor tm);
	
//	public abstract void completeUndo(TaskMonitor tm);
	
	public abstract void failUndo(TaskMonitor tm);

	public abstract void fail(Date completedAt, String error);
	
}
