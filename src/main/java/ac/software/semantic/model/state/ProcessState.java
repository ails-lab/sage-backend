package ac.software.semantic.model.state;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.payload.response.ResponseTaskObject;
import ac.software.semantic.service.monitor.GenericMonitor;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class ProcessState implements DoableState {

	public List<NotificationMessage> messages;
	
	public List<NotificationMessage> getMessages() {
		return messages;
	}

	public void setMessages(List<NotificationMessage> messages) {
		this.messages = messages;
	}
	
	public void setMessage(NotificationMessage message) {
		if (message != null) {
			messages = new ArrayList<>();
			messages.add(message);
		} else {
			messages = null;
		}
	}
	
	public void clearMessages() {
		this.messages = null;
	}
	
	public void addMessage(NotificationMessage message) {
		if (message != null) {
			if (this.messages == null) {
				this.messages = new ArrayList<>();
			}
		
			this.messages.add(message);
		}
	}

	public abstract void fail(Date completedAt, String error);
	
	public abstract ResponseTaskObject createResponseState();
}
