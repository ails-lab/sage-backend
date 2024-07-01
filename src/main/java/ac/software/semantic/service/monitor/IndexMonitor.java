package ac.software.semantic.service.monitor;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.constants.notification.NotificationChannel;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.payload.notification.CreateNotificationObject;
import ac.software.semantic.payload.notification.NotificationObject;
import ac.software.semantic.service.WebSocketService;
import ac.software.semantic.service.container.EnclosedObjectContainer;


public class IndexMonitor extends GenericMonitor {

	private Map<String, IndexMonitorEntry> elementsMap;
	
	private IndexMonitorEntry currentElement;

	public class IndexMonitorEntry extends MonitorEntryBase {
		private String element;
		private Integer count;
		private Integer total;
		
		IndexMonitorEntry(String element) {
			super();
			this.element = element;
		}

		public String getElement() {
			return element;
		}

		public void setElement(String element) {
			this.element = element;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}
		
		public Integer getTotal() {
			return total;
		}

		public void setTotal(Integer total) {
			this.total = total;
		}

		public ExecutionInfo createFromMonitorEntry() {
			ExecutionInfo ei = new ExecutionInfo(getElement());
			ei.setTotalCount(getCount());
			ei.setFailures(getFailures().size());
			ei.setStarted(isStarted());
			ei.setCompleted(isCompleted());
			ei.setFailed(isFailed());
			ei.buildMessages(this);
			
			return ei;
		}

	}
	
	public IndexMonitor(NotificationChannel channel, EnclosedObjectContainer dc, WebSocketService wsService, Date startedAt) {
		super(channel, dc, wsService, startedAt);
		
		elementsMap = new LinkedHashMap<>();
	}
	
	@Override
	public Collection<? extends MonitorEntry> getMonitorEntries() {
		return elementsMap.values();
	}
	
	public void createStructure(IndexStructure idxStruct) {
        for (ClassIndexElement cie : idxStruct.getElements()) {
        	elementsMap.put(cie.getClazz(), new IndexMonitorEntry(cie.getClazz()));
        }
	}
	
	
	public void startElement(ClassIndexElement cie) {
		currentElement = elementsMap.get(cie.getClazz());
		currentElement.setStarted(true);
		currentElement.count = 0;
	}
	
	public void incrementCurrentCount() {
		currentElement.count++;
	}
	
	public void completeCurrentElement() {
		currentElement.setCompleted(true);
	}
	
	public void failCurrentElement(Throwable ex) {
		currentElement.setFailed(true);
		
		this.completedAt = new Date();
		
		if (failureException == null) { 
			failureException = ex;
		}
	}

	public void startIndexing(IndexDocument idoc) {
		timer = new Timer();
		timer.schedule(new TimerTask() {
			public void run() {
				sendMessage(new CreateNotificationObject(CreatingState.CREATING, id, idoc));
			}
		}, 0, 500);		
	}
	
	public NotificationObject sendMessage(NotificationObject eno) {
		eno.setOrder(order++);
		eno.getContent().setStartedAt(startedAt);
		eno.getContent().setCompletedAt(completedAt);
		
		if (eno instanceof CreateNotificationObject) {
			((CreateNotificationObject)eno).getContent().setProgress(buildExecutionInfo());
		}
		
//		if (message != null) {
//			eno.addMessage(new NotificationMessage(MessageType.INFO, message += (count != null ? count : "")));
//		}
		
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
		
		if (eno instanceof CreateNotificationObject) {
			((CreateNotificationObject)eno).getContent().setProgress(buildExecutionInfo());
		}
		
		eno.getContent().addMessage(new NotificationMessage(MessageType.INFO, info));
		
		if (failureException != null) {
			eno.getContent().addMessage(new NotificationMessage(MessageType.ERROR, failureException.getMessage()));
		}
		
		wsService.send(channel, currentUser, eno);
		
		lastSentNotification = eno;
		
		return eno;
	}
	
//	public NotificationObject sendMessage(NotificationObject eno, Date startedAt) {
//		eno.setOrder(order++);
//		eno.setStartedAt(startedAt);
//		eno.setCompletedAt(completedAt);
//		
//		if (message != null) {
//			eno.addMessage(new NotificationMessage(MessageType.INFO, message += (count != null ? count : "")));
//		}
//		
//		if (failureException != null) {
//			eno.addMessage(new NotificationMessage(MessageType.ERROR, failureException.getMessage()));
//		}
//		
//		wsService.send(channel, currentUser, eno);
//		
//		lastSentNotification = eno;
//		
//		return eno;
//	}
	
}
