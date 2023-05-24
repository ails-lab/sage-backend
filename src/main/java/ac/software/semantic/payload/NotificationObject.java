package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.SseMessageObject;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.BaseContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.ObjectContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class NotificationObject extends SseMessageObject {

	protected String id;
	protected String instanceId;

	protected NotificationType type;
	protected String state;

	private Date issuedAt;
	
	protected Date startedAt;
	protected Date completedAt;
	
	protected Integer count;

	protected List<NotificationMessage> messages;
	
	public NotificationObject() { }
	
	public NotificationObject(NotificationType type, String state, String id) {
		this(type, state, id, null, null, null, (Integer)null);
		
		issuedAt = new Date();
	}

	public NotificationObject(NotificationType type, String state, String id, String instanceId) {
		this(type, state, id, instanceId, null, null, (Integer)null);
		
		issuedAt = new Date();
	}

	public static NotificationObject createNotificationObject(NotificationType type, String state, ObjectContainer oc) {
		if (oc instanceof MappingContainer) {
			return new 	NotificationObject(type, state, (MappingContainer)oc);
		} else if (oc instanceof AnnotatorContainer) {
			return new 	NotificationObject(type, state, (AnnotatorContainer)oc);
		} else if (oc instanceof EmbedderContainer) {
			return new 	NotificationObject(type, state, (EmbedderContainer)oc);
		} else if (oc instanceof DatasetContainer) {
			return new 	NotificationObject(type, state, (DatasetContainer)oc);
		} else {
			return null;
		}

	}
	
//	public NotificationObject(NotificationType type, String state, MappingContainer mc) {
//		this.type = type;
//		this.state = state;
//		this.id = mc.getMappingId().toString();
//		this.instanceId = mc.getMappingInstanceId() != null ? mc.getMappingInstanceId().toString() : null;
//		
//		issuedAt = new Date();
//	}

//	public NotificationObject(NotificationType type, String state, AnnotatorContainer ac) {
//		this.type = type;
//		this.state = state;
//		this.id = ac.getAnnotatorId().toString();
//		
//		issuedAt = new Date();
//	}
//
//	public NotificationObject(NotificationType type, String state, EmbedderContainer ec) {
//		this.type = type;
//		this.state = state;
//		this.id = ec.getEmbedderId().toString();
//		
//		issuedAt = new Date();
//	}
//
//	public NotificationObject(NotificationType type, String state, DatasetContainer dc) {
//		this.type = type;
//		this.state = state;
//		this.id = dc.getDatasetId().toString();
//		
//		issuedAt = new Date();
//	}
//	
//	public NotificationObject(NotificationType type, String state, PagedAnnotationValidationContainer pavc) {
//		this.type = type;
//		this.state = state;
//		this.id = pavc.getPagedAnnotationValidationId().toString();
//		this.instanceId = pavc.getAnnotationEditGroupId().toString();
//		
//		issuedAt = new Date();
//	}
	
	public NotificationObject(NotificationType type, String state, BaseContainer mc) {
		this.type = type;
		this.state = state;
		this.id = mc.getPrimaryId().toString();
		this.instanceId = mc.getSecondaryId() != null ? mc.getSecondaryId().toString() : null;
		
		issuedAt = new Date();
	}
	
	public NotificationObject(NotificationType type, String state, String id, String instanceId, Date startedAt, Date completedAt) {
		this(type, state, id, instanceId, startedAt, completedAt, (Integer)null);
		
		issuedAt = new Date();
	}

	public NotificationObject(NotificationType type, String state, DatasetContainer dc, Date startedAt, Date completedAt) {
		this.type = type;
		this.state = state;
		this.id = dc.getDatasetId().toString();
		
		this.startedAt = startedAt;
		this.completedAt = completedAt;
		
		issuedAt = new Date();
	}

	
	public NotificationObject(NotificationType type, String state, String id, String instanceId, Date startedAt, Date completedAt, Integer count) {
		this.type = type;
		this.state = state;
		this.id = id;
		this.instanceId = instanceId;
		
		this.startedAt = startedAt;
		this.completedAt = completedAt;
		
		this.count = count;
		
		issuedAt = new Date();
	}
	
	public NotificationObject(NotificationType type, String state, String id, String instanceId, Date startedAt, Date completedAt, int count, List<NotificationMessage> messages) {
		this.type = type;
		this.state = state;
		this.id = id;
		this.instanceId = instanceId;
		
		this.startedAt = startedAt;
		this.completedAt = completedAt;
		
		this.count = count;
		
		this.messages = messages;
		
		issuedAt = new Date();
	}
	

	
	public NotificationObject(NotificationType type, String state, DatasetContainer dc, Date startedAt, Date completedAt, NotificationMessage message) {
		this.type = type;
		this.state = state;
		this.id = dc.getDatasetId().toString();
		
		this.startedAt = startedAt;
		this.completedAt = completedAt;
		
		this.messages = new ArrayList<>();
		this.messages.add(message);
		
		issuedAt = new Date();
	}
	
	public NotificationObject(NotificationType type, String state, String id, String instanceId, Date startedAt, Date completedAt, NotificationMessage message) {
		this.type = type;
		this.state = state;
		this.id = id;
		this.instanceId = instanceId;
		
		this.startedAt = startedAt;
		this.completedAt = completedAt;
		
		this.messages = new ArrayList<>();
		this.messages.add(message);
		
		issuedAt = new Date();
	}


	public NotificationType getType() {
		return type;
	}

	public void setType(NotificationType type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Date getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}

	public Date getCompletedAt() {
		return completedAt;
	}

	public void setCompletedAt(Date completedAt) {
		this.completedAt = completedAt;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public List<NotificationMessage> getMessages() {
		return messages;
	}

//	public void setMessages(List<NotificationMessage> messages) {
//		this.messages = messages;
//	}
	
	public void addMessage(NotificationMessage message) {
		if (messages == null) {
			this.messages = new ArrayList<>();
		}
		
		this.messages.add(message);
	}

	public Date getIssuedAt() {
		return issuedAt;
	}

	public void setIssuedAt(Date issuedAt) {
		this.issuedAt = issuedAt;
	}
}
