package ac.software.semantic.payload.notification;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.PagedAnnotationValidationState;
import ac.software.semantic.model.constants.state.ValidatingState;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.state.ValidateState;
import ac.software.semantic.payload.response.ResponseTaskObject;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.container.BaseContainer;
import ac.software.semantic.service.container.CreatableContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.ValidatableContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class NotificationObject extends MessageObject {

	protected String id;
	protected String instanceId;

	protected NotificationType type;

	private Date issuedAt;
	
	protected ResponseTaskObject content;
	
	public NotificationObject() { }
	
	public NotificationObject(NotificationType type, String state, String id) {
		this(type, state, id, null, null, null, (Integer)null);
		
		issuedAt = new Date();
	}

	public NotificationObject(NotificationType type, String state, String id, String instanceId) {
		this(type, state, id, instanceId, null, null, (Integer)null);
		
		issuedAt = new Date();
	}

	public static NotificationObject createNotificationObject(NotificationType type, String state, EnclosedObjectContainer<?,?,?> oc) {
		return new 	NotificationObject(type, state, oc);
	}
	
	public NotificationObject(NotificationType type, String state, BaseContainer<?,?> mc) {
		this.type = type;
		this.id = mc.getPrimaryId().toString();
		this.instanceId = mc.getSecondaryId() != null ? mc.getSecondaryId().toString() : null;
		
		content = new ResponseTaskObject();
		content.setState(state);
		
//		if (mc instanceof PublishableContainer && !(mc instanceof ExecutableContainer)) { // hack // why ?
//        	PublishState<?> ps = ((PublishableContainer) mc).checkPublishState();
//        	if (ps != null) {
//	    		content = ps.createResponseState();
//        	}
//		}
		
		issuedAt = new Date();
	}
	
	private void setDetails(NotificationType type, BaseContainer<?,?> mc) {
		this.type = type;
		this.id = mc.getPrimaryId().toString();
		this.instanceId = mc.getSecondaryId() != null ? mc.getSecondaryId().toString() : null;
		
		issuedAt = new Date();
	}


	public NotificationObject(NotificationType type, CreatableContainer<?,?,?,?> cc) {
//		String state = ic.checkCreateState() != null ? ic.checkCreateState().getCreateState().toString() : CreatingState.NOT_CREATED.toString();
		
		setDetails(type, cc);
		
		CreateState ps = cc.checkCreateState();
		if (ps == null) {
			content = new ResponseTaskObject();
			content.setState(CreatingState.NOT_CREATED.toString());
		} else {
			content = ps.createResponseState();
		}
	}

	public NotificationObject(NotificationType type, ExecutableContainer<?,?,?,?> ec) {
//		String state = ec.getExecuteState() != null ? ec.getExecuteState().getExecuteState().toString() : MappingState.NOT_EXECUTED.toString();
		
		setDetails(type, ec);

		ExecuteState ps = ec.checkExecuteState();
		if (ps == null) {
			content = new ResponseTaskObject();
			content.setState(MappingState.NOT_EXECUTED.toString());
		} else {
			content = ps.createResponseState();
		}
		
		content.setStateMessage(ps.getExecuteMessage());
	}
	
	public NotificationObject(NotificationType type, PublishableContainer<?,?,?,?,?> pc) {
//		String state = pc.checkPublishState() != null ? pc.checkPublishState().getPublishState().toString() : DatasetState.UNPUBLISHED.toString();
		
		setDetails(type, pc);
		
		PublishState<?> ps = pc.checkPublishState();
		if (ps == null) {
			content = new ResponseTaskObject();
			content.setState(DatasetState.UNPUBLISHED.toString());
		} else {
			content = ps.createResponseState();
		}

	}

	public NotificationObject(NotificationType type, ValidatableContainer<?,?> vc) {
//		String state = ic.checkValidateState() != null ? ic.checkValidateState().getValidateState().toString() : ValidatingState.NOT_VALIDATED.toString();
		
		setDetails(type, vc);
		
		ValidateState ps = vc.checkValidateState();
		if (ps == null) {
			content = new ResponseTaskObject();
			content.setState(ValidatingState.NOT_VALIDATED.toString());
		} else {
			content = ps.createResponseState();
		}
	}
	
	public NotificationObject(NotificationType type, PagedAnnotationValidationContainer vc) {
//		String state = ic.checkValidateState() != null ? ic.checkValidateState().getValidateState().toString() : ValidatingState.NOT_VALIDATED.toString();
		
		setDetails(type, vc);
		
		PagedAnnotationValidationState ps = vc.getObject().getLifecycle();
		content = new ResponseTaskObject();
		content.setState(ps.toString());
	}

	private NotificationObject(NotificationType type, String state, String id, String instanceId, Date startedAt, Date completedAt, Integer count) {
		this.type = type;
		this.id = id;
		this.instanceId = instanceId;
		
		content = new ResponseTaskObject();
		content.setState(state);
		content.setStartedAt(startedAt);
		content.setCompletedAt(completedAt);
		content.setCount(count);
		
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

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public Date getIssuedAt() {
		return issuedAt;
	}

	public void setIssuedAt(Date issuedAt) {
		this.issuedAt = issuedAt;
	}
	
	public ResponseTaskObject getContent() {
		return content;
	}

}
