package ac.software.semantic.payload.notification;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.service.container.ExecutableContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExecuteNotificationObject extends NotificationObject {
	
//	public ExecuteNotificationObject(MappingState state, String id) {
//		super(NotificationType.execute, state.toString(), id);
//	}

	public ExecuteNotificationObject(MappingState state, String id, String instanceId) {
		super(NotificationType.execute, state.toString(), id, instanceId);
	}

	public ExecuteNotificationObject(ExecutableContainer<?,?,?,?> ec) {
//		super(NotificationType.execute, ec.getExecuteState() != null ? ec.getExecuteState().getExecuteState().toString() : MappingState.NOT_EXECUTED.toString(), ec);
		super(NotificationType.execute, ec);
	}
	
//	public ExecuteNotificationObject(MappingState state, ExecutableContainer<?,?,?,?> ec) {
//		super(NotificationType.execute, state.toString(), ec);
//	}

}
