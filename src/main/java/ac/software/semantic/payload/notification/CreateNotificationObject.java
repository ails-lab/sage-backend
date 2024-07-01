package ac.software.semantic.payload.notification;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.service.container.CreatableContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateNotificationObject extends NotificationObject {
	
	public CreateNotificationObject(CreatingState state, String id, IndexDocument idoc) {
		super(NotificationType.create, state.toString(), id);
	}

	public CreateNotificationObject(CreatableContainer<?,?,?,?> ic) throws Exception {
//		super(NotificationType.create, ic.checkCreateState() != null ? ic.checkCreateState().getCreateState().toString() : CreatingState.NOT_CREATED.toString(), ic);
		super(NotificationType.create, ic);
	}

}
