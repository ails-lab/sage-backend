package ac.software.semantic.payload.notification;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.ValidatingState;
import ac.software.semantic.service.container.ValidatableContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValidateNotificationObject extends NotificationObject {
	
	public ValidateNotificationObject(ValidatableContainer<?,?> ic) throws Exception {
//		super(NotificationType.validate, ic.checkValidateState() != null ? ic.checkValidateState().getValidateState().toString() : ValidatingState.NOT_VALIDATED.toString(), ic);
		super(NotificationType.validate, ic);
	}


}
