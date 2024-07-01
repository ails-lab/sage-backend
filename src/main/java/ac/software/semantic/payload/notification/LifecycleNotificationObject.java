package ac.software.semantic.payload.notification;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.PagedAnnotationValidationState;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LifecycleNotificationObject extends NotificationObject {
	
//	public LifecycleNotificationObject(PagedAnnotationValidationState state, PagedAnnotationValidationContainer pavc) {
//		super(NotificationType.lifecycle, state.toString(), pavc);
//	}
	
	public LifecycleNotificationObject(PagedAnnotationValidationContainer pavc) {
		super(NotificationType.lifecycle, pavc);
	}
}
