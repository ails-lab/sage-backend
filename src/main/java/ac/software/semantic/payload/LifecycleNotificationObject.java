package ac.software.semantic.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.model.state.PagedAnnotationValidationState;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LifecycleNotificationObject extends NotificationObject {
	
	public LifecycleNotificationObject(PagedAnnotationValidationState state, PagedAnnotationValidationContainer pavc) {
		super(NotificationType.lifecycle, state.toString(), pavc);
	}
}
