package ac.software.semantic.payload.notification;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.PublishableContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PublishNotificationObject extends NotificationObject {
	
	public PublishNotificationObject(PublishableContainer pc)  {
//		super(NotificationType.publish, pc.checkPublishState() != null ? pc.checkPublishState().getPublishState().toString() : DatasetState.UNPUBLISHED.toString(), pc);
		super(NotificationType.publish, pc);
	}
	
}
