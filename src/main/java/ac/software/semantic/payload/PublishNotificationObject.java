package ac.software.semantic.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.ObjectContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.PublishableContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PublishNotificationObject extends NotificationObject {
	
	public PublishNotificationObject(DatasetState state, String id) {
		super(NotificationType.publish, state.toString(), id);
	}

//	public PublishNotificationObject(DatasetState state, DatasetContainer dc) {
//		super(NotificationType.publish, state.toString(), dc);
//	}
//	
//	public PublishNotificationObject(DatasetState state, AnnotatorContainer ac) {
//		super(NotificationType.publish, state.toString(), ac);
//	}
//	
//	public PublishNotificationObject(DatasetState state, EmbedderContainer ec) {
//		super(NotificationType.publish, state.toString(), ec);
//	}
//
//	public PublishNotificationObject(DatasetState state, PagedAnnotationValidationContainer pavc) {
//		super(NotificationType.publish, state.toString(), pavc);
//	}

	public PublishNotificationObject(PublishableContainer pc) throws Exception {
		super(NotificationType.publish, pc.checkPublishState() != null ? pc.checkPublishState().getPublishState().toString() : DatasetState.UNPUBLISHED.toString(), pc);
	}
	public PublishNotificationObject(DatasetState state, PublishableContainer pc) {
		super(NotificationType.publish, state.toString(), (ObjectContainer)pc);
	}
	
}
