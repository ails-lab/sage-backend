package ac.software.semantic.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IndexNotificationObject extends NotificationObject {
	
//	public IndexNotificationObject(IndexingState state, String id) {
//		super(NotificationType.index, state.toString(), id);
//	}
//
	public IndexNotificationObject(IndexingState state, DatasetContainer dc) {
		super(NotificationType.index, state.toString(), dc);
	}


}
