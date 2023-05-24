package ac.software.semantic.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.service.DatasetService.DatasetContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateDistributionNotificationObject extends NotificationObject {
	
	public CreateDistributionNotificationObject(MappingState state, DatasetContainer dc) {
		super(NotificationType.createDistribution, state.toString(), dc);
	}
	
	public CreateDistributionNotificationObject(MappingState state, DatasetContainer dc, NotificationMessage no) {
		super(NotificationType.createDistribution, state.toString(), dc);
		
		this.addMessage(no);
	}
}
