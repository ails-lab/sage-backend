package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.NotificationType;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.payload.NotificationObject;
import ac.software.semantic.service.ObjectContainer;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.ExecutableContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExecuteNotificationObject extends NotificationObject {
	
	private List<ExecutionInfo> d2rmlExecution;
	
	public ExecuteNotificationObject(MappingState state, String id) {
		super(NotificationType.execute, state.toString(), id);
	}

	public ExecuteNotificationObject(MappingState state, String id, String instanceId) {
		super(NotificationType.execute, state.toString(), id, instanceId);
	}
	
//	public ExecuteNotificationObject(MappingState state, MappingContainer mc) {
//		super(NotificationType.execute, state.toString(), mc);
//	}
//
//	public ExecuteNotificationObject(MappingState state, AnnotatorContainer ac) {
//		super(NotificationType.execute, state.toString(), ac);
//	}
//
//	public ExecuteNotificationObject(MappingState state, EmbedderContainer ec) {
//		super(NotificationType.execute, state.toString(), ec);
//	}

	public ExecuteNotificationObject(ExecutableContainer ec) {
		super(NotificationType.execute, ec.getExecuteState() != null ? ec.getExecuteState().getExecuteState().toString() : MappingState.NOT_EXECUTED.toString(), ec);
	}
	
	public ExecuteNotificationObject(MappingState state, ExecutableContainer ec) {
		super(NotificationType.execute, state.toString(), ec);
	}
	
	public List<ExecutionInfo> getD2rmlExecution() {
		return d2rmlExecution;
	}

	public void setD2rmlExecution(List<ExecutionInfo> maps) {
		this.d2rmlExecution = maps;
	}

	public void addMap(ExecutionInfo map) {
		if (d2rmlExecution == null) {
			d2rmlExecution = new ArrayList<>();
		}
		d2rmlExecution.add(map);
	}


}
