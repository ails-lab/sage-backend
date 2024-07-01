package ac.software.semantic.service;

import java.util.Date;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;

public interface RunningService<D extends SpecificationDocument, F extends Response> extends ContainerService<D,F> {

	public Date preRun(TaskDescription tdescr, WebSocketService wsService); 
	
	public Date postRunSuccess(TaskDescription tdescr, WebSocketService wsService);
	
	public Date postRunFail(TaskDescription tdescr, WebSocketService wsService);

}
