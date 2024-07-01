package ac.software.semantic.service;

import java.util.Date;

import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.exception.TaskFailureException;

public interface CreatingService<D extends SpecificationDocument, F extends Response> extends ContainerService<D,F> {

	public ListenableFuture<Date> create(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
	
	public ListenableFuture<Date> destroy(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
	
}
