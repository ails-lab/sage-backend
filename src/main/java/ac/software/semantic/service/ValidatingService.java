package ac.software.semantic.service;

import java.util.Date;

import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.exception.TaskFailureException;

public interface ValidatingService<D extends SpecificationDocument, F extends Response> extends ContainerService<D,F> {

	public ListenableFuture<Date> validate(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
	
//	public Optional<String> previewLastExecution(ObjectContainer oc) throws IOException;
}
