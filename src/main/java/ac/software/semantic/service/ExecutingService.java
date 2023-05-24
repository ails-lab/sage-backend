package ac.software.semantic.service;

import java.util.Date;

import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.TaskDescription;

public interface ExecutingService extends ContainerService {

	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
	
//	public Optional<String> previewLastExecution(ObjectContainer oc) throws IOException;
}
