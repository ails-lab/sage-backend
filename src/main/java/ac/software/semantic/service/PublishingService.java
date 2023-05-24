package ac.software.semantic.service;

import java.util.Date;

import org.bson.types.ObjectId;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.security.UserPrincipal;

public interface PublishingService extends ContainerService {

	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
	
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
}
