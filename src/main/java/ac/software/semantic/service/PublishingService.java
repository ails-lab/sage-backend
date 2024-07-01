package ac.software.semantic.service;

import java.util.Date;

import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.exception.TaskFailureException;

public interface PublishingService<D extends SpecificationDocument, F extends Response> extends ContainerService<D,F> {

	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
	
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
	
	public default Date prePublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return null;
	}
	
	public default Date postPublishSuccess(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return tdescr.getMonitor() != null ? tdescr.getMonitor().getCompletedAt() : null;
	}
	
	public default Date postPublishFail(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return tdescr.getMonitor() != null ? tdescr.getMonitor().getCompletedAt() : null;
	}
	
	public default Date preUnpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return tdescr.getMonitor() != null ? tdescr.getMonitor().getCompletedAt() : null;
	}
	
	public default Date postUnpublishSuccess(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return tdescr.getMonitor() != null ? tdescr.getMonitor().getCompletedAt() : null;
	}
	
	public default Date postUnpublishFail(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return tdescr.getMonitor() != null ? tdescr.getMonitor().getCompletedAt() : null;
	}
}
