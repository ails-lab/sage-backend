package ac.software.semantic.service.container;

import java.util.Date;

import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.service.WebSocketService;
import ac.software.semantic.service.exception.TaskFailureException;

@FunctionalInterface
public interface TaskFunctionalInterface {

	public ListenableFuture<Date> applyTask(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException;
}