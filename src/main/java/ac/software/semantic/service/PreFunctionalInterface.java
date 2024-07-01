package ac.software.semantic.service;

import java.util.Date;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.service.exception.TaskFailureException;

@FunctionalInterface
public interface PreFunctionalInterface {

	Date preTask(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException ;
	
}