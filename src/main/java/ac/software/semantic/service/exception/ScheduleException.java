package ac.software.semantic.service.exception;

import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.service.container.EnclosedObjectContainer;

public class ScheduleException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public ScheduleException(String message) {
		super(message);
	}
	
	public static ScheduleException alreadyScheduled(EnclosedObjectContainer oc) {
		return new ScheduleException("The " + APIResponse.className(oc.getClass()) + " is already scheduled.");
	}

	public static ScheduleException invalidCronExpression(String cronExpression) {
		return new ScheduleException("Cron expression " + cronExpression + " is invalid.");
	}
}
