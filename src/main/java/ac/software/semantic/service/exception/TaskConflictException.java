package ac.software.semantic.service.exception;

import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.service.container.AnnotationValidationContainer;
import ac.software.semantic.service.container.CreatableContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.IntermediatePublishableContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.StartableContainer;
import ac.software.semantic.service.container.ValidatableContainer;
import ac.software.semantic.service.container.PreparableContainer;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SchedulableContainer;

public class TaskConflictException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public TaskConflictException(String message) {
		super(message);
	}

	public static TaskConflictException notOwner(ObjectContainer<?,?> oc) {
		return new TaskConflictException("You are not the owner of the " + APIResponse.className(oc.getClass()) + ".");
	}

	public static TaskConflictException alreadyPublished(PublishableContainer<?,?,?,?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is already published.");
	}

	public static TaskConflictException isExecuting(ExecutableContainer<?,?,?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is being executed.");
	}

	public static TaskConflictException isPublished(IntermediatePublishableContainer<?,?,?,?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is published.");
	}

	public static TaskConflictException alreadyCreated(CreatableContainer<?,?,?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is already created.");
	}

	public static TaskConflictException notPublished(IntermediatePublishableContainer<?,?,?,?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is not published.");
	}

	public static TaskConflictException notCreated(CreatableContainer<?,?,?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is not created.");
	}
	
	public static TaskConflictException isCreated(CreatableContainer<?,?,?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is created.");
	}

	public static TaskConflictException isScheduled(SchedulableContainer<?,?> sc) {
		return new TaskConflictException("The " + APIResponse.className(sc.getClass()) + " is scheduled.");
	}

	public static TaskConflictException isStarted(StartableContainer<?,?> oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is active.");
	}

	public static TaskConflictException isPreparing(PreparableContainer sc) {
		return new TaskConflictException("The " + APIResponse.className(sc.getClass()) + " is being prepared.");
	}

	public static TaskConflictException notPrepared(PreparableContainer sc) {
		return new TaskConflictException("The " + APIResponse.className(sc.getClass()) + " is not prepared.");
	}

	public static TaskConflictException unknownPrepareState(PreparableContainer sc) {
		return new TaskConflictException("Unknown prepare state of " + APIResponse.className(sc.getClass()) + ".");
	}

	public static TaskConflictException hasValidations(AnnotationValidationContainer<?,?,?> ac) {
		return new TaskConflictException("The " + APIResponse.className(ac.getClass()) + " has validations.");
	}

	public static TaskConflictException noValidator(ValidatableContainer<?,?> vc) {
		return new TaskConflictException("No validator has been specified for the " + APIResponse.className(vc.getClass()) + ".");
	}

}
