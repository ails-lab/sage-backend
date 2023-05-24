package ac.software.semantic.service;

import ac.software.semantic.payload.APIResponse;

public class TaskConflictException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public TaskConflictException(String message) {
		super(message);
	}
	
	public static TaskConflictException alreadyPublished(PublishableContainer oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is already published.");
	}

	public static TaskConflictException isPublished(PublishableContainer oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is published.");
	}

	public static TaskConflictException alreadyIndexed(ObjectContainer oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is already indexed.");
	}

	public static TaskConflictException notPublished(PublishableContainer oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is not published.");
	}

	public static TaskConflictException notIndexed(ObjectContainer oc) {
		return new TaskConflictException("The " + APIResponse.className(oc.getClass()) + " is not indexed.");
	}

}
