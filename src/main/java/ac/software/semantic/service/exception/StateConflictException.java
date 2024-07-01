package ac.software.semantic.service.exception;

import ac.software.semantic.payload.response.APIResponse;

public class StateConflictException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public StateConflictException(String message) {
		super(message);
	}
	

}
