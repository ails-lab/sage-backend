package ac.software.semantic.service.exception;

import java.util.Date;

public class TaskFailureException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private Date failureTime;
	
	public TaskFailureException(Exception cause, Date failureTime) {
		super(cause);
	
		this.failureTime = failureTime;
	}

	public Date getFailureTime() {
		return failureTime;
	}

	public void setFailureTime(Date failureTime) {
		this.failureTime = failureTime;
	}
	

}
