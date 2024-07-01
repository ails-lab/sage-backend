package ac.software.semantic.payload.request;

import java.util.List;

import ac.software.semantic.model.UserTaskDescription;

public class UserTaskUpdateRequest implements UpdateRequest {

	private String name;
	private List<UserTaskDescription> tasks;
	
	private String cronExpression;
	private boolean freshRunOnly;
	
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public List<UserTaskDescription> getTasks() {
		return tasks;
	}

	public void setTasks(List<UserTaskDescription> tasks) {
		this.tasks = tasks;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public boolean isFreshRunOnly() {
		return freshRunOnly;
	}

	public void setFreshRunOnly(boolean freshRunOnly) {
		this.freshRunOnly = freshRunOnly;
	}
}
