package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.UserTaskDescription;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserTaskResponse implements Response, RunResponse {
	
	private String id;
	
	private String uuid;
	
	private Date createdAt;
	private Date updatedAt;
	
	private String name;
	
	private List<UserTaskDescription> tasks;
	
	private String cronExpression;
	private boolean scheduled;
	
	private ResponseTaskObject runState;
	
	private boolean freshRunOnly;

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public List<UserTaskDescription> getTasks() {
		return tasks;
	}

	public void setTasks(List<UserTaskDescription> tasks) {
		this.tasks = tasks;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public boolean isScheduled() {
		return scheduled;
	}

	public void setScheduled(boolean scheduled) {
		this.scheduled = scheduled;
	}

	public ResponseTaskObject getRunState() {
		return runState;
	}

	public void setRunState(ResponseTaskObject runState) {
		this.runState = runState;
	}

	public boolean isFreshRunOnly() {
		return freshRunOnly;
	}

	public void setFreshRunOnly(boolean freshRunOnly) {
		this.freshRunOnly = freshRunOnly;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	
}
