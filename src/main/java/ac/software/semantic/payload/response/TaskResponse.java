package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.TaskType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskResponse {
	private String id;
	private String uuid;

	private Date startTime;
	
	private Date endTime;
	
	private TaskState state;

	private String failureException;
	
	private TaskType type;
	private String description;
	
	private boolean stoppable;
	
	private List<TaskResponse> children;

	public TaskResponse() {}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public TaskState getState() {
		return state;
	}

	public void setState(TaskState state) {
		this.state = state;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public String getFailureException() {
		return failureException;
	}

	public void setFailureException(Throwable ex) {
		if (ex != null) {
			this.failureException = ex.toString();
		}
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public TaskType getType() {
		return type;
	}

	public void setType(TaskType type) {
		this.type = type;
	}

	public boolean isStoppable() {
		return stoppable;
	}

	public void setStoppable(boolean stoppable) {
		this.stoppable = stoppable;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public List<TaskResponse> getChildren() {
		return children;
	}

	public void setChildren(List<TaskResponse> children) {
		this.children = children;
	}

	public void addChild(TaskResponse child) {
		if (children == null) {
			children = new ArrayList<>();
		}
		this.children.add(child);
	}
}
