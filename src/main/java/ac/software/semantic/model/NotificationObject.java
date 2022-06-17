package ac.software.semantic.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;

public class NotificationObject {

	private String type;
	private String state;
	private String id;
	
	private String instanceId;
	
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Date startedAt;
	
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Date completedAt;
	
	private int count;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String message;

	public NotificationObject(String type, String state, String id) {
		this(type, state, id, null, null, null, -1);
	}

	public NotificationObject(String type, String state, String id, String instanceId, Date startedAt, Date completedAt) {
		this(type, state, id, instanceId, startedAt, completedAt, -1);
	}
		
	public NotificationObject(String type, String state, String id, String instanceId, Date startedAt, Date completedAt, int count) {
		this.type = type;
		this.state = state;
		this.id = id;
		this.instanceId = instanceId;
		
		this.startedAt = startedAt;
		this.completedAt = completedAt;
		
		this.count = count;
	}
	
	public NotificationObject(String type, String state, String id, String instanceId, Date startedAt, Date completedAt, String message) {
		this.type = type;
		this.state = state;
		this.id = id;
		this.instanceId = instanceId;
		
		this.startedAt = startedAt;
		this.completedAt = completedAt;
		
		this.message = message;
	}


	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Date getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}

	public Date getCompletedAt() {
		return completedAt;
	}

	public void setCompletedAt(Date completedAt) {
		this.completedAt = completedAt;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
