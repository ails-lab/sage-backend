package ac.software.semantic.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;

public class ExecutingNotificationObject {

	private String type = "execute";
	private String id;
	private String instanceId;
	private MappingState state;

	private ExecutionInfo executionInfo;

//	private String subtype = "EXECUTION_PROGRESS";
	
	public ExecutingNotificationObject(String id, String instanceId, ExecutionInfo executionInfo) {
		this.state = MappingState.EXECUTING;
		this.id = id;
		this.instanceId = instanceId;
		this.executionInfo = executionInfo;
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

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public MappingState getState() {
		return state;
	}

	public void setState(MappingState state) {
		this.state = state;
	}

	public ExecutionInfo getExecutionInfo() {
		return executionInfo;
	}

	public void setExecutionInfo(ExecutionInfo executionInfo) {
		this.executionInfo = executionInfo;
	}

//	public String getSubtype() {
//		return subtype;
//	}
//
//	public void setSubtype(String subtype) {
//		this.subtype = subtype;
//	}

}
