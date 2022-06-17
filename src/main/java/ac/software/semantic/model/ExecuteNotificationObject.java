package ac.software.semantic.model;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;


public class ExecuteNotificationObject {

	private String type = "execute";
	private List<ExecutionInfo> maps;
	private String id;
	private String instanceId;
	private MappingState state;
	
	private int count;
	
//	private String subtype = "EXECUTION_START";
	
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Date startedAt;
	
	
	public ExecuteNotificationObject(String id, String instanceId, List<ExecutionInfo> maps, Date startedAt) {
		this.state = MappingState.EXECUTING;
		this.id = id;
		this.instanceId = instanceId;
		this.maps = maps;
		this.startedAt = startedAt;
		this.count = -1;
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


	public List<ExecutionInfo> getMaps() {
		return maps;
	}


	public void setMaps(List<ExecutionInfo> maps) {
		this.maps = maps;
	}


//	public String getSubtype() {
//		return subtype;
//	}
//
//
//	public void setSubtype(String subtype) {
//		this.subtype = subtype;
//	}


	public Date getStartedAt() {
		return startedAt;
	}


	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}


	public int getCount() {
		return count;
	}


	public void setCount(int count) {
		this.count = count;
	}

}
