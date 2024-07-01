package ac.software.semantic.model.state;

import java.util.Date;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.payload.response.ResponseTaskObject;
import ac.software.semantic.service.monitor.GenericMonitor;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MappingExecuteState extends ExecuteState  {

	private Integer count;
	private Integer executeShards;
	
	private Integer sparqlCount;
	private Integer sparqlExecuteShards;

	public MappingExecuteState() {
		super();
		this.count = -1;
		this.executeShards = -1;
		
//		this.sparqlCount = -1;
//		this.sparqlExecuteShards = -1;
	}
	
	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	@Override
	public Integer getExecuteShards() {
		return executeShards;
	}

	public void setExecuteShards(Integer executeShards) {
		this.executeShards = executeShards;
	}

	public Integer getSparqlCount() {
		return sparqlCount;
	}

	public void setSparqlCount(Integer sparqlCount) {
		this.sparqlCount = sparqlCount;
	}

	public Integer getSparqlExecuteShards() {
		return sparqlExecuteShards;
	}

	public void setSparqlExecuteShards(Integer sparqlExecuteShards) {
		this.sparqlExecuteShards = sparqlExecuteShards;
	}

	@Override
	public void startDo(TaskMonitor tm) {
		setExecuteState(MappingState.EXECUTING);
		setExecuteStartedAt(new Date());
		setExecuteCompletedAt(null);
		setExecuteMessage(null);
		setExecuteShards(0);
		setSparqlExecuteShards(0);
		setCount(0);
		setSparqlCount(0);
		clearMessages();
	}

	@Override
	public void completeDo(TaskMonitor tm) {
		setExecuteState(MappingState.EXECUTED);
		setExecuteCompletedAt(tm.getCompletedAt());
		setExecuteMessage(null);
	}

	@Override
	public void failDo(TaskMonitor tm) {
		setExecuteState(MappingState.EXECUTION_FAILED);
        setExecuteCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
        setExecuteMessage(null);
	}
	
	@Override
	public void fail(Date completedAt, String error) {
		if (executeState == MappingState.EXECUTING || executeState == MappingState.PREPARING_EXECUTION) {
			setExecuteState(MappingState.EXECUTION_FAILED);
		}
        setExecuteCompletedAt(completedAt);
        setMessage(new NotificationMessage(MessageType.ERROR, error));
        setExecuteMessage(null);
	}

	@Override
	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = new ResponseTaskObject();
	
    	res.setState(getExecuteState().toString());
    	res.setStartedAt(getExecuteStartedAt());
    	res.setCompletedAt(getExecuteCompletedAt());
    	res.setMessages(getMessages());
	    	
    	res.setCount(getCount());
    	res.setSparqlCount(getSparqlCount());
    	res.setProgress(getD2rmlExecution());
    	
    	return res;
	}
}	   
