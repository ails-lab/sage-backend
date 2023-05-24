package ac.software.semantic.model.state;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.constants.MessageType;
import ac.software.semantic.service.GenericMonitor;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MappingExecuteState extends ExecuteState {

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
		setExecuteShards(0);
		setSparqlExecuteShards(0);
		setCount(0);
		setSparqlCount(0);
		clearMessages();
	}

	@Override
	public void startUndo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void failUndo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void completeDo(TaskMonitor tm) {
		setExecuteState(MappingState.EXECUTED);
		setExecuteCompletedAt(tm.getCompletedAt());

		
	}

	@Override
	public void failDo(TaskMonitor tm) {
		setExecuteState(MappingState.EXECUTION_FAILED);
        setExecuteCompletedAt(tm.getCompletedAt());
        setMessage(tm.getFailureMessage());
	}
	
	@Override
	public void fail(Date completedAt, String error) {
		if (executeState == MappingState.EXECUTING || executeState == MappingState.PREPARING_EXECUTION) {
			setExecuteState(MappingState.EXECUTION_FAILED);
		}
        setExecuteCompletedAt(completedAt);
        setMessage(new NotificationMessage(MessageType.ERROR, error));
	}

}	   
