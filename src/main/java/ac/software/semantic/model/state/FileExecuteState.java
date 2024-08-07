package ac.software.semantic.model.state;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.payload.response.ResponseTaskObject;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileExecuteState extends ExecuteState {

	private String fileName;
	private List<String> contentFileNames;
	
	public FileExecuteState() {
		super();
	}
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public List<String> getContentFileNames() {
		return contentFileNames;
	}

	public void setContentFileNames(List<String> contentFileNames) {
		this.contentFileNames = contentFileNames;
	}

	@Override
	public void startDo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void completeDo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void failDo(TaskMonitor tm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Date completedAt, String error) {
		// TODO Auto-generated method stub
		
	}
	   
	@Override
	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = new ResponseTaskObject();
	
    	res.setState(getExecuteState().toString());
    	res.setStartedAt(getExecuteStartedAt());
    	res.setCompletedAt(getExecuteCompletedAt());
    	res.setMessages(getMessages());
	    	
    	res.setFileName(getFileName());
    	
    	return res;
	}
}	   
