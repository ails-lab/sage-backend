package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.constants.state.ThesaurusLoadState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResponseTaskObject {

	private String state;
	
	private List<NotificationMessage> messages;
	private List<ExecutionInfo> progress;
	
	private Date startedAt;
	private Date completedAt;
	
	private Boolean conforms;
	private String report;
	
	private Integer count;
	private Integer sparqlCount;
	
	private String fileName;
	
	private String tripleStoreName;
	
	private List<String> uriSpaces;
	
	private List<ResponseTaskObject> groups;
	
	private Integer group;
	
	private String stateMessage;

//	// should be removed once model mapped is updated
//	public static ResponseTaskObject create(PublishState<?> ps) {
//		ResponseTaskObject res = new ResponseTaskObject();
//		
//    	if (ps != null) {    	
//	    	res.setState(ps.getPublishState().toString());
//    		res.setStartedAt(ps.getPublishStartedAt());
//    		res.setCompletedAt(ps.getPublishCompletedAt());
//    		res.setMessages(ps.getMessages());
//    	} else {
//    		res.setState(DatasetState.UNPUBLISHED.toString());
//    	} 
//    	
//    	return res;
//	}
	
	public static ResponseTaskObject create(ThesaurusLoadState lts) {
		ResponseTaskObject res = new ResponseTaskObject();
		
		if (lts  != null) {
			res.setState(lts.toString());
		} else {
			res.setState(ThesaurusLoadState.NOT_LOADED.toString());
		}
    	
		return res;
	}


	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public List<NotificationMessage> getMessages() {
		return messages;
	}

	public void setMessages(List<NotificationMessage> messages) {
		this.messages = messages;
	}

	public List<ExecutionInfo> getProgress() {
		return progress;
	}

	public void setProgress(List<ExecutionInfo> progress) {
		this.progress = progress;
	}

	public Date getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}
	
	public void addMap(ExecutionInfo map) {
		if (progress == null) {
			progress = new ArrayList<>();
		}
		progress.add(map);
	}

	public Date getCompletedAt() {
		return completedAt;
	}

	public void setCompletedAt(Date completedAt) {
		this.completedAt = completedAt;
	}

	public Boolean getConforms() {
		return conforms;
	}

	public void setConforms(Boolean conforms) {
		this.conforms = conforms;
	}

	public String getReport() {
		return report;
	}

	public void setReport(String report) {
		this.report = report;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Integer getSparqlCount() {
		return sparqlCount;
	}

	public void setSparqlCount(Integer sparqlCount) {
		this.sparqlCount = sparqlCount;
	}
	
	public void addMessage(NotificationMessage message) {
		if (messages == null) {
			this.messages = new ArrayList<>();
		}
		
		this.messages.add(message);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}


	public String getTripleStoreName() {
		return tripleStoreName;
	}


	public void setTripleStoreName(String tripleStoreName) {
		this.tripleStoreName = tripleStoreName;
	}


	public List<String> getUriSpaces() {
		return uriSpaces;
	}


	public void setUriSpaces(List<String> uriSpaces) {
		this.uriSpaces = uriSpaces;
	}


	public List<ResponseTaskObject> getGroups() {
		return groups;
	}


	public void setGroups(List<ResponseTaskObject> groups) {
		this.groups = groups;
	}


	public Integer getGroup() {
		return group;
	}


	public void setGroup(Integer group) {
		this.group = group;
	}


	public String getStateMessage() {
		return stateMessage;
	}


	public void setStateMessage(String stateMessage) {
		this.stateMessage = stateMessage;
	}

}
