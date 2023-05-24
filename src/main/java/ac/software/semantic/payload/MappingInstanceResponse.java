package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.MappingState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MappingInstanceResponse {

	   private String id;
	   
	   private MappingState executeState;
	   private DatasetState publishState;
	   
	   private List<NotificationMessage> executeMessages;
	   private List<ExecutionInfo> d2rmlExecution;
	   
	   private Date executeStartedAt;
	   private Date executeCompletedAt;

	   private Date publishStartedAt;
	   private Date publishCompletedAt;
	   
	   private List<ParameterBinding> binding;
	   
	   private Integer count;
	   private Integer sparqlCount;
	   
	   private boolean publishedFromCurrentFileSystem;
	   private boolean newExecution;
	   
	   private List<String> dataFiles;
	   
	   @JsonIgnore
	   private boolean legacy;
	   
//	   private String uuid;
	   
	   public MappingInstanceResponse() {  
	   }
	   
	   public boolean hasBinding() {
		   return binding != null && binding.size() > 0; 
	   }
	   
		public Date getExecuteStartedAt() {
			return executeStartedAt;
		}
	
		public void setExecuteStartedAt(Date startedAt) {
			this.executeStartedAt = startedAt;
		}
	
		public Date getExecuteCompletedAt() {
			return executeCompletedAt;
		}
	
		public void setExecuteCompletedAt(Date completedAt) {
			this.executeCompletedAt = completedAt;
		}
	
		public List<ParameterBinding> getBinding() {
			return binding;
		}

		public void setBinding(List<ParameterBinding> binding) {
			this.binding = binding;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public MappingState getExecuteState() {
			return executeState;
		}

		public void setExecuteState(MappingState executeState) {
			this.executeState = executeState;
		}

		public DatasetState getPublishState() {
			return publishState;
		}

		public void setPublishState(DatasetState publishState) {
			this.publishState = publishState;
		}

		public Date getPublishStartedAt() {
			return publishStartedAt;
		}

		public void setPublishStartedAt(Date publishStartedAt) {
			this.publishStartedAt = publishStartedAt;
		}

		public Date getPublishCompletedAt() {
			return publishCompletedAt;
		}

		public void setPublishCompletedAt(Date publishCompletedAt) {
			this.publishCompletedAt = publishCompletedAt;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}

		public boolean getNewExecution() {
			return newExecution;
		}

		public void setNewExecution(boolean newExecution) {
			this.newExecution = newExecution;
		}

		public boolean isPublishedFromCurrentFileSystem() {
			return publishedFromCurrentFileSystem;
		}

		public void setPublishedFromCurrentFileSystem(boolean publishedFromCurrentFileSystem) {
			this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
		}

		public boolean isLegacy() {
			return legacy;
		}

		public void setLegacy(boolean legacy) {
			this.legacy = legacy;
		}

		public Integer getSparqlCount() {
			return sparqlCount;
		}

		public void setSparqlCount(Integer sparqlCount) {
			this.sparqlCount = sparqlCount;
		}

		public List<NotificationMessage> getExecuteMessages() {
			return executeMessages;
		}

		public void setExecuteMessages(List<NotificationMessage> executeMessages) {
			this.executeMessages = executeMessages;
		}

		public List<String> getDataFiles() {
			return dataFiles;
		}

		public void setDataFiles(List<String> dataFiles) {
			this.dataFiles = dataFiles;
		}

		public List<ExecutionInfo> getD2rmlExecution() {
			return d2rmlExecution;
		}

		public void setD2rmlExecution(List<ExecutionInfo> d2rmlExecution) {
			this.d2rmlExecution = d2rmlExecution;
		}


}
