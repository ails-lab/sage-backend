package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.PreprocessInstruction;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.state.MappingState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class EmbedderDocumentResponse implements Response {
   
   private String id;
   private String uuid;

   private ClassIndexElement element;
   private String embedder;
   
   private String variant;
   
	private String onClass;
	
   private MappingState executeState;
   private DatasetState publishState;
   
   private List<NotificationMessage> executeMessages;
   private List<ExecutionInfo> d2rmlExecution;

   private Integer count;
   
   private Date executeStartedAt;
   private Date executeCompletedAt;
   private Date publishStartedAt;
   private Date publishCompletedAt;
   
   private boolean publishedFromCurrentFileSystem;
   private boolean newExecution;
   
   public EmbedderDocumentResponse() {
	   executeState = MappingState.NOT_EXECUTED;
	   publishState = DatasetState.UNPUBLISHED;
   }
   
   public String getId() {
       return id;
   }
   
	public void setId(String id) {
		this.id = id;
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

	public String getEmbedder() {
		return embedder;
	}

	public void setEmbedder(String embedder) {
		this.embedder = embedder;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
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
		if (publishState == null) {
			this.publishState = DatasetState.UNPUBLISHED;
		} else {
			this.publishState = publishState;
		}
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public List<NotificationMessage> getExecuteMessages() {
		return executeMessages;
	}

	public void setExecuteMessages(List<NotificationMessage> messages) {
		this.executeMessages = messages;
	}

	public boolean isPublishedFromCurrentFileSystem() {
		return publishedFromCurrentFileSystem;
	}

	public void setPublishedFromCurrentFileSystem(boolean publishedFromCurrentFileSystem) {
		this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
	}

	public boolean isNewExecution() {
		return newExecution;
	}

	public void setNewExecution(boolean newExecution) {
		this.newExecution = newExecution;
	}

	public ClassIndexElement getElement() {
		return element;
	}

	public void setElement(ClassIndexElement element) {
		this.element = element;
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

	public String getVariant() {
		return variant;
	}

	public void setVariant(String variant) {
		this.variant = variant;
	}

	public List<ExecutionInfo> getD2rmlExecution() {
		return d2rmlExecution;
	}

	public void setD2rmlExecution(List<ExecutionInfo> d2rmlExecution) {
		this.d2rmlExecution = d2rmlExecution;
	}

}