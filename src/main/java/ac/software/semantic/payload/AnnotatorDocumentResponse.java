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
import ac.software.semantic.model.state.MappingState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnnotatorDocumentResponse implements Response {
   
   private String id;
   private String uuid;

   private List<PathElement> onProperty;
   private String asProperty;
   private String annotator;
   
   private String variant;
   
   private String thesaurus;
   
   private MappingState executeState;
   private DatasetState publishState;
   
   private List<NotificationMessage> executeMessages;
   private List<ExecutionInfo> d2rmlExecution;

   private Integer count;
   
   private Date executeStartedAt;
   private Date executeCompletedAt;
   private Date publishStartedAt;
   private Date publishCompletedAt;
   
   private List<DataServiceParameterValue> parameters;
   
   private List<PreprocessInstruction> preprocess;
   
   private AnnotationEditGroupResponse editGroup;
   
   @JsonIgnore
   private boolean legacy;
   
   private boolean publishedFromCurrentFileSystem;
   private boolean newExecution;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private PrefixizedUri defaultTarget;

   public AnnotatorDocumentResponse() {
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

	public String getAnnotator() {
		return annotator;
	}

	public void setAnnotator(String annotator) {
		this.annotator = annotator;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public List<PathElement> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<PathElement> onProperty) {
		this.onProperty = onProperty;
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

	public String getThesaurus() {
		return thesaurus;
	}

	public void setThesaurus(String thesaurus) {
		this.thesaurus = thesaurus;
	}

	public AnnotationEditGroupResponse getEditGroup() {
		return editGroup;
	}

	public void setEditGroup(AnnotationEditGroupResponse editGroup) {
		this.editGroup = editGroup;
	}

	public List<DataServiceParameterValue> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameterValue> parameters) {
		this.parameters = parameters;
	}

	public List<PreprocessInstruction> getPreprocess() {
		return preprocess;
	}

	public void setPreprocess(List<PreprocessInstruction> preprocess) {
		this.preprocess = preprocess;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public String getVariant() {
		return variant;
	}

	public void setVariant(String variant) {
		this.variant = variant;
	}

	public PrefixizedUri getDefaultTarget() {
		return defaultTarget;
	}

	public void setDefaultTarget(PrefixizedUri defaultTarget) {
		this.defaultTarget = defaultTarget;
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

	public boolean isLegacy() {
		return legacy;
	}

	public void setLegacy(boolean legacy) {
		this.legacy = legacy;
	}

	public List<ExecutionInfo> getD2rmlExecution() {
		return d2rmlExecution;
	}

	public void setD2rmlExecution(List<ExecutionInfo> d2rmlExecution) {
		this.d2rmlExecution = d2rmlExecution;
	}

//	public String getExecuteUuid() {
//		return executeUuid;
//	}
//
//	public void setExecuteUuid(String executionUuid) {
//		this.executeUuid = executionUuid;
//	}

}