package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.PreprocessInstruction;


public class AnnotatorDocumentResponse {
   
   private String id;
   private String uuid;

   private List<String> onProperty;
   private String asProperty;
   private String annotator;
   
   private String variant;
   
   private String thesaurus;
   
   private MappingState executeState;
   private DatasetState publishState;
   
   private int count;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date executeStartedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date executeCompletedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)   
   private Date publishStartedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date publishCompletedAt;
   
   private List<DataServiceParameterValue> parameters;
   
   private List<PreprocessInstruction> preprocess;
   
   private AnnotationEditGroupResponse editGroup;

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

	public List<String> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<String> onProperty) {
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

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getVariant() {
		return variant;
	}

	public void setVariant(String variant) {
		this.variant = variant;
	}

//	public String getExecuteUuid() {
//		return executeUuid;
//	}
//
//	public void setExecuteUuid(String executionUuid) {
//		this.executeUuid = executionUuid;
//	}

}