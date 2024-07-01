package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.PathElement;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PagedAnnotationValidationResponse implements Response, ExecutePublishResponse, LifecycleResponse {

	private String id;
	
	private String uuid;
	
	private String name;

	private String annotationEditGroupId;
	
	private String datasetUuid;
	private List<PathElement> onProperty;
	private String asProperty;

	private int annotatedPagesCount;
	private int nonAnnotatedPagesCount;

	private boolean isComplete;
	
	private ResponseTaskObject executeState;
	private ResponseTaskObject publishState;
	private ResponseTaskObject lifecycleState;

	private String mode;
	
	private Boolean publishedFromCurrentFileSystem;
	private Boolean newExecution;
	
	private Date createdAt;
	private Date updatedAt;
	
	private List<VocabularyContextResponse> systemVocabularies;
	private List<VocabularyContextResponse> userVocabularies;
	
	// custom for progress view. check what to do with them
	private String propertyName;
	private List<PathElement> propertyPath;
	private ProgressResponse progress;
	private boolean locked;
	private boolean active;
	
	private String tag;
	
	public PagedAnnotationValidationResponse() {

	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getAnnotationEditGroupId() {
		return annotationEditGroupId;
	}

	public void setAnnotationEditGroupId(String annotationEditGroupId) {
		this.annotationEditGroupId = annotationEditGroupId;
	}

	public boolean isComplete() {
		return isComplete;
	}

	public void setComplete(boolean complete) {
		isComplete = complete;
	}

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public List<PathElement> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<PathElement> onProperty) {
		this.onProperty = onProperty;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public int getAnnotatedPagesCount() {
		return annotatedPagesCount;
	}

	public void setAnnotatedPagesCount(int annotatedPagesCount) {
		this.annotatedPagesCount = annotatedPagesCount;
	}

	public int getNonAnnotatedPagesCount() {
		return nonAnnotatedPagesCount;
	}

	public void setNonAnnotatedPagesCount(int nonAnnotatedPagesCount) {
		this.nonAnnotatedPagesCount = nonAnnotatedPagesCount;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public Boolean isPublishedFromCurrentFileSystem() {
		return publishedFromCurrentFileSystem;
	}

	public void setPublishedFromCurrentFileSystem(Boolean publishedFromCurrentFileSystem) {
		this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
	}

	public Boolean isNewExecution() {
		return newExecution;
	}

	public void setNewExecution(Boolean newExecution) {
		this.newExecution = newExecution;
	}

	public ResponseTaskObject getExecuteState() {
		return executeState;
	}

	public void setExecuteState(ResponseTaskObject executeState) {
		this.executeState = executeState;
	}

	public ResponseTaskObject getPublishState() {
		return publishState;
	}

	public void setPublishState(ResponseTaskObject publishState) {
		this.publishState = publishState;
	}

	public ResponseTaskObject getLifecycleState() {
		return lifecycleState;
	}

	public void setLifecycleState(ResponseTaskObject lifecycleState) {
		this.lifecycleState = lifecycleState;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public List<VocabularyContextResponse> getSystemVocabularies() {
		return systemVocabularies;
	}

	public void setSystemVocabularies(List<VocabularyContextResponse> systemVocabularies) {
		this.systemVocabularies = systemVocabularies;
	}

//	public void setSystemVocabulariesFromObjectId(List<ObjectId> systemVocabularies) {
//		if (systemVocabularies != null) {
//			this.systemVocabularies = new ArrayList<>();
//			for (ObjectId s : systemVocabularies) {
//				this.systemVocabularies.add(s.toString());
//			}
//		} else {
//			this.systemVocabularies = null;
//		}
//	}

	public List<VocabularyContextResponse> getUserVocabularies() {
		return userVocabularies;
	}

	public void setUserVocabularies(List<VocabularyContextResponse> userVocabularies) {
		this.userVocabularies = userVocabularies;
	}

	public ProgressResponse getProgress() {
		return progress;
	}

	public void setProgress(ProgressResponse progress) {
		this.progress = progress;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}

	public List<PathElement> getPropertyPath() {
		return propertyPath;
	}

	public void setPropertyPath(List<PathElement> propertyPath) {
		this.propertyPath = propertyPath;
	}

	public boolean isLocked() {
		return locked;
	}

	public void setLocked(boolean locked) {
		this.locked = locked;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

//	public void setUserVocabulariesFromObjectId(List<ObjectId> userVocabularies) {
//		if (userVocabularies != null) {
//			this.userVocabularies = new ArrayList<>();
//			for (ObjectId s : userVocabularies) {
//				this.userVocabularies.add(s.toString());
//			}
//		} else {
//			this.userVocabularies = null;
//		}
//	}
}
