package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PagedAnnotationValidationState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PagedAnnotationValidationResponse implements Response {

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
	
	private MappingState executeState;
	private DatasetState publishState;
	
	private PagedAnnotationValidationState lifecycleState;
	
	private List<NotificationMessage> executeMessages;

	private String mode;
	
	private Date lifecycleStartedAt; 
	private Date lifecycleCompletedAt;
	
	private Date executeStartedAt;
	private Date executeCompletedAt;
	private Integer executeCount;

	private Date publishStartedAt;
	private Date publishCompletedAt;
	
	private boolean publishedFromCurrentFileSystem;
	private boolean newExecution;
	
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

	public Date getExecuteStartedAt() {
		return executeStartedAt;
	}

	public void setExecuteStartedAt(Date executeStartedAt) {
		this.executeStartedAt = executeStartedAt;
	}

	public Date getExecuteCompletedAt() {
		return executeCompletedAt;
	}

	public void setExecuteCompletedAt(Date executeCompletedAt) {
		this.executeCompletedAt = executeCompletedAt;
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

	public PagedAnnotationValidationState getLifecycleState() {
		return lifecycleState;
	}

	public void setLifecycleState(PagedAnnotationValidationState lifecycleState) {
		this.lifecycleState = lifecycleState;
	}

	public Date getLifecycleStartedAt() {
		return lifecycleStartedAt;
	}

	public void setLifecycleStartedAt(Date lifecycleStartedAt) {
		this.lifecycleStartedAt = lifecycleStartedAt;
	}

	public Date getLifecycleCompletedAt() {
		return lifecycleCompletedAt;
	}

	public void setLifecycleCompletedAt(Date lifecycleCompletedAt) {
		this.lifecycleCompletedAt = lifecycleCompletedAt;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public List<NotificationMessage> getExecuteMessages() {
		return executeMessages;
	}

	public void setExecuteMessages(List<NotificationMessage> executeMessages) {
		this.executeMessages = executeMessages;
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

	public Integer getExecuteCount() {
		return executeCount;
	}

	public void setExecuteCount(Integer executeCount) {
		this.executeCount = executeCount;
	}

}
