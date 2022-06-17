package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.MappingState;

public class PagedAnnotationValidationResponse {

	private String id;
	
	private String uuid;
	
	private String name;

	private String annotationEditGroupId;
	
	private String datasetUuid;
	private List<String> onProperty;
	private String asProperty;

	private int annotatedPagesCount;
	private int nonAnnotatedPagesCount;

	private boolean isComplete;
	
	private MappingState executeState;
	private DatasetState publishState;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Date executeStartedAt;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Date executeCompletedAt;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Date publishStartedAt;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Date publishCompletedAt;

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

	public List<String> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<String> onProperty) {
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

}
