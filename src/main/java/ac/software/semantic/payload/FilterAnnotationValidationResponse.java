package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.AnnotationEditFilter;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.FilterValidationType;
import ac.software.semantic.model.MappingState;

public class FilterAnnotationValidationResponse {
	private String id;

	private String name;
	
	private List<AnnotationEditFilter> filters;
	
	private ObjectId annotationEditGroupId;
	
	private String datasetUuid;
	private List<String> onProperty;
	private String asProperty;
	private List<String> annotatorDocumentUuid;

	private String uuid;
	
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

	public FilterAnnotationValidationResponse() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ObjectId getAnnotationEditGroupId() {
		return annotationEditGroupId;
	}

	public void setAnnotationEditGroupId(ObjectId annotationEditGroupId) {
		this.annotationEditGroupId = annotationEditGroupId;
	}


	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
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
//	
//	public String getOnPropertyAsString() {
//		return AnnotationEditGroup.onPropertyListAsString(this.getOnProperty());
//	}

	public List<String> getAnnotatorDocumentUuid() {
		return annotatorDocumentUuid;
	}

	public void setAnnotatorDocumentUuid(List<String> annotatorDocumentUuid) {
		this.annotatorDocumentUuid = annotatorDocumentUuid;
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

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<AnnotationEditFilter> getFilters() {
		return filters;
	}

	public void setFilters(List<AnnotationEditFilter> filters) {
		this.filters = filters;
	}

}
