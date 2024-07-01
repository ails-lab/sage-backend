package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.AnnotationEditFilter;
import ac.software.semantic.model.PathElement;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FilterAnnotationValidationResponse implements Response, ExecutePublishResponse {
	private String id;

	private String name;
	
	private List<AnnotationEditFilter> filters;
	
	private ObjectId annotationEditGroupId;
	
	private String datasetUuid;
	private List<PathElement> onProperty;
	private String asProperty;
	private List<String> annotatorDocumentUuid;

	private String uuid;
	
	private ResponseTaskObject executeState;
	private ResponseTaskObject publishState;

	private Boolean publishedFromCurrentFileSystem;
	private Boolean newExecution;
	
	private Date createdAt;
	private Date updatedAt;
	
	private String tag;

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

	public List<PathElement> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<PathElement> onProperty) {
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

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}


}
