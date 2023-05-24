package ac.software.semantic.model;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.PagedAnnotationValidationState;

@Document(collection = "PagedAnnotationValidation")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PagedAnnotationValidation extends MappingExecutePublishDocument<MappingPublishState> implements AnnotationValidation {
	@Id
	private ObjectId id;

	private String name;
	
	@JsonIgnore
	private ObjectId userId;

	private ObjectId annotationEditGroupId;
	
	private String datasetUuid;
	private List<String> onProperty;
	private String asProperty;
	private List<String> annotatorDocumentUuid;
	
	private ObjectId databaseId;

	private int pageSize;
	private int annotatedPagesCount;
	private int nonAnnotatedPagesCount;
	
	private int annotationsCount;

	private boolean isComplete; // false = not active, true = active // deprecated
	
	private String uuid;
	
	private PagedAnnotationValidationState lifecycle;
	private Date lifecycleStartedAt;
	private Date resumingStartedAt;
	private Date lifecycleCompletedAt;
	
	private String mode;
	
	private Date updatedAt;
	
	private boolean active;
	
	// for correctness the PagedAnnotationValidation should correspond to a virtuosoConfiguration !!! (and the annotation edits ???)

	public PagedAnnotationValidation() {
		super();
	}

	public ObjectId getId() {
		return id;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public ObjectId getAnnotationEditGroupId() {
		return annotationEditGroupId;
	}

	public boolean isComplete() {
		return isComplete;
	}

	public void setComplete(boolean complete) {
		isComplete = complete;
	}

	public void setAnnotationEditGroupId(ObjectId annotationEditGroupId) {
		this.annotationEditGroupId = annotationEditGroupId;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
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
	
//	public String getOnPropertyAsString() {
//		return AnnotationEditGroup.onPropertyListAsString(this.getOnProperty());
//	}

	public int getAnnotationsCount() {
		return annotationsCount;
	}

	public void setAnnotationsCount(int annotationsCount) {
		this.annotationsCount = annotationsCount;
	}

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

	public PagedAnnotationValidationState getLifecycle() {
		return lifecycle;
	}

	public void setLifecycle(PagedAnnotationValidationState lifecycle) {
		this.lifecycle = lifecycle;
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

	public Date getResumingStartedAt() {
		return resumingStartedAt;
	}

	public void setResumingStartedAt(Date resumingStartedAt) {
		this.resumingStartedAt = resumingStartedAt;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}



}
