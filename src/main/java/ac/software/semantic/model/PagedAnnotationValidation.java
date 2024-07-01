package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.DatasetContained;
import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.base.StartableDocument;
import ac.software.semantic.model.constants.state.PagedAnnotationValidationState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.payload.response.ResponseTaskObject;
import ac.software.semantic.vocs.SEMRVocabulary;

@Document(collection = "PagedAnnotationValidation")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PagedAnnotationValidation extends MappingExecutePublishDocument<MappingExecuteState, MappingPublishState> implements AnnotationValidation, DatasetContained, StartableDocument, DatedDocument {
	
	@Id
	private ObjectId id;

	@JsonIgnore
	private ObjectId databaseId;
	
	private String name;
	
	@JsonIgnore
	private ObjectId userId;

	private ObjectId annotationEditGroupId;
	
	private ObjectId datasetId;
	
	private String datasetUuid;
	private List<String> onProperty;
	private String asProperty;
//	private List<String> annotatorDocumentUuid;
	
	private String tag;
	
//	private boolean customTarget;
	
	private List<List<String>> controlProperties;
	
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
	
	private Date createdAt;
	private Date updatedAt;
	
	private boolean active;
	
	private List<ObjectId> systemVocabularies;
	private List<ObjectId> userVocabularies;
	
	// for correctness the PagedAnnotationValidation should correspond to a virtuosoConfiguration !!! (and the annotation edits ???)

	private PagedAnnotationValidation() {
		super();
	}
	
	public PagedAnnotationValidation(Dataset dataset) {
		this();
	
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = dataset.getDatabaseId();
		   
		this.datasetId = dataset.getId();
		this.datasetUuid = dataset.getUuid();
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

	@Override
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
	
	public String getTripleStoreGraph(SEMRVocabulary resourceVocabulary, boolean separate) {
		if (separate) {
			return resourceVocabulary.getAnnotationValidatorAsResource(this).toString();
		}
		
		if (asProperty != null) {
			return asProperty;
		} else {
			return resourceVocabulary.getDatasetAnnotationsAsResource(datasetUuid).toString();
		}
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

//	public List<String> getAnnotatorDocumentUuid() {
//		return annotatorDocumentUuid;
//	}
//
//	public void setAnnotatorDocumentUuid(List<String> annotatorDocumentUuid) {
//		this.annotatorDocumentUuid = annotatorDocumentUuid;
//	}

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

	@Override
	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = new ResponseTaskObject();

		res.setState(getLifecycle().toString());
		if (getLifecycle() == PagedAnnotationValidationState.RESUMING || getLifecycle() == PagedAnnotationValidationState.RESUMING_FAILED) {
			res.setStartedAt(getResumingStartedAt());
		} else {
			res.setStartedAt(getLifecycleStartedAt());
		}
		res.setCompletedAt(getLifecycleCompletedAt());
		
		return res;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public List<ObjectId> getSystemVocabularies() {
		return systemVocabularies;
	}

	public void setSystemVocabularies(List<ObjectId> systemVocabularies) {
		this.systemVocabularies = systemVocabularies;
	}

	public void setSystemVocabulariesFromString(List<String> systemVocabularies) {
		if (systemVocabularies != null) {
			this.systemVocabularies = new ArrayList<>();
			for (String s : systemVocabularies) {
				this.systemVocabularies.add(new ObjectId(s));
			}
		} else {
			this.systemVocabularies = null;
		}
	}

	public List<ObjectId> getUserVocabularies() {
		return userVocabularies;
	}

	public void setUserVocabularies(List<ObjectId> userVocabularies) {
		this.userVocabularies = userVocabularies;
	}

	public void setUserVocabulariesFromString(List<String> userVocabularies) {
		if (userVocabularies != null) {
			this.userVocabularies = new ArrayList<>();
			for (String s : userVocabularies) {
				this.userVocabularies.add(new ObjectId(s));
			}
		} else {
			this.userVocabularies = null;
		}
	}

	public List<List<String>> getControlProperties() {
		return controlProperties;
	}

	public void setControlProperties(List<List<String>> controlProperties) {
		this.controlProperties = controlProperties;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

//	public boolean isCustomTarget() {
//		return customTarget;
//	}
//
//	public void setCustomTarget(boolean customTarget) {
//		this.customTarget = customTarget;
//	}

}
