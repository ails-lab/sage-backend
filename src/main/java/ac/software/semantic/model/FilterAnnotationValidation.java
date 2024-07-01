package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.base.DatasetContained;
import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.constants.type.FilterValidationType;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.vocs.SEMRVocabulary;

@Document(collection = "FilterAnnotationValidation")
public class FilterAnnotationValidation extends MappingExecutePublishDocument<MappingExecuteState, MappingPublishState> implements AnnotationValidation, DatasetContained, DatedDocument {
	
	@Id
	private ObjectId id;

	private String uuid;
	
	@JsonIgnore
	private ObjectId databaseId;

	private ObjectId datasetId;
	private String datasetUuid;

	private String name;

	private List<AnnotationEditFilter> filters;
	
	@JsonIgnore
	private ObjectId userId;

	private ObjectId annotationEditGroupId;
	
	private List<String> onProperty;
	private String asProperty;
	private List<String> annotatorDocumentUuid;
	
	private String tag;
	
	private Date createdAt;
	private Date updatedAt;
	
	private FilterAnnotationValidation() {
	}

	public FilterAnnotationValidation(Dataset dataset) {
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

	public void setAnnotationEditGroupId(ObjectId annotationEditGroupId) {
		this.annotationEditGroupId = annotationEditGroupId;
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

	public List<AnnotationEditFilter> getDeleteFilters() {
		List<AnnotationEditFilter> res = new ArrayList<>();
		for (AnnotationEditFilter f : filters) {
			if (f.getAction() == FilterValidationType.DELETE) {
				res.add(f);
			}
		}
		return res;
	}
	
	public List<AnnotationEditFilter> getReplaceFilters() {
		List<AnnotationEditFilter> res = new ArrayList<>();
		for (AnnotationEditFilter f : filters) {
			if (f.getAction() == FilterValidationType.REPLACE) {
				res.add(f);
			}
		}
		return res;
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
