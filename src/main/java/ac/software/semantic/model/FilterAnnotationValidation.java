package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.FilterValidationType;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;

@Document(collection = "FilterAnnotationValidation")
public class FilterAnnotationValidation extends MappingExecutePublishDocument<MappingPublishState> implements AnnotationValidation {
	@Id
	private ObjectId id;

	private String name;

	private List<AnnotationEditFilter> filters;
	
	@JsonIgnore
	private ObjectId userId;

	private ObjectId annotationEditGroupId;
	
	private String datasetUuid;
	private List<String> onProperty;
	private String asProperty;
	private List<String> annotatorDocumentUuid;
	
	private ObjectId databaseId;

	private String uuid;
	
	public FilterAnnotationValidation() {
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
 
}
