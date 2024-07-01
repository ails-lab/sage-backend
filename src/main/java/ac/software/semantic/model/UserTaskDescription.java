package ac.software.semantic.model;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.TaskType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserTaskDescription {
	
	private ObjectId fileSystemConfigurationId;
	private ObjectId tripleStoreConfigurationId;
	private ObjectId elasticConfigurationId;
	
	private ObjectId datasetId;
	private ObjectId mappingId;
	private ObjectId mappingInstanceId;
	
	private ObjectId annotatorId;
	private ObjectId embedderId;
	
	private ObjectId indexId;
	
	private ObjectId pagedAnnotationValidationId;
	private ObjectId filterAnnotationValidationId;
	
	private TaskType type;
	
	private Integer group;
	
	public UserTaskDescription() {
		
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}

	public ObjectId getTripleStoreConfigurationId() {
		return tripleStoreConfigurationId;
	}

	public void setTripleStoreConfigurationId(ObjectId tripleStoreConfigurationId) {
		this.tripleStoreConfigurationId = tripleStoreConfigurationId;
	}

	public ObjectId getElasticConfigurationId() {
		return elasticConfigurationId;
	}

	public void setElasticConfigurationId(ObjectId elasticConfigurationId) {
		this.elasticConfigurationId = elasticConfigurationId;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public ObjectId getMappingId() {
		return mappingId;
	}

	public void setMappingId(ObjectId mappingId) {
		this.mappingId = mappingId;
	}

	public ObjectId getMappingInstanceId() {
		return mappingInstanceId;
	}

	public void setMappingInstanceId(ObjectId mappingInstanceId) {
		this.mappingInstanceId = mappingInstanceId;
	}

	public ObjectId getAnnotatorId() {
		return annotatorId;
	}

	public void setAnnotatorId(ObjectId annotatorId) {
		this.annotatorId = annotatorId;
	}

	public ObjectId getEmbedderId() {
		return embedderId;
	}

	public void setEmbedderId(ObjectId embedderId) {
		this.embedderId = embedderId;
	}

	public ObjectId getIndexId() {
		return indexId;
	}

	public void setIndexId(ObjectId indexId) {
		this.indexId = indexId;
	}

	public ObjectId getPagedAnnotationValidationId() {
		return pagedAnnotationValidationId;
	}

	public void setPagedAnnotationValidationId(ObjectId pagedAnnotationValidationId) {
		this.pagedAnnotationValidationId = pagedAnnotationValidationId;
	}

	public ObjectId getFilterAnnotationValidationId() {
		return filterAnnotationValidationId;
	}

	public void setFilterAnnotationValidationId(ObjectId filterAnnotationValidationId) {
		this.filterAnnotationValidationId = filterAnnotationValidationId;
	}

	public TaskType getType() {
		return type;
	}

	public void setType(TaskType type) {
		this.type = type;
	}

	public Integer getGroup() {
		return group;
	}

	public void setGroup(Integer group) {
		this.group = group;
	}

}
