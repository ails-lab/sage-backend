package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

@Document(collection = "FilterAnnotationValidation")
public class FilterAnnotationValidation implements AnnotationValidation {
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

	private String uuid;
	
	private List<ExecuteState> execute;
	private List<PublishState> publish;

	public FilterAnnotationValidation() {
       execute = new ArrayList<>();
       publish = new ArrayList<>();

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
	
	public String getOnPropertyAsString() {
		return AnnotationEditGroup.onPropertyListAsString(this.getOnProperty());
	}

	public List<String> getAnnotatorDocumentUuid() {
		return annotatorDocumentUuid;
	}

	public void setAnnotatorDocumentUuid(List<String> annotatorDocumentUuid) {
		this.annotatorDocumentUuid = annotatorDocumentUuid;
	}
	
	public List<PublishState> getPublish() {
		return publish;
	}

	public void setPublish(List<PublishState> publish) {
		this.publish = publish;
	}
	
	public PublishState getPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {
			for (PublishState s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			publish = new ArrayList<>();
		}
		
		PublishState s = new PublishState();
		s.setPublishState(DatasetState.UNPUBLISHED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		publish.add(s);
		
		return s;	
	}
	
	public PublishState checkPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {
			for (PublishState s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}	

	public List<ExecuteState> getExecute() {
		return execute;
	}

	public void setExecute(List<ExecuteState> execute) {
		this.execute = execute;
	}	
	
	public ExecuteState getExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {
			for (ExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			execute = new ArrayList<>();
		}
		
		ExecuteState s = new ExecuteState();
		s.setExecuteState(MappingState.NOT_EXECUTED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		execute.add(s);
		
		return s;
	}

	public ExecuteState checkExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {		
			for (ExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
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
 
}
