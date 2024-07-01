package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.ContentDocument;
import ac.software.semantic.model.constants.type.PrototypeType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "PrototypeDocuments")
public class PrototypeDocument implements ContentDocument, DatedDocument {
	
	@Id
	private ObjectId id;

	private ObjectId databaseId;
	   
	private ObjectId userId;
	
	private ObjectId datasetId;
	
	private String datasetUuid;
	   
	private String name;
	private String description;
	   
	private String url;
	   
	private String uuid;
	   
	private PrototypeType type;

	private Date createdAt;
	private Date updatedAt;
	
	private String content;
	
	private List<DataServiceParameter> parameters;
	
	// d2rml mapping dependencies
	private List<DependencyBinding> dependencies;
	
	// annotator fields
	private List<DataServiceParameter> fields;
	
	private PrototypeDocument() {
		super();
	}
	
	public PrototypeDocument(Dataset dataset) {
		this();
		
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = dataset.getDatabaseId();
		   
		this.datasetId = dataset.getId();
		this.datasetUuid = dataset.getUuid();
	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
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

	public PrototypeType getType() {
		return type;
	}

	public void setType(PrototypeType type) {
		this.type = type;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public List<DataServiceParameter> getParameters() {
		return parameters;
	}
	
//	public List<String> getParameterNames() {
//		if (parameters != null) {
//			List<String> res = new ArrayList<>();
//			for (DataServiceParameter dsp : parameters) {
//				res.add(dsp.getName());
//			}
//			return res;
//		}
//		
//		return null;
//	}

	public void setParameters(List<DataServiceParameter> parameters) {
		this.parameters = parameters;
	}

	public List<DependencyBinding> getDependencies() {
		return dependencies;
	}

	public void setDependencies(List<DependencyBinding> dependencies) {
		this.dependencies = dependencies;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<DataServiceParameter> getFields() {
		return fields;
	}

	public void setFields(List<DataServiceParameter> fields) {
		this.fields = fields;
	}
	
}
