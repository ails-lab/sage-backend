package ac.software.semantic.model;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "DataServices")
public class DataService {

	public enum DataServiceType {
		ANNOTATOR, 
		EMBEDDER
	}
	
	private ObjectId id;
	
	private String identifier;
	private String title;
	private List<DataServiceParameter> parameters;
	private List<String> asProperties;
	private List<DataServiceVariant> variants;
	
	private List<ObjectId> databaseId;
	private String description;

	private DataServiceType type;
	
	private String uri;
	
	public DataService(String identifier, String title, List<DataServiceParameter> parameters, List<String> asProperties, List<DataServiceVariant> variants) {
		this.identifier = identifier;
		this.title = title;
		this.parameters = parameters;
		this.asProperties = asProperties;
		this.variants = variants;
	}
	
	public String getIdentifier() {
		return identifier;
	}
	
	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	
	public String getTitle() {
		return title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<String> getAsProperties() {
		return asProperties;
	}

	public void setAsProperties(List<String> asProperties) {
		this.asProperties = asProperties;
	}

	public List<DataServiceVariant> getVariants() {
		return variants;
	}

	public void setVariants(List<DataServiceVariant> variants) {
		this.variants = variants;
	}

	public DataServiceType getType() {
		return type;
	}

	public void setType(DataServiceType type) {
		this.type = type;
	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public List<ObjectId> getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(List<ObjectId> databaseId) {
		this.databaseId = databaseId;
	}

	public List<DataServiceParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameter> parameters) {
		this.parameters = parameters;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	
}
