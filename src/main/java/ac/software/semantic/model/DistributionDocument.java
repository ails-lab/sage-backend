package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.CreatableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.type.ExportVocabulary;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.model.state.IndexState;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "DistributionDocuments")
public class DistributionDocument implements CreatableDocument<IndexState>, SpecificationDocument, DatedDocument, IdentifiableDocument {

	@Id
	@JsonIgnore
	private ObjectId id;

	private String uuid;

	@JsonIgnore
	private ObjectId databaseId;

	@JsonIgnore
	private ObjectId userId;
	
	@JsonIgnore	
	private ObjectId datasetId;

	@JsonIgnore
	private String datasetUuid;
	
	private String name;
	
	private String identifier;
	
	private List<String> classes;
	private List<SerializationType> serializations;
	
	private ExportVocabulary serializationVocabulary;
	
	private String license;
	
	private String compress;

//	private ObjectId indexStructureId;
//	
//	private ObjectId elasticConfigurationId;

	private Date createdAt;
	private Date updatedAt;
	
	private List<IndexState> create;
	
//	@JsonProperty("default")
//	@Field("default")
//	private boolean idefault;
	
	private DistributionDocument() {
		super();
	}

	public DistributionDocument(Database database) {
		this();
		
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = database.getId();
	}

	public DistributionDocument(Dataset dataset) {
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

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	@Override
	public List<IndexState> getCreate() {
		return create;
	}

	@Override
	public void setCreate(List<IndexState> create) {
		this.create = create;
	}

	@Override
	public IndexState getCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId) {
		if (create != null) {
			for (IndexState s : create) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					return s;
				}
			}
		} else {
			create = new ArrayList<>();
		}
		
		IndexState s = new IndexState();
		s.setCreateState(CreatingState.NOT_CREATED);
		s.setFileSystemConfigurationId(fileSystemConfigurationId);
		create.add(s);
		
		return s;
	}

	@Override
	public IndexState checkCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId) {
		if (create != null) {		
			for (IndexState s : create) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}
	
	@Override
	public void deleteCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId) {
		if (create != null) {
			for (int i = 0; i < create.size(); i++) {
				if (create.get(i).getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					create.remove(i);
					break;
				}
			}
		
			if (create.size() == 0) {
				create = null;
			}
		}
	}

	public List<String> getClasses() {
		return classes;
	}

	public void setClasses(List<String> classes) {
		this.classes = classes;
	}

	public List<SerializationType> getSerializations() {
		return serializations;
	}

	public void setSerializations(List<SerializationType> serializations) {
		this.serializations = serializations;
	}

	public ExportVocabulary getSerializationVocabulary() {
		return serializationVocabulary;
	}

	public void setSerializationVocabulary(ExportVocabulary serializationVocabulary) {
		this.serializationVocabulary = serializationVocabulary;
	}

	public String getLicense() {
		return license;
	}

	public void setLicense(String license) {
		this.license = license;
	}

	public String getCompress() {
		return compress;
	}

	public void setCompress(String compress) {
		this.compress = compress;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getIdentifier() {
		return identifier;
	}


	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}


	public Date getCreatedAt() {
		return createdAt;
	}


	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	@Override
	public String getIdentifier(IdentifierType type) {
		return getIdentifier();
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		setIdentifier(identifier);
	}

}