package ac.software.semantic.model;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.DatasetContained;
import ac.software.semantic.model.base.ServiceDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.expr.Computation;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.service.SideSpecificationDocument;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "ComparatorDocuments")
public class ComparatorDocument implements SpecificationDocument, DatedDocument, IdentifiableDocument {
	@Id
	@JsonIgnore
	private ObjectId id;

	@JsonIgnore
	private ObjectId databaseId;

	@JsonIgnore
	private ObjectId userId;

	private ObjectId datasetId;
	private String datasetUuid;

	private String uuid;
	private String identifier;
	
	private String name;

	private Date createdAt;
	private Date updatedAt;

	private SchemaSelector structure;
//	private ClassIndexElement element;

//	private String onClass;

//	private List<String> keys;

	private Computation computation;
	
//	private List<IndexKeyMetadata> keysMetadata;
	
	private String description;
	
	private ObjectId schemaDatasetId;
	
	private ComparatorDocument() {
		super();
	}
	
	public ComparatorDocument(Database database) {
		this();
		
		this.databaseId = database.getId();
	}
	
	public ComparatorDocument(Dataset dataset) {
		this();
		
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = dataset.getDatabaseId();
		   
		this.datasetId = dataset.getId();
		this.datasetUuid = dataset.getUuid();
	}
	
	public ObjectId getId() {
		return id;
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

//	public ObjectId getDatasetId() {
//		return datasetId;
//	}
//
//	public void setDatasetId(ObjectId datasetId) {
//		this.datasetId = datasetId;
//	}
//
//	public String getDatasetUuid() {
//		return datasetUuid;
//	}
//
//	public void setDatasetUuid(String datasetUuid) {
//		this.datasetUuid = datasetUuid;
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

//	public ClassIndexElement getElement() {
//		return element;
//	}
//
//	public void setElement(ClassIndexElement element) {
//		this.element = element;
//	}

//	public String getOnClass() {
//		return onClass;
//	}
//
//	public void setOnClass(String onClass) {
//		this.onClass = onClass;
//	}

//	public List<String> getKeys() {
//		return keys;
//	}
//
//	public void setKeys(List<String> keys) {
//		this.keys = keys;
//	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public Computation getComputation() {
		return computation;
	}

	public void setComputation(Computation computation) {
		this.computation = computation;
	}

//	public List<IndexKeyMetadata> getKeysMetadata() {
//		return keysMetadata;
//	}
//
//	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
//		this.keysMetadata = keysMetadata;
//	}

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

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public ObjectId getSchemaDatasetId() {
		return schemaDatasetId;
	}

	public void setSchemaDatasetId(ObjectId schemaDatasetId) {
		this.schemaDatasetId = schemaDatasetId;
	}

	@Override
	public String getIdentifier(IdentifierType type) {
		return getIdentifier();
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		setIdentifier(identifier);
	}

	public SchemaSelector getStructure() {
		return structure;
	}

	public void setStructure(SchemaSelector structure) {
		this.structure = structure;
	}
}
