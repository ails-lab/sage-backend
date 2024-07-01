package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IndexStructureResponse implements Response, MultiUserResponse {

	private String id;

	private String name;
	private String description;
	
	private String uuid;
	
	private String identifier;
	
	private String indexEngine;
	
	private Date createdAt;
	private Date updatedAt;
	
//	private Map<String, List<PathElement>> keyMap;
	
	private List<ClassIndexElementResponse> elements;
	private List<IndexKeyMetadata> keysMetadata;
	
	private String schemaDatasetId;
	
	private Object schema;
	
	private boolean ownedByUser;
	
	public IndexStructureResponse() {
//		keyMap = new HashMap<>();
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
	
//	public Map<String, List<PathElement>> getKeyMap() {
//		return keyMap;
//	}
//
//	public void setKeyMap(Map<String, List<PathElement>> keyMap) {
//		this.keyMap = keyMap;
//	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getIndexEngine() {
		return indexEngine;
	}

	public void setIndexEngine(String indexEngine) {
		this.indexEngine = indexEngine;
	}

	public List<ClassIndexElementResponse> getElements() {
		return elements;
	}

	public void setElements(List<ClassIndexElementResponse> elements) {
		this.elements = elements;
	}

	public List<IndexKeyMetadata> getKeysMetadata() {
		return keysMetadata;
	}

	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
		this.keysMetadata = keysMetadata;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
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

	public String getSchemaDatasetId() {
		return schemaDatasetId;
	}

	public void setSchemaDatasetId(String schemaDatasetId) {
		this.schemaDatasetId = schemaDatasetId;
	}
	
	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}

	public Object getSchema() {
		return schema;
	}

	public void setSchema(Object schema) {
		this.schema = schema;
	}
}
