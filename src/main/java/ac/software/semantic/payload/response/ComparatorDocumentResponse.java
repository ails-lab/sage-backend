package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.model.expr.Computation;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComparatorDocumentResponse implements Response, MultiUserResponse {
	
	private String id;

	private String name;
	private String description;
	   
	private String uuid;
	   
	private String url;
	
//	private PrototypeType type;

	private Date createdAt;
	private Date updatedAt;
	
	private String identifier;
	
//	private String onClass;
	private ClassIndexElementResponse element;
	private List<IndexKeyMetadata> keysMetadata;

	private Computation computation;
	
	private String schemaDatasetId;
	
	private boolean ownedByUser;
	
	public ComparatorDocumentResponse() {
		
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
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

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}

//	public String getOnClass() {
//		return onClass;
//	}
//
//	public void setOnClass(String onClass) {
//		this.onClass = onClass;
//	}

	public ClassIndexElementResponse getElement() {
		return element;
	}

	public void setElement(ClassIndexElementResponse element) {
		this.element = element;
	}

	public List<IndexKeyMetadata> getKeysMetadata() {
		return keysMetadata;
	}

	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
		this.keysMetadata = keysMetadata;
	}

	public Computation getComputation() {
		return computation;
	}

	public void setComputation(Computation computation) {
		this.computation = computation;
	}

	public String getSchemaDatasetId() {
		return schemaDatasetId;
	}

	public void setSchemaDatasetId(String schemaDatasetId) {
		this.schemaDatasetId = schemaDatasetId;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
}
