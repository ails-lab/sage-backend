package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.type.PrototypeType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PrototypeDocumentResponse implements Response, MultiUserResponse {
	
	private String id;

	private String name;
	private String description;
	   
	private String uuid;
	   
	private String url;
	
	private PrototypeType type;

	private Date createdAt;
	private Date updatedAt;
	
	private String content;
	
	private List<DataServiceParameter> parameters;
	private List<DataServiceParameter> fields;
	
	private boolean ownedByUser;
	
	public PrototypeDocumentResponse() {
		
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

	public void setParameters(List<DataServiceParameter> parameters) {
		this.parameters = parameters;
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

	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}
}
