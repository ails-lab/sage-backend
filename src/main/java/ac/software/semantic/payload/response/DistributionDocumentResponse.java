package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.ExportVocabulary;
import ac.software.semantic.model.constants.type.SerializationType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DistributionDocumentResponse implements Response, CreateResponse {
   
	private String id;
	private String uuid;
	
	private String name;
	
	private String identifier;

	private List<String> classes;
	private List<SerializationType> serializations;
	
	private ExportVocabulary serializationVocabulary;
	
	private String license;
	
	private String compress;

	private ResponseTaskObject createState;
	
	private Date createdAt;
	private Date updatedAt;
	
	public DistributionDocumentResponse() {
		super();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
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

	public ResponseTaskObject getCreateState() {
		return createState;
	}

	public void setCreateState(ResponseTaskObject createState) {
		this.createState = createState;
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

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

}