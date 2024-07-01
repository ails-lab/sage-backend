package ac.software.semantic.payload.response;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.index.ClassIndexElement;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class EmbedderDocumentResponse implements Response, ExecutePublishResponse, MultiUserResponse {
   
   private String id;
   private String uuid;

   private ClassIndexElement element;
   private String embedder;
   
   private String variant;
   
	private String onClass;
	
	private ResponseTaskObject executeState;
	private ResponseTaskObject publishState;

   private Boolean publishedFromCurrentFileSystem;
   private Boolean newExecution;
   
	private Date updatedAt;
	private Date createdAt;

	private boolean ownedByUser;

	
   public EmbedderDocumentResponse() {
   }
   
   public String getId() {
       return id;
   }
   
	public void setId(String id) {
		this.id = id;
	}

	public String getEmbedder() {
		return embedder;
	}

	public void setEmbedder(String embedder) {
		this.embedder = embedder;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Boolean isPublishedFromCurrentFileSystem() {
		return publishedFromCurrentFileSystem;
	}

	public void setPublishedFromCurrentFileSystem(Boolean publishedFromCurrentFileSystem) {
		this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
	}

	public Boolean isNewExecution() {
		return newExecution;
	}

	public void setNewExecution(Boolean newExecution) {
		this.newExecution = newExecution;
	}

	public ClassIndexElement getElement() {
		return element;
	}

	public void setElement(ClassIndexElement element) {
		this.element = element;
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

	public String getVariant() {
		return variant;
	}

	public void setVariant(String variant) {
		this.variant = variant;
	}

	public ResponseTaskObject getExecuteState() {
		return executeState;
	}

	public void setExecuteState(ResponseTaskObject executeState) {
		this.executeState = executeState;
	}

	public ResponseTaskObject getPublishState() {
		return publishState;
	}

	public void setPublishState(ResponseTaskObject publishState) {
		this.publishState = publishState;
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

	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}

}