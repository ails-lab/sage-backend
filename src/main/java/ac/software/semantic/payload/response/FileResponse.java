package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileResponse implements Response, ExecutePublishResponse, MultiUserResponse {
    private String id;
    private String name;
    private String datasetId;
    private String uuid;
    
    private String description;
    
    private ResponseTaskObject executeState;
    private ResponseTaskObject publishState;

//    private String fileName;
    private String url;
    
    private List<String> files;
    
    private Date createdAt;
    private Date updatedAt;


   private Boolean publishedFromCurrentFileSystem;
   private Boolean newExecution;
	   
   private boolean active;
   
   private int order;
   private int group;
   
   private boolean ownedByUser;
   
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


	public String getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	
	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public Boolean isNewExecution() {
		return newExecution;
	}

	public void setNewExecution(Boolean newExecution) {
		this.newExecution = newExecution;
	}

	public Boolean isPublishedFromCurrentFileSystem() {
		return publishedFromCurrentFileSystem;
	}

	public void setPublishedFromCurrentFileSystem(Boolean publishedFromCurrentFileSystem) {
		this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
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

//	public String getFileName() {
//		return fileName;
//	}
//
//	public void setFileName(String fileName) {
//		this.fileName = fileName;
//	}

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

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public int getGroup() {
		return group;
	}

	public void setGroup(int group) {
		this.group = group;
	}

	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}	
}
