package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.MappingState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileResponse {
    private String id;
    private String name;
    private String datasetId;
    private String uuid;
    
    private String fileName;

    private DatasetState publishState;
    private MappingState executeState;
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Date publishStartedAt;
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Date publishCompletedAt;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Date executeStartedAt;
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Date executeCompletedAt;

    private List<String> files;

//    private String url;
    
    private boolean publishedFromCurrentFileSystem;
   private boolean newExecution;
	   
   @JsonIgnore
   private boolean legacy;

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

	public DatasetState getPublishState() {
		return publishState;
	}

	public void setPublishState(DatasetState publishState) {
		this.publishState = publishState;
	}

	public Date getPublishStartedAt() {
		return publishStartedAt;
	}

	public void setPublishStartedAt(Date publishStartedAt) {
		this.publishStartedAt = publishStartedAt;
	}

	public Date getPublishCompletedAt() {
		return publishCompletedAt;
	}

	public void setPublishCompletedAt(Date publishCompletedAt) {
		this.publishCompletedAt = publishCompletedAt;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public MappingState getExecuteState() {
		return executeState;
	}

	public void setExecuteState(MappingState executeState) {
		this.executeState = executeState;
	}
	
	public Date getExecuteStartedAt() {
		return executeStartedAt;
	}

	public void setExecuteStartedAt(Date executeStartedAt) {
		this.executeStartedAt = executeStartedAt;
	}

	public Date getExecuteCompletedAt() {
		return executeCompletedAt;
	}

	public void setExecuteCompletedAt(Date executeCompletedAt) {
		this.executeCompletedAt = executeCompletedAt;
	}

	public boolean isNewExecution() {
		return newExecution;
	}

	public void setNewExecution(boolean newExecution) {
		this.newExecution = newExecution;
	}

	public boolean isLegacy() {
		return legacy;
	}

	public void setLegacy(boolean legacy) {
		this.legacy = legacy;
	}

	public boolean isPublishedFromCurrentFileSystem() {
		return publishedFromCurrentFileSystem;
	}

	public void setPublishedFromCurrentFileSystem(boolean publishedFromCurrentFileSystem) {
		this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
	}

//	public String getUrl() {
//		return url;
//	}
//
//	public void setUrl(String url) {
//		this.url = url;
//	}

}
