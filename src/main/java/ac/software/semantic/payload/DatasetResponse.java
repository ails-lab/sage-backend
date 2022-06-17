package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ac.software.semantic.model.ImportType;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.IndexingState;
import ac.software.semantic.model.ResourceOption;

public class DatasetResponse {
	private String id;

   	private String name;
   	private List<String> typeUri;
    
//   	@JsonInclude(JsonInclude.Include.NON_NULL)
//    private String sourceUri;
//   	
//   	@JsonInclude(JsonInclude.Include.NON_NULL)
//    private String targetUri;
    
   	private String uuid;
   	
   	private List<DatasetResponse> datasets;

   	private String publishDatabase;
   	
    private DatasetState publishState;

   	@JsonInclude(JsonInclude.Include.NON_NULL)
    private Date publishStartedAt;
   	
   	@JsonInclude(JsonInclude.Include.NON_NULL)
    private Date publishCompletedAt;
    
   	private String type;
    
    private IndexingState indexState;
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Date indexStartedAt;
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Date indexCompletedAt;
    
    private List<ResourceOption> links;

	private ImportType importType;
   	public DatasetResponse() {
	   this.datasets = new ArrayList<>();
	   
	   publishState = DatasetState.UNPUBLISHED;
	   indexState = IndexingState.NOT_INDEXED;
	   
	   this.links = new ArrayList<>();
	}

	public ImportType getImportType() {
		return importType;
	}

	public void setImportType(ImportType importType) {
		this.importType = importType;
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

    public void addDataset(DatasetResponse dr) {
    	datasets.add(dr);
    }
    
    public List<DatasetResponse> getDatasets() {
    	return datasets;
    }
    
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
   @Override
   public String toString() {
       return "id=" + id + " name=" + name + " datasets=" + datasets;  
   }

	public String getUuid() {
		return uuid;
	}
	
	public void setUuid(String uuid) {
		this.uuid = uuid;
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



	public List<String> getTypeUri() {
		return typeUri;
	}

	public void setTypeUri(List<String> typeUri) {
		this.typeUri = typeUri;
	}

//	public String getSourceUri() {
//		return sourceUri;
//	}
//
//	public void setSourceUri(String sourceUri) {
//		this.sourceUri = sourceUri;
//	}
//
//	public String getTargetUri() {
//		return targetUri;
//	}
//
//	public void setTargetUri(String targetUri) {
//		this.targetUri = targetUri;
//	}

	public DatasetState getPublishState() {
		return publishState;
	}

	public void setPublishState(DatasetState publishState) {
		if (publishState == null) {
			this.publishState = DatasetState.UNPUBLISHED;
		} else {
			this.publishState = publishState;
		}
	}

	public IndexingState getIndexState() {
		return indexState;
	}

	public void setIndexState(IndexingState indexState) {
		if (indexState == null) {
			this.indexState = IndexingState.NOT_INDEXED;
		} else {
			this.indexState = indexState;
		}
	}

	public Date getIndexStartedAt() {
		return indexStartedAt;
	}

	public void setIndexStartedAt(Date indexStartedAt) {
		this.indexStartedAt = indexStartedAt;
	}

	public Date getIndexCompletedAt() {
		return indexCompletedAt;
	}

	public void setIndexCompletedAt(Date indexCompletedAt) {
		this.indexCompletedAt = indexCompletedAt;
	}

	public List<ResourceOption> getLinks() {
		return links;
	}

	public void setLinks(List<ResourceOption> links) {
		this.links = links;
	}

	public String getPublishDatabase() {
		return publishDatabase;
	}

	public void setPublishDatabase(String publishDatabase) {
		this.publishDatabase = publishDatabase;
	}	

}
