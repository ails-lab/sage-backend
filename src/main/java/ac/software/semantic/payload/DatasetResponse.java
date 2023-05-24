package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.DatasetType;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.constants.ThesaurusLoadStatus;
import ac.software.semantic.model.state.MappingState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasetResponse implements Response {
	private String id;

   	private String name;
   	private List<String> typeUri;
    
//   	@JsonInclude(JsonInclude.Include.NON_NULL)
//    private String sourceUri;
//   	
//   	@JsonInclude(JsonInclude.Include.NON_NULL)
//    private String targetUri;
    
   	private String uuid;
   	
   	private String identifier;
   	
   	private List<DatasetResponse> datasets;
   	
   	@JsonProperty("public")
    private boolean publik;
    
//    private String tripleStore;

    private DatasetScope scope;
   	private DatasetType type;

    private DatasetState publishState;
   	private String publishDatabase;
    private Date publishStartedAt;
    private Date publishCompletedAt;
    private List<NotificationMessage> publishMessages;
    
    private IndexingState indexState;
   	private String indexDatabase;
    private Date indexStartedAt;
    private Date indexCompletedAt;
    private List<NotificationMessage> indexMessages;

    private MappingState createDistributionState;
    private Date createDistributionStartedAt;
    private Date createDistributionCompletedAt;
    private List<NotificationMessage> createDistributionMessages;
    
    private List<ResourceOption> links;

	private TemplateResponse template;
	
	private String asProperty;
   	
	private ThesaurusLoadStatus loadState;
	
	public DatasetResponse() {
	   this.datasets = new ArrayList<>();
	   
	   publishState = DatasetState.UNPUBLISHED;
	   indexState = IndexingState.NOT_INDEXED;
	   
	   this.links = new ArrayList<>();
	}

//	public ImportType getImportType() {
//		return importType;
//	}
//
//	public void setImportType(ImportType importType) {
//		this.importType = importType;
//	}

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
    
	public DatasetType getType() {
		return type;
	}

	public void setType(DatasetType type) {
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

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

//	public String getTripleStore() {
//		return tripleStore;
//	}
//
//	public void setTripleStore(String tripleStore) {
//		this.tripleStore = tripleStore;
//	}

	public boolean isPublik() {
		return publik;
	}

	public void setPublik(boolean publik) {
		this.publik = publik;
	}

	public TemplateResponse getTemplate() {
		return template;
	}

	public void setTemplate(TemplateResponse template) {
		this.template = template;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public DatasetScope getScope() {
		return scope;
	}

	public void setScope(DatasetScope scope) {
		this.scope = scope;
	}

	public ThesaurusLoadStatus getLoadState() {
		return loadState;
	}

	public void setLoadState(ThesaurusLoadStatus loadState) {
		this.loadState = loadState;
	}

	public MappingState getCreateDistributionState() {
		return createDistributionState;
	}

	public void setCreateDistributionState(MappingState createDistributionState) {
		this.createDistributionState = createDistributionState;
	}

	public Date getCreateDistributionStartedAt() {
		return createDistributionStartedAt;
	}

	public void setCreateDistributionStartedAt(Date createDistributionStartedAt) {
		this.createDistributionStartedAt = createDistributionStartedAt;
	}

	public Date getCreateDistributionCompletedAt() {
		return createDistributionCompletedAt;
	}

	public void setCreateDistributionCompletedAt(Date createDistributionCompletedAt) {
		this.createDistributionCompletedAt = createDistributionCompletedAt;
	}

	public List<NotificationMessage> getCreateDistributionMessages() {
		return createDistributionMessages;
	}

	public void setCreateDistributionMessages(List<NotificationMessage> createDistributionMessages) {
		this.createDistributionMessages = createDistributionMessages;
	}

	public List<NotificationMessage> getPublishMessages() {
		return publishMessages;
	}

	public void setPublishMessages(List<NotificationMessage> publishMessages) {
		this.publishMessages = publishMessages;
	}

	public List<NotificationMessage> getIndexMessages() {
		return indexMessages;
	}

	public void setIndexMessages(List<NotificationMessage> indexMessages) {
		this.indexMessages = indexMessages;
	}

	public String getIndexDatabase() {
		return indexDatabase;
	}

	public void setIndexDatabase(String indexDatabase) {
		this.indexDatabase = indexDatabase;
	}	

}
