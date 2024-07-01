package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.RemoteTripleStore;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.VocabularyEntityDescriptor;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.DatasetCategory;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetTag;
import ac.software.semantic.model.constants.type.DatasetType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasetResponse implements Response, PublishResponse {
	private String id;

   	private String name;
//   	private List<String> typeUri;
   	private List<DatasetTag> tags;
    
   	private String uuid;
   	
   	private String identifier;
   	
   	private List<DatasetResponse> datasets;
   	
   	@JsonProperty("public")
    private boolean publik;
    
   	private DatasetCategory category;
    private DatasetScope scope;
   	private DatasetType type;

   	private ResponseTaskObject publishState;
   	private ResponseTaskObject loadState;
//   	private String publishDatabase;

    private MappingState createDistributionState;
    private Date createDistributionStartedAt;
    private Date createDistributionCompletedAt;
    private List<NotificationMessage> createDistributionMessages;
    
    private List<ResourceOption<String>> links;

	private TemplateResponse template;
	
	private String asProperty;
   	
//	private ThesaurusLoadState loadState;
	
	private RemoteTripleStore remoteTripleStore;

	private List<VocabularyEntityDescriptor> entityDescriptors;
	
	private UserResponse user;
	private List<MappingResponse> mappings;
	private List<FileResponse> rdfFiles;
	private List<IndexDocumentResponse> indices;
	private List<DistributionDocumentResponse> distributions;
	private List<UserTaskResponse> userTasks;
	private List<PrototypeDocumentResponse> prototypes;
	
	private List<ProjectDocumentResponse> projects;
	
	private Date createdAt;
	private Date updatedAt;
	
	private Integer maxGroup;
	private List<Integer> publicGroups;
	
	private boolean multiGroup;

	public DatasetResponse() {
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
    	if (datasets == null) {
    		datasets = new ArrayList<>();
    	}
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

//	public List<String> getTypeUri() {
//		return typeUri;
//	}
//
//	public void setTypeUri(List<String> typeUri) {
//		this.typeUri = typeUri;
//	}

	public List<ResourceOption<String>> getLinks() {
		return links;
	}

	public void setLinks(List<ResourceOption<String>> links) {
		this.links = links != null && links.size() > 0 ? links : null;
	}

//	public String getPublishDatabase() {
//		return publishDatabase;
//	}
//
//	public void setPublishDatabase(String publishDatabase) {
//		this.publishDatabase = publishDatabase;
//	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

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

	public RemoteTripleStore getRemoteTripleStore() {
		return remoteTripleStore;
	}

	public void setRemoteTripleStore(RemoteTripleStore remoteTripleStore) {
		this.remoteTripleStore = remoteTripleStore;
	}

	public DatasetCategory getCategory() {
		return category;
	}

	public void setCategory(DatasetCategory category) {
		this.category = category;
	}

	public ResponseTaskObject getPublishState() {
		return publishState;
	}

	public void setPublishState(ResponseTaskObject publishState) {
		this.publishState = publishState;
	}

	public ResponseTaskObject getLoadState() {
		return loadState;
	}

	public void setLoadState(ResponseTaskObject loadState) {
		this.loadState = loadState;
	}

	public UserResponse getUser() {
		return user;
	}

	public void setUser(UserResponse user) {
		this.user = user;
	}

	public List<MappingResponse> getMappings() {
		return mappings;
	}

	public void setMappings(List<MappingResponse> mappings) {
		this.mappings = mappings != null && mappings.size() > 0 ? mappings : null;
	}

	public List<FileResponse> getRdfFiles() {
		return rdfFiles;
	}

	public void setRdfFiles(List<FileResponse> rdfFiles) {
		this.rdfFiles = rdfFiles != null && rdfFiles.size() > 0 ? rdfFiles : null;
	}

	public List<IndexDocumentResponse> getIndices() {
		return indices;
	}

	public void setIndices(List<IndexDocumentResponse> indices) {
		this.indices = indices != null && indices.size() > 0 ? indices : null;
	}

	public List<DistributionDocumentResponse> getDistributions() {
		return distributions;
	}

	public void setDistributions(List<DistributionDocumentResponse> distributions) {
		this.distributions = distributions != null && distributions.size() > 0 ? distributions : null;
	}

	public List<UserTaskResponse> getUserTasks() {
		return userTasks;
	}

	public void setUserTasks(List<UserTaskResponse> userTasks) {
		this.userTasks = userTasks != null && userTasks.size() > 0 ? userTasks : null;
	}

	public List<VocabularyEntityDescriptor> getEntityDescriptors() {
		return entityDescriptors;
	}

	public void setEntityDescriptors(List<VocabularyEntityDescriptor> entityDescriptors) {
		this.entityDescriptors = entityDescriptors;
	}

	public List<DatasetTag> getTags() {
		return tags;
	}

	public void setTags(List<DatasetTag> tags) {
		this.tags = tags;
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

	public List<ProjectDocumentResponse> getProjects() {
		return projects;
	}

	public void setProjects(List<ProjectDocumentResponse> projects) {
		this.projects = projects;
	}

	public Integer getMaxGroup() {
		return maxGroup;
	}

	public void setMaxGroup(Integer maxGroup) {
		this.maxGroup = maxGroup;
	}

	public boolean isMultiGroup() {
		return multiGroup;
	}

	public void setMultiGroup(boolean multiGroup) {
		this.multiGroup = multiGroup;
	}

	public List<PrototypeDocumentResponse> getPrototypes() {
		return prototypes;
	}

	public void setPrototypes(List<PrototypeDocumentResponse> prototypes) {
		this.prototypes = prototypes;
	}

	public List<Integer> getPublicGroups() {
		return publicGroups;
	}

	public void setPublicGroups(List<Integer> publicGroups) {
		this.publicGroups = publicGroups;
	}

}
