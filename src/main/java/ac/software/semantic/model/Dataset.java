package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.GroupingDocument;
import ac.software.semantic.model.base.InverseMemberDocument;
import ac.software.semantic.model.base.MemberDocument;
import ac.software.semantic.model.base.PublishableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.DatasetCategory;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetTag;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.ResourceOptionType;
import ac.software.semantic.model.state.DatasetPublishState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.service.DatasetEncloser;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Datasets")
public class Dataset extends PublishableDocument<MappingExecuteState, DatasetPublishState> 
                     implements SpecificationDocument, 
                                EnclosingDocument, 
                                MemberDocument<Dataset>, 
                                DatasetEncloser, 
                                InverseMemberDocument<ProjectDocument>,
                                DatedDocument, IdentifiableDocument,
                                GroupingDocument {

	@Id
	private ObjectId id;

	@JsonIgnore
	@Indexed
	private ObjectId databaseId;

	private String name;
	private List<String> typeUri; // obsolete: keep for compatibility // replaced by tags
	private List<DatasetTag> tags;

	@Field("public")
	private boolean publik;

	@Indexed
	private ObjectId userId;
	
	private String uuid;

	private String identifier;

	private List<ResourceOption<ObjectId>> links;
	private List<ResourceOption> options;

	private List<ObjectId> datasets;

//	private List<IndexStateOld> index; // do not delete related code

	private List<DatasetPublishState> firstPublish;

	private String asProperty;

	private DatasetCategory category;
	private DatasetScope scope;
	private String type; // for compatibility not DatasetType

	private ObjectId templateId;

	private List<ParameterBinding> binding;

	private Date updatedAt;
	private Date createdAt;
	
	private RemoteTripleStore remoteTripleStore;
	
	private List<ObjectId> projectId;

	private List<VocabularyEntityDescriptor> entityDescriptors;
	
//	private Integer version;
	
	private Integer maxGroup;
	private List<Integer> publicGroups;
	
	private boolean multiGroup;
	
	private Dataset() {
	}

	public Dataset(Database database) {
		this();
		
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = database.getId();
	}
	
	public ObjectId getId() {
		return id;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public List<ObjectId> getDatasets() {
		return datasets;
	}

	public DatasetScope getScope() {
		if (scope == null) {
			int p = type.indexOf("-");
			return DatasetScope.get(type.substring(0, p).toUpperCase());
		} else {
			return scope;
		}
	}

	public void setScope(DatasetScope scope) {
		this.scope = scope;
	}

	public DatasetType getDatasetType() {
		if (scope == null) {
			int p = type.indexOf("-");
			return DatasetType.get(type.substring(p + 1).toUpperCase());
		}

		return DatasetType.get(type);
	}

	public void setDatasetType(DatasetType type) {
		this.type = type.toString();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getTypeUri() {
		return typeUri;
	}
	
	//temp 
	public List<String> getTypeUris() {
		List<String> res = new ArrayList<>();
		
		DatasetScope scope = getScope();
		DatasetType type = getDatasetType();
		
		if (type == DatasetType.CATALOG) {
			res.add(SEMAVocabulary.DataCatalog.toString());
		} else if (type == DatasetType.DATASET) {
			if (scope == DatasetScope.VOCABULARY) {
				res.add(SEMAVocabulary.VocabularyCollection.toString());
			} else if (scope == DatasetScope.COLLECTION) { 			
				res.add(SEMAVocabulary.DataCollection.toString());
			} else if (scope == DatasetScope.ALIGNMENT) { 			
				res.add(SEMAVocabulary.Alignment.toString());
			}
		}
		
		return res;
	}

	public void setTypeUri(List<String> typeUri) {
		this.typeUri = typeUri;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public List<DatasetPublishState> getFirstPublish() {
		return firstPublish;
	}

	public void setFirstPublish(List<DatasetPublishState> firstPublish) {
		this.firstPublish = firstPublish;
	}

	public DatasetPublishState checkFirstPublishState(ObjectId databaseConfigurationId) {
		if (firstPublish != null) {
			for (DatasetPublishState s : firstPublish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}

		return null;
	}

	public void setFirstPublishState(DatasetPublishState ps, ObjectId databaseConfigurationId) {
		if (firstPublish != null) {
			for (int i = 0; i < firstPublish.size(); i++) {
				if (firstPublish.get(i).getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					firstPublish.set(i, ps);
					return;
				}
			}
		} else {
			firstPublish = new ArrayList<>();
		}

		firstPublish.add(ps);
	}

	public List<ResourceOption<ObjectId>> getLinks() {
		return links;
	}

	public void setLinks(List<ResourceOption<ObjectId>> links) {
		this.links = links;
	}

	public void addLink(ResourceOption<ObjectId> ro) {
		this.links.add(ro);
	}

	public List<ResourceOption> getOptions() {
		return options;
	}

	public void setOptions(List<ResourceOption> options) {
		this.options = options;
	}

	public ResourceOption<ObjectId> getLinkByType(ResourceOptionType type) {
		if (links != null) {
			for (ResourceOption<ObjectId> ro : links) {
				if (ro.getType() == type) {
					return ro;
				}
			}
		}

		return null;
	}

	public ResourceOption getOptionsByType(ResourceOptionType type) {
		if (options != null) {
			for (ResourceOption ro : options) {
				if (ro.getType() == type) {
					return ro;
				}
			}
		}

		return null;
	}

	public TripleStoreConfiguration getPublishVirtuosoConfiguration(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) { // currently support only one publication site
			if (checkPublishState(vc.getId()) != null) {
				return vc;
			}
		}

		return null;
	}

	public ProcessStateContainer<DatasetPublishState> getCurrentPublishState(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) {
			DatasetPublishState ps = checkPublishState(vc.getId());
			if (ps != null) {
				return new ProcessStateContainer<>(ps, vc);
			}
		}

		return null;
	}

	public boolean isPublik() {
		return publik;
	}

	public void setPublik(boolean publik) {
		this.publik = publik;
	}

	public ObjectId getTemplateId() {
		return templateId;
	}

	public void setTemplateId(ObjectId templateId) {
		this.templateId = templateId;
	}

	public List<ParameterBinding> getBinding() {
		return binding;
	}

	public void setBinding(List<ParameterBinding> binding) {
		this.binding = binding;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public RemoteTripleStore getRemoteTripleStore() {
		return remoteTripleStore;
	}

	public void setRemoteTripleStore(RemoteTripleStore remoteTripleStore) {
		this.remoteTripleStore = remoteTripleStore;
	}
	
	public boolean isRemote() {
		return this.remoteTripleStore != null;
	}

	public boolean isLocal() {
		return this.remoteTripleStore == null;
	}
	
	public String getSparqlEndpoint(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
    	if (isLocal()) {
        	// get triple store of first dataset ... wrong!

    		// !!! ASSUME ALL DATASETS ARE IN THE SAME TRIPLE STORE !!! NOT TRUE IN GENERAL
    		return getPublishVirtuosoConfiguration(virtuosoConfigurations).getSparqlEndpoint();
    	} else {
    		return getRemoteTripleStore().getSparqlEndpoint();
    	}
	}

	public DatasetCategory getCategory() {
		return category;
	}

	public void setCategory(DatasetCategory category) {
		this.category = category;
	}

	@Override
	public boolean hasMember(Dataset ds) {
		if (datasets == null) {
			return false;
		} else {
			return datasets.contains(ds.getId());
		}
		
	}
	
	@Override
	public void addMember(Dataset ds) {
		addDataset(ds.getId());
	}
	
	@Override
	public void removeMember(Dataset ds) {
		removeDataset(ds.getId());
	}
	
	@Override
	public List<ObjectId> getMemberIds(Class<? extends Dataset> clazz) {
		return getDatasets();
	}
	
	public void addDataset(ObjectId dataset) {
		if (datasets == null) {
			datasets = new ArrayList<>();
		}
		this.datasets.add(dataset);
	}

	public void removeDataset(ObjectId dataset) {
		if (datasets != null) {
			this.datasets.remove(dataset);
		}
	}

	public List<ObjectId> getProjectId() {
		return projectId;
	}

	public void setProjectId(List<ObjectId> projectId) {
		this.projectId = projectId;
	}

	@Override
	public void addTo(ProjectDocument source) {
		if (projectId == null) {
			projectId = new ArrayList<>();
		}
		projectId.add(source.getId());
		
	}

	@Override
	public void removeFrom(ProjectDocument source) {
		if (projectId != null) {
			projectId.remove(source.getId());
			
			if (projectId.isEmpty()) {
				projectId = null;
			}
		}
		
	}

	@Override
	public boolean isMemberOf(ProjectDocument target) {
		if (projectId != null) {
			return projectId.contains(target.getId());
		} else {
			return false;
		}
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

	public Resource asResource(SEMRVocabulary resourceVocabulary) {
		return resourceVocabulary.getDatasetContentAsResource(this);
	}

	public String getContentTripleStoreGraph(SEMRVocabulary resourceVocabulary) {
		return getContentTripleStoreGraph(resourceVocabulary, 0);
	}

	public String getContentTripleStoreGraph(SEMRVocabulary resourceVocabulary, int group) {
		if (getScope() == DatasetScope.ANNOTATION && getDatasetType() == DatasetType.DATASET && getAsProperty() != null) { // legacy
			return getAsProperty();
		} else {
			return resourceVocabulary.getDatasetContentAsResource(this).toString() + (group > 0 ? "_" + group : "") ;
		}
	}

	public String getMetadataTripleStoreGraph(SEMRVocabulary resourceVocabulary) {
//		if (version == null) {
//			return resourceVocabulary.getContentGraphResource(this).toString();
//		} else {
			return resourceVocabulary.getDatasetMetadataAsResource(this).toString();
//		}
	}

	@Override
	public int getMaxGroup() {
		if (maxGroup == null) {
			return 0;
		} else {
			return maxGroup;
		}
	}

	@Override
	public void setMaxGroup(int maxGroup) {
		this.maxGroup = maxGroup;
	}

	public boolean isMultiGroup() {
		return multiGroup;
	}

	public void setMultiGroup(boolean multiGroup) {
		this.multiGroup = multiGroup;
	}

	@Override
	public String getIdentifier(IdentifierType type) {
		return getIdentifier();
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		setIdentifier(identifier);
	}

	public List<Integer> getPublicGroups() {
		return publicGroups;
	}

	public void setPublicGroups(List<Integer> publicGroups) {
		this.publicGroups = publicGroups;
	}

}
