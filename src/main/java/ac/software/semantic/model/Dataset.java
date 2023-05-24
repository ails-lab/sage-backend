package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.DatasetType;
import ac.software.semantic.model.constants.ResourceOptionType;
import ac.software.semantic.model.state.CreateDistributionState;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Datasets")
public class Dataset extends PublishDocument<PublishState> {
   @Id
   private ObjectId id;
   
   private ObjectId databaseId;

   private String name;
   private List<String> typeUri;
   
   @Field("public")
   private boolean publik;
   
//   private ObjectId tripleStoreId;
   
//   private String sourceUri;
//   private String targetUri;
   
   private ObjectId userId;
   private String uuid;
   
   private String identifier;
   
   private List<ResourceOption> links;
   private List<ResourceOption> options;
   
   private List<ObjectId> datasets;
   
//   private List<PublishState> publish;
   private List<CreateDistributionState> createDistribution;
   private List<IndexState> index;
   
   private List<PublishState> firstPublish;
   
   private String asProperty;
   
   private DatasetScope scope;
   private String type;
   

//   private ImportType importType;
   
   private ObjectId templateId;
   
   private List<ParameterBinding> binding;
   
   private Date updatedAt;
   
   public Dataset() {
       
//       datasets = new ArrayList<>();
       
//       publish = new ArrayList<>();
//       index = new ArrayList<>();
       
//       links = new ArrayList<>();
//       options = new ArrayList<>();
       
   }

   	public ObjectId getId() {
   		return id;
   	}

//	public ImportType getImportType() {
//		return importType;
//	}
//
//	public void setImportType(ImportType importType) {
//		this.importType = importType;
//	}

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
	
	public DatasetScope getScope() {
		if (scope == null) {
			int p = type.indexOf("-");
			return DatasetScope.get(type.substring(0,p).toUpperCase());
		} else {
			return scope;
		}
	}

	public void setScope(DatasetScope scope) {
		this.scope = scope;
	}
	
//	// legacy // should not be used
//	public String getType() {
//		return type;
//	}
		
	public DatasetType getDatasetType() {
		if (scope == null) {
			int p = type.indexOf("-");
			return DatasetType.get(type.substring(p + 1).toUpperCase());
		}
		
		return DatasetType.get(type);
	}

//	public void setType(String type) {
//		this.type = type;
//	}

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

//	public List<PublishState> getPublish() {
//		return publish;
//	}
//
//	public void setPublish(List<PublishState> publish) {
//		this.publish = publish;
//	}
	
	public List<PublishState> getFirstPublish() {
		return firstPublish;
	}

	public void setFirstPublish(List<PublishState> firstPublish) {
		this.firstPublish = firstPublish;
	}
	
//	public PublishState getPublishState(ObjectId databaseConfigurationId) {
//		if (publish != null) {		
//			for (PublishState s : publish) {
//				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
//					return s;
//				}
//			}
//		} else {
//			publish = new ArrayList<>();
//		}
//		
//		PublishState s = new PublishState();
//		s.setPublishState(DatasetState.UNPUBLISHED);
//		s.setDatabaseConfigurationId(databaseConfigurationId);
//		publish.add(s);
//		
//		return s;	
//	}
	
//	public PublishState checkPublishState(ObjectId databaseConfigurationId) {
//		if (publish != null) {
//			for (PublishState s : publish) {
//				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
//					return s;
//				}
//			}
//		}
//		
//		return null;
//	}
	
	public PublishState checkFirstPublishState(ObjectId databaseConfigurationId) {
		if (firstPublish != null) {
			for (PublishState s : firstPublish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}
	
	public void setFirstPublishState(PublishState ps, ObjectId databaseConfigurationId) {
		if (firstPublish != null ) {		
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
	
//	public synchronized void removePublishState(PublishState ps) {
//		if (publish != null) {
//			publish.remove(ps);
//		} 
//	}
	
	public synchronized void removeIndexState(IndexState is) {
		if (index != null) {
			index.remove(is);
		} 
	}

	public List<IndexState> getIndex() {
		return index;
	}

	public void setIndex(List<IndexState> index) {
		this.index = index;
	}	
	
	public IndexState getIndexState(ObjectId databaseConfigurationId) {
		if (index != null) {
			for (IndexState s : index) {
				if (s.getElasticConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			index = new ArrayList<>();
		}
		
		IndexState s = new IndexState();
		s.setElasticConfigurationId(databaseConfigurationId);
		index.add(s);
		
		return s;	
	}
	
	public IndexState checkIndexState(ObjectId databaseConfigurationId) {
		if (index != null) {
			for (IndexState s : index) {
				if (s.getElasticConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}

	public List<ResourceOption> getLinks() {
		return links;
	}

	public void setLinks(List<ResourceOption> links) {
		this.links = links;
	}
	
	public void addLink(ResourceOption ro) {
		this.links.add(ro);
	}

	public List<ResourceOption> getOptions() {
		return options;
	}

	public void setOptions(List<ResourceOption> options) {
		this.options = options;
	}
	
	public ResourceOption getLinkByType(ResourceOptionType type) {
		if (links != null) {
			for (ResourceOption ro : links) {
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
	
	public ProcessStateContainer getCurrentPublishState(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) {
			PublishState ps = checkPublishState(vc.getId());
			if (ps != null) {
				return new ProcessStateContainer(ps, vc);
			}
		}
		
		return null;
	}
	
	public ProcessStateContainer getCurrentIndexState(Collection<ElasticConfiguration> elasticConfigurations) {
		for (ElasticConfiguration ec : elasticConfigurations) {
			IndexState is = checkIndexState(ec.getId());
			if (is != null) {
				return new ProcessStateContainer(is, ec);
			}
		}
		
		return null;
	}

	public ProcessStateContainer getCurrentCreateDistributionState(ObjectId fileSystemConfigurationId, Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) {
			CreateDistributionState cds = checkCreateDistributionState(fileSystemConfigurationId, vc.getId());
			if (cds != null) {
				return new ProcessStateContainer(cds, vc);
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

//	public ObjectId getTripleStoreId() {
//		return tripleStoreId;
//	}
//
//	public void setTripleStoreId(ObjectId tripleStoreId) {
//		this.tripleStoreId = tripleStoreId;
//	}

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

	public List<CreateDistributionState> getCreateDistribution() {
		return createDistribution;
	}

	public void setCreateDistribution(List<CreateDistributionState> createDistribution) {
		this.createDistribution = createDistribution;
	}

	public CreateDistributionState getCreateDistributionState(ObjectId fileSystemConfigurationId, ObjectId tripleStoreConfigurationId) {
		if (createDistribution != null) {
			for (CreateDistributionState s : createDistribution) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId) && s.getTripleStoreConfigurationId().equals(tripleStoreConfigurationId)) {
					return s;
				}
			}
		} else {
			createDistribution = new ArrayList<>();
		}
		
		CreateDistributionState s = new CreateDistributionState();
		s.setCreateDistributionState(MappingState.NOT_EXECUTED);
		s.setFileSystemConfigurationId(fileSystemConfigurationId);
		s.setTripleStoreConfigurationId(tripleStoreConfigurationId);
		createDistribution.add(s);
		
		return s;
	}

	public CreateDistributionState checkCreateDistributionState(ObjectId fileSystemConfigurationId, ObjectId tripleStoreConfigurationId) {
		if (createDistribution != null) {
			for (CreateDistributionState s : createDistribution) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId) && s.getTripleStoreConfigurationId().equals(tripleStoreConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}		
	
	
	public synchronized void removeCreateDistributionState(CreateDistributionState cds) {
		if (createDistribution != null) {
			createDistribution.remove(cds);
			
			if (createDistribution.size() == 0) {
				createDistribution = null;
			}
		} 
	}

	public void deleteExecuteState(ObjectId fileSystemConfigurationId, ObjectId tripleStoreConfigurationId) {
		if (createDistribution != null) {
			for (int i = 0; i < createDistribution.size(); i++) {
				if (createDistribution.get(i).getFileSystemConfigurationId().equals(fileSystemConfigurationId) && createDistribution.get(i).getTripleStoreConfigurationId().equals(tripleStoreConfigurationId)) {
					createDistribution.remove(i);
					break;
				}
			}
			
			if (createDistribution.size() == 0) {
				createDistribution = null;
			}
				
		} 
	}

}

