package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "Datasets")
public class Dataset {
   @Id
   private ObjectId id;
   
   private ObjectId databaseId;

   private String name;
   private List<String> typeUri;
   
//   private String sourceUri;
//   private String targetUri;
   
   private ObjectId userId;
   private String uuid;
   
   private List<ResourceOption> links;
   private List<ResourceOption> options;
   
   private List<ObjectId> datasets;
   
   private List<PublishState> publish;
   private List<IndexState> index;
   
   private String asProperty;
   
   private String type;

   private ImportType importType;
   
   public Dataset() {
       
       datasets = new ArrayList<>();
       
       publish = new ArrayList<>();
       index = new ArrayList<>();
       
       links = new ArrayList<>();
       options = new ArrayList<>();
       
   }

   	public ObjectId getId() {
   		return id;
   	}

	public ImportType getImportType() {
		return importType;
	}

	public void setImportType(ImportType importType) {
		this.importType = importType;
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

	public List<ObjectId> getDatasets() {
		return datasets;
	}

	public void addDataset(ObjectId dataset) {
		this.datasets.add(dataset);
	}

	public void removeDataset(ObjectId dataset) {
		this.datasets.remove(dataset);
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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

	public List<PublishState> getPublish() {
		return publish;
	}

	public void setPublish(List<PublishState> publish) {
		this.publish = publish;
	}
	
	public PublishState getPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {		
			for (PublishState s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			publish = new ArrayList<>();
		}
		
		PublishState s = new PublishState();
		s.setPublishState(DatasetState.UNPUBLISHED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		publish.add(s);
		
		return s;	
	}
	
	public PublishState checkPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {
			for (PublishState s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}
	
	public synchronized void removePublishState(PublishState ps) {
		if (publish != null) {
			publish.remove(ps);
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
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			index = new ArrayList<>();
		}
		
		IndexState s = new IndexState();
		s.setDatabaseConfigurationId(databaseConfigurationId);
		index.add(s);
		
		return s;	
	}
	
	public IndexState checkIndexState(ObjectId databaseConfigurationId) {
		if (index != null) {
			for (IndexState s : index) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
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
		for (ResourceOption ro : links) {
			if (ro.getType() == type) {
				return ro;
			}
		}
		
		return null;
	}

	public ResourceOption getOptionsByType(ResourceOptionType type) {
		for (ResourceOption ro : options) {
			if (ro.getType() == type) {
				return ro;
			}
		}
		
		return null;
	}
	
	public VirtuosoConfiguration getPublishVirtuosoConfiguration(Collection<VirtuosoConfiguration> virtuosoConfigurations) {
		
		for (VirtuosoConfiguration vc : virtuosoConfigurations) { // currently support only one publication site    	
			if (checkPublishState(vc.getId()) != null) {
				return vc;
			}
		}
		
		return null;
	}
	
	public PublishState getCurrentPublishState(Collection<VirtuosoConfiguration> virtuosoConfigurations) {
		for (VirtuosoConfiguration vc : virtuosoConfigurations) {
			PublishState ps = checkPublishState(vc.getId());
			if (ps != null) {
				return ps;
			}
		}
		
		return null;
	}
}

