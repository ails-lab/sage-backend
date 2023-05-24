package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;


@Document(collection = "VocabularizerDocuments")
public class VocabularizerDocument {
   
   @Id
   @JsonIgnore
   private ObjectId id;

   @JsonIgnore
   private ObjectId userId;
   
   @JsonIgnore
   private String datasetUuid;

   private String uuid;
   
   private List<String> onProperty;
   private String separator;
   
   
//   private String executeUuid;
   
   private List<MappingExecuteState> execute;
   private List<MappingPublishState> publish;
   private List<IndexState> index;

   
   private String name;
   
   public VocabularizerDocument() {
   }
   
   public VocabularizerDocument(ObjectId userId, String datasetUuid, String uuid, List<String> onProperty, String name, String separator) {
       this.userId = userId;
       this.datasetUuid = datasetUuid;
       this.uuid = uuid;

       this.onProperty = onProperty;
       
       this.name = name;
       this.separator = separator;
       
       this.execute = new ArrayList<>();
       this.publish = new ArrayList<>();
       this.index = new ArrayList<>();
   }
   

   public ObjectId getId() {
       return id;
   }
   

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	public List<String> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<String> onProperty) {
		this.onProperty = onProperty;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<MappingPublishState> getPublish() {
		return publish;
	}

	public void setPublish(List<MappingPublishState> publish) {
		this.publish = publish;
	}
	
	public MappingPublishState getPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {
			for (MappingPublishState s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			publish = new ArrayList<>();
		}
		
		MappingPublishState s = new MappingPublishState();
		s.setPublishState(DatasetState.UNPUBLISHED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		publish.add(s);
		
		return s;
	}

	public MappingPublishState checkPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {		
			for (MappingPublishState s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
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

	public List<MappingExecuteState> getExecute() {
		return execute;
	}

	public void setExecute(List<MappingExecuteState> execute) {
		this.execute = execute;
	}	
	
	public MappingExecuteState getExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {
			for (MappingExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			execute = new ArrayList<>();
		}
		
		MappingExecuteState s = new MappingExecuteState();
		s.setExecuteState(MappingState.NOT_EXECUTED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		execute.add(s);
		
		return s;
	}

	public MappingExecuteState checkExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {		
			for (MappingExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}
}