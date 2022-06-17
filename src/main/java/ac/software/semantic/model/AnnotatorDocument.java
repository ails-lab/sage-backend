package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;


@Document(collection = "AnnotatorDocuments")
public class AnnotatorDocument {
   
   @Id
   @JsonIgnore
   private ObjectId id;

   @JsonIgnore
   private ObjectId userId;
   
   @JsonIgnore
   private String datasetUuid;

   private String uuid;
   
   private List<String> onProperty;
   private String asProperty;
   private String annotator;
   
   private String variant;
   
   private String manualUuid;
   
   private String thesaurus;
   
   private List<ExecuteState> execute;
   private List<PublishState> publish;
   
   private List<DataServiceParameterValue> parameters;
   
   private List<PreprocessInstruction> preprocess;
   
   private ObjectId annotatorEditGroupId;
   
   public AnnotatorDocument() {

       this.execute = new ArrayList<>();
       this.publish = new ArrayList<>();
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

	public String getAnnotator() {
		return annotator;
	}

	public void setAnnotator(String annotator) {
		this.annotator = annotator;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
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

	public String getThesaurus() {
		return thesaurus;
	}

	public void setThesaurus(String thesaurus) {
		this.thesaurus = thesaurus;
	}

	public String getManualUuid() {
		return manualUuid;
	}

	public void setManualUuid(String manualUuid) {
		this.manualUuid = manualUuid;
	}

	public List<DataServiceParameterValue> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameterValue> parameters) {
		this.parameters = parameters;
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
	
	public List<ExecuteState> getExecute() {
		return execute;
	}

	public void setExecute(List<ExecuteState> execute) {
		this.execute = execute;
	}	
	
	public ExecuteState getExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {
			for (ExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			execute = new ArrayList<>();
		}
		
		ExecuteState s = new ExecuteState();
		s.setExecuteState(MappingState.NOT_EXECUTED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		execute.add(s);
		
		return s;
	}

	public ExecuteState checkExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {		
			for (ExecuteState s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}


	public List<PreprocessInstruction> getPreprocess() {
		return preprocess;
	}


	public void setPreprocess(List<PreprocessInstruction> preprocess) {
		this.preprocess = preprocess;
	}


	public String getVariant() {
		return variant;
	}


	public void setVariant(String variant) {
		this.variant = variant;
	}


	public ObjectId getAnnotatorEditGroupId() {
		return annotatorEditGroupId;
	}


	public void setAnnotatorEditGroupId(ObjectId annotatorEditGroupId) {
		this.annotatorEditGroupId = annotatorEditGroupId;
	}


}