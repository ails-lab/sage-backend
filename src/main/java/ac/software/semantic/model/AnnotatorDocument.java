package ac.software.semantic.model;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.state.AnnotatorPublishState;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "AnnotatorDocuments")
public class AnnotatorDocument extends MappingExecutePublishDocument<AnnotatorPublishState> implements ServiceDocument {
   
   @Id
   @JsonIgnore
   private ObjectId id;

   @JsonIgnore
   private ObjectId userId;
   
   @JsonIgnore
   private String datasetUuid;
   
   private ObjectId databaseId;

   private String uuid;
   
   private List<String> onProperty; // deprecated
   
   private String asProperty;
   private String annotator;
   
   private String variant;
   
   private String manualUuid;
   
   private String thesaurus;
   
   private List<DataServiceParameterValue> parameters;
   
   private List<PreprocessInstruction> preprocess;
   
   private ObjectId annotatorEditGroupId;
   
   private Date updatedAt;
   
   private String defaultTarget;
   
   public AnnotatorDocument() {
	   super();
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

	public List<PreprocessInstruction> getPreprocess() {
		return preprocess;
	}

	public void setPreprocess(List<PreprocessInstruction> preprocess) {
		this.preprocess = preprocess;
	}

	@Override
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


	public Date getUpdatedAt() {
		return updatedAt;
	}


	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}


	public String getDefaultTarget() {
		return defaultTarget;
	}


	public void setDefaultTarget(String defaultTarget) {
		this.defaultTarget = defaultTarget;
	}

	@Override
	public String getIdentifier() {
		return getAnnotator();
	}
	
	@Override
	public DataServiceType getType() {
		return DataServiceType.ANNOTATOR;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

}