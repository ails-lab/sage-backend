package ac.software.semantic.model;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.base.DatasetContained;
import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.base.ServiceDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.AnnotatorPublishState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.service.SideSpecificationDocument;
import ac.software.semantic.vocs.SEMRVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "ClustererDocuments")
public class ClustererDocument extends MappingExecutePublishDocument<MappingExecuteState, AnnotatorPublishState> 
								implements ServiceDocument, 
										   DatasetContained, 
										   DatedDocument, 
										   SideSpecificationDocument, 
										   ParametricDocument, 
										   IdentifiableDocument 
										   {
   
   @Id
   @JsonIgnore
   private ObjectId id;

   @JsonIgnore
   private ObjectId databaseId;

   @JsonIgnore
   private ObjectId userId;
   
   @Indexed
   private ObjectId datasetId;
   
   @JsonIgnore
   private String datasetUuid;
   
   private String uuid;
   
   private String name;
   private String identifier;
   
   private List<DataServiceParameterValue> parameters;
   
   private String clusterer;     //system annotator identifier
   private ObjectId clustererId; //user annotator
   
//   private List<PreprocessInstruction> preprocess;
   
//   private List<PreprocessInstruction> definedColumns;
//   private List<ConditionInstruction> mapConditions;
   
//   private List<ControlProperty> controlProperties;
//   private ClassIndexElement controlElement;
//   
//   private List<IndexKeyMetadata> controlKeysMetadata;
   
   private List<SchemaSelector> controls;
   
   private Date createdAt;
   private Date updatedAt;
   
   private List<String> annotatorTags;
   
//	private ClassIndexElement element;

	private String onClass;
	
	private String variant;
	
//	private List<IndexKeyMetadata> keysMetadata;
	
   private ClustererDocument() {
	   super();
   }

   public ClustererDocument(Dataset dataset) {
	   this();
	   
	   this.uuid = UUID.randomUUID().toString();
	   
	   this.databaseId = dataset.getDatabaseId();
		  
	   this.datasetId = dataset.getId();
	   this.datasetUuid = dataset.getUuid();
   }
   
   public ObjectId getId() {
       return id;
   }
   
   @Override
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

	public String getTripleStoreGraph(SEMRVocabulary resourceVocabulary, boolean separate) {
		return resourceVocabulary.getClustererAsResource(this).toString();
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public List<DataServiceParameterValue> getParameters() {
		return parameters;
	}
//	
//	public List<DataServiceParameterValue> getShowedParameters(boolean owner, List<DataServiceParameter> paramDefs) {
//		if (parameters == null) {
//			return parameters;
//		} else {
//			List<DataServiceParameterValue> res = new ArrayList<>();
//			
//			loop:
//			for (DataServiceParameterValue p : parameters) {
//				if (paramDefs == null || owner) {
//					res.add(p);
//				} else {
//					for (DataServiceParameter pd : paramDefs) {
//						if (pd.getName().equals(p.getName())) {
//							if (!pd.isHidden()) {
//								res.add(p);
//							} else {
//								DataServiceParameterValue np = new DataServiceParameterValue();
//								np.setName(p.getName());
//								np.setValue("**********");
//								
//								res.add(np);
//							}
//							continue loop;
//						}
//					}
//					res.add(p);
//				}
//			}
//			
//			return res;
//		}
//	}
//
	public void setParameters(List<DataServiceParameterValue> parameters) {
		this.parameters = parameters;
	}
//
//	public List<PreprocessInstruction> getPreprocess() {
//		return preprocess;
//	}
//
//	public void setPreprocess(List<PreprocessInstruction> preprocess) {
//		this.preprocess = preprocess;
//	}


	public Date getUpdatedAt() {
		return updatedAt;
	}


	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}


//	@Override
//	public DataServiceType getType() {
//		return DataServiceType.CLUSTERER;
//	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	@Override
	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

//	public List<String> getTags() {
//		return tags;
//	}
//
//	public void setTags(List<String> tags) {
//		if (tags != null) {
//			for (int i = 0; i < tags.size(); i++) {
//				if (tags.get(i).trim().length() == 0) {
//					tags.remove(i);
//					i--;
//				}
//			}
//			
//			if (tags.size() == 0) {
//				tags = null;
//			}
//		}
//		
//		this.tags = tags;
//	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public Resource asResource(SEMRVocabulary resourceVocabulary) {
		return resourceVocabulary.getClustererAsResource(this);
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

//	public ClassIndexElement getElement() {
//		return element;
//	}
//
//	public void setElement(ClassIndexElement element) {
//		this.element = element;
//	}

//	public List<String> getKeys() {
//		return keys;
//	}
//
//	public void setKeys(List<String> keys) {
//		this.keys = keys;
//	}

//	public List<PreprocessInstruction> getDefinedColumns() {
//		return definedColumns;
//	}
//
//	public void setDefinedColumns(List<PreprocessInstruction> definedColumns) {
//		this.definedColumns = definedColumns;
//	}
//
//	public List<ConditionInstruction> getMapConditions() {
//		return mapConditions;
//	}
//
//	public void setMapConditions(List<ConditionInstruction> mapConditions) {
//		this.mapConditions = mapConditions;
//	}

//	public List<ControlProperty> getControlProperties() {
//		return controlProperties;
//	}
//
//	public void setControlProperties(List<ControlProperty> controlProperties) {
//		this.controlProperties = controlProperties;
//	}

	public static Map<Integer, IndexKeyMetadata> getKeyMetadataMap(List<IndexKeyMetadata> keysMetadata) {
		Map<Integer, IndexKeyMetadata> res = new HashMap<>();
		
		if (keysMetadata != null) {
			for (IndexKeyMetadata ikm : keysMetadata) {
				res.put(ikm.getIndex(), ikm);
			}
		}
		
		return res;
	}

//	public void setKeysMetadata(List<IndexKeyMetadata> ikm) {
//		this.keysMetadata = ikm;
//	}
//
//	public List<IndexKeyMetadata> getKeysMetadata() {
//		return this.keysMetadata;
//	}
//
//	public ObjectId getAnnotatorId() {
//		return annotatorId;
//	}
//
//	public void setAnnotatorId(ObjectId annotatorId) {
//		this.annotatorId = annotatorId;
//	}
//
//	public List<String> getKeys() {
//		List<String> keys = new ArrayList<>();
//		for (IndexKeyMetadata ikm : keysMetadata) {
//			keys.add(ikm.getName());
//		}
//
//		return keys;
//	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	
	@Override
	public String getIdentifier(IdentifierType type) {
		return identifier;
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		this.identifier = identifier;
	}

	public List<String> getAnnotatorTags() {
		return annotatorTags;
	}

	public void setAnnotatorTags(List<String> annotatorTags) {
		this.annotatorTags = annotatorTags;
	}

	public String getClusterer() {
		return clusterer;
	}

	public void setClusterer(String clusterer) {
		this.clusterer = clusterer;
	}

	public ObjectId getClustererId() {
		return clustererId;
	}

	public void setClustererId(ObjectId clustererId) {
		this.clustererId = clustererId;
	}

	@Override
	public String getVariant() {
		return variant;
	}


	public void setVariant(String variant) {
		this.variant = variant;
	}

	@Override
	public String getIdentity() {
		return getClusterer();
	}

	@Override
	public DataServiceType getType() {
		return DataServiceType.CLUSTERER;
	}

	public List<SchemaSelector> getControls() {
		return controls;
	}

	public void setControls(List<SchemaSelector> controls) {
		this.controls = controls;
	}
}