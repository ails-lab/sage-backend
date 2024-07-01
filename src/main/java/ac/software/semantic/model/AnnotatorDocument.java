package ac.software.semantic.model;

import java.util.ArrayList;
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
import ac.software.semantic.model.base.GroupedDocument;
import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.base.OrderedDocument;
import ac.software.semantic.model.base.ServiceDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.AnnotatorPublishState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.service.SideSpecificationDocument;
import ac.software.semantic.vocs.SEMRVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "AnnotatorDocuments")
public class AnnotatorDocument extends MappingExecutePublishDocument<MappingExecuteState, AnnotatorPublishState> implements ServiceDocument, DatasetContained, DatedDocument, SideSpecificationDocument, ParametricDocument, IdentifiableDocument, OrderedDocument, GroupedDocument {
   
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
   
   private List<String> onProperty; // deprecated
   
   private String asProperty;
   private String annotator;     //system annotator identifier
   private ObjectId annotatorId; //user annotator
   
   private String variant;
   
   private String thesaurus; // deprecated
   private ObjectId thesaurusId; 
   
   private List<DataServiceParameterValue> parameters;
   
   private List<PreprocessInstruction> preprocess;
   
   private List<PreprocessInstruction> definedColumns;
   private List<ConditionInstruction> mapConditions;
   
   private ObjectId annotatorEditGroupId; // legacy
   
//   private List<ControlProperty> controlProperties;
   private SchemaSelector control;
   private List<String> bodyProperties;
   
   private Date createdAt;
   private Date updatedAt;
   
   private String defaultTarget;
   
   private List<String> tags;
   
	private String onClass;

//	private ClassIndexElement element;
//	private List<IndexKeyMetadata> keysMetadata;
	private SchemaSelector structure;
	
	private int order;
	private int group;
   
   private AnnotatorDocument() {
	   super();
   }

   public AnnotatorDocument(Dataset dataset) {
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

	public String getAnnotator() {
		return annotator;
	}

	public void setAnnotator(String annotator) {
		this.annotator = annotator;
	}

	public String getAsProperty() {
		return asProperty;
	}
	
	public String getTripleStoreGraph(SEMRVocabulary resourceVocabulary, boolean separate) {
		if (separate) {
			return resourceVocabulary.getAnnotatorAsResource(this).toString();
		}
		
		if (asProperty != null) {
			return asProperty;
		} else {
			return resourceVocabulary.getDatasetAnnotationsAsResource(datasetUuid).toString();
		}
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
		if (preprocess != null && preprocess.size() > 0) {
			this.preprocess = preprocess;
		} else {
			this.preprocess = null;
		}
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
	public String getIdentity() {
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

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		if (tags != null) {
			for (int i = 0; i < tags.size(); i++) {
				if (tags.get(i).trim().length() == 0) {
					tags.remove(i);
					i--;
				}
			}
			
			if (tags.size() == 0) {
				tags = null;
			}
		}
		
		this.tags = tags;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public Resource asResource(SEMRVocabulary resourceVocabulary) {
		return resourceVocabulary.getAnnotatorAsResource(this);
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

	public List<PreprocessInstruction> getDefinedColumns() {
		return definedColumns;
	}

	public void setDefinedColumns(List<PreprocessInstruction> definedColumns) {
		this.definedColumns = definedColumns;
	}

	public List<ConditionInstruction> getMapConditions() {
		return mapConditions;
	}

	public void setMapConditions(List<ConditionInstruction> mapConditions) {
		this.mapConditions = mapConditions;
	}

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

	public ObjectId getAnnotatorId() {
		return annotatorId;
	}

	public void setAnnotatorId(ObjectId annotatorId) {
		this.annotatorId = annotatorId;
	}

//	public List<String> getKeys() {
//		List<String> keys = new ArrayList<>();
//		for (IndexKeyMetadata ikm : keysMetadata) {
//			keys.add(ikm.getName());
//		}
//
//		return keys;
//	}

	public ObjectId getThesaurusId() {
		return thesaurusId;
	}

	public void setThesaurusId(ObjectId thesaurusId) {
		this.thesaurusId = thesaurusId;
	}

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

	public List<String> getBodyProperties() {
		return bodyProperties;
	}

	public void setBodyProperties(List<String> bodyProperties) {
		if (bodyProperties != null && bodyProperties.size() > 0) {
			this.bodyProperties = bodyProperties;
		} else {
			this.bodyProperties = null;
		}
	}

	@Override
	public int getOrder() {
		return order;
	}

	@Override
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public int getGroup() {
		return group;
	}

	@Override
	public void setGroup(int group) {
		this.group = group;
		
	}

	public SchemaSelector getControl() {
		return control;
	}

	public void setControl(SchemaSelector control) {
		this.control = control;
	}

	public SchemaSelector getStructure() {
		return structure;
	}

	public void setStructure(SchemaSelector structure) {
		this.structure = structure;
	}
}