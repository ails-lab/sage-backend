package ac.software.semantic.model;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.base.DatasetContained;
import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.base.ServiceDocument;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.service.SideSpecificationDocument;
import ac.software.semantic.vocs.SEMRVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "EmbedderDocuments")
public class EmbedderDocument extends MappingExecutePublishDocument<MappingExecuteState, MappingPublishState> implements ServiceDocument, DatasetContained, DatedDocument, SideSpecificationDocument, ParametricDocument {

	@Id
//	@JsonIgnore
	private ObjectId id;

//	@JsonIgnore
	private ObjectId databaseId;

//	@JsonIgnore
	private ObjectId userId;

	private ObjectId datasetId;
	
//	@JsonIgnore
	private String datasetUuid;

	private String uuid;

	private ClassIndexElement element;

	private String embedder;

	private String variant;

	private String onClass;

//	private List<String> keys;

	private List<DataServiceParameterValue> parameters;
	 
	private List<IndexKeyMetadata> keysMetadata;

	private Date updatedAt;
	private Date createdAt;

	private EmbedderDocument() {
		super();
	}
	
	public EmbedderDocument(Dataset dataset) {
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

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public ClassIndexElement getElement() {
		return element;
	}

	public void setElement(ClassIndexElement element) {
		this.element = element;
	}

	public String getEmbedder() {
		return embedder;
	}

	public void setEmbedder(String embedder) {
		this.embedder = embedder;
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

	@Override
	public String getVariant() {
		return variant;
	}

	public void setVariant(String variant) {
		this.variant = variant;
	}
	
	public String getTripleStoreGraph(SEMRVocabulary resourceVocabulary, boolean separate) {
		if (separate) {
			return resourceVocabulary.getEmbedderAsResource(this).toString();
		}
		
		return resourceVocabulary.getDatasetEmbeddingsAsResource(datasetUuid).toString();
	}

	@Override
	public String getIdentity() {
		return getEmbedder();
	}

	@Override
	public DataServiceType getType() {
		return DataServiceType.EMBEDDER;
	}

//	public List<String> getKeys() {
//		return keys;
//	}
//
//	public void setKeys(List<String> keys) {
//		this.keys = keys;
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

	@Override
	public Resource asResource(SEMRVocabulary resourceVocabulary) {
		return resourceVocabulary.getEmbedderAsResource(this);
	}

	public List<DataServiceParameterValue> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameterValue> parameters) {
		this.parameters = parameters;
	}

	public List<IndexKeyMetadata> getKeysMetadata() {
		return keysMetadata;
	}

	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
		this.keysMetadata = keysMetadata;
	}

}