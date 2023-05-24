package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.MappingPublishState;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "EmbedderDocuments")
public class EmbedderDocument extends MappingExecutePublishDocument<MappingPublishState> implements ServiceDocument {
   
   @Id
   @JsonIgnore
   private ObjectId id;

   @JsonIgnore
   private ObjectId userId;
   
   @JsonIgnore
   private String datasetUuid;

   private String uuid;
   
   private ObjectId databaseId;
   
   private ClassIndexElement element; 
   
   private String embedder;
   
   private String variant;
   
   private String onClass;
   
   private List<String> keys;
   
   private List<IndexKeyMetadata> keysMetadata;
	
   private Date updatedAt;
   
   public EmbedderDocument() {
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

	@Override
	public String getIdentifier() {
		return getEmbedder();
	}

	@Override
	public DataServiceType getType() {
		return DataServiceType.EMBEDDER;
	}


	public List<String> getKeys() {
		return keys;
	}


	public void setKeys(List<String> keys) {
		this.keys = keys;
	}
	
	public Map<Integer, IndexKeyMetadata> getKeyMetadataMap() {
		Map<Integer, IndexKeyMetadata> res = new HashMap<>();
		
		if (keysMetadata != null) {
			for (IndexKeyMetadata ikm : keysMetadata) {
				res.put(ikm.getIndex(), ikm);
			}
		}
		
		return res;
	}


	public ObjectId getDatabaseId() {
		return databaseId;
	}


	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

}