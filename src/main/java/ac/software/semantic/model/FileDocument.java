package ac.software.semantic.model;

import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.DatasetContained;
import ac.software.semantic.model.base.GroupedDocument;
import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.base.OrderedDocument;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.model.state.PublishState;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "FileDocuments")
public class FileDocument extends MappingExecutePublishDocument<FileExecuteState, FilePublishState> implements DatasetContained, DatedDocument, OrderedDocument, GroupedDocument {
   
   @Id
   private ObjectId id;

   @JsonIgnore
   private ObjectId databaseId;
   
   private ObjectId userId;
   
   @Indexed
   private ObjectId datasetId;
   
   private String datasetUuid;
   
   private String name;
   private String description;
   
   private String url;
   
   private String uuid;
   
   private Date createdAt;
   private Date updatedAt;

   private boolean active;
   
   private int order;
   
   private int group;
   
   private FileDocument() {
   }

   public FileDocument(Dataset dataset) {
	   this();
	   
	   this.uuid = UUID.randomUUID().toString();
	   
	   this.databaseId = dataset.getDatabaseId();
	   
	   this.datasetId = dataset.getId();
	   this.datasetUuid = dataset.getUuid();
   }

   public ObjectId getId() {
       return id;
   }
 
   public String getName() {
       return name;
   }
   
   public void setName(String name) {
       this.name = name;
   }

	@Override
	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}


	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}
	
	public ProcessStateContainer<FilePublishState> getCurrentPublishState(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) {
			FilePublishState ps = checkPublishState(vc.getId());
			if (ps != null) {
				return new ProcessStateContainer<FilePublishState>(ps, vc);
			}
		}
		
		return null;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	@Override
	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
		
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
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

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
}