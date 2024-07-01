package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import ac.software.semantic.model.base.CreatableDocument;
import ac.software.semantic.model.base.GroupedDocument;
import ac.software.semantic.model.base.OrderedDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.state.IndexState;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "IndexDocuments")
public class IndexDocument implements CreatableDocument<IndexState>, SpecificationDocument, OrderedDocument, DatedDocument, GroupedDocument {

	@Id
	@JsonIgnore
	private ObjectId id;
	
	private String name;

	private String uuid;

	@JsonIgnore
	private ObjectId databaseId;

	@JsonIgnore
	private ObjectId userId;
	
	@JsonIgnore	
	private ObjectId datasetId;

	@JsonIgnore
	private String datasetUuid;

	private ObjectId indexStructureId;
	
	private ObjectId elasticConfigurationId;

	private Date createdAt;
	private Date updatedAt;
	
	private List<IndexState> create;
	
	private int order;
	private int group;
	
//	@JsonProperty("default")
//	@Field("default")
//	private boolean idefault;
	
	private IndexDocument() {
		super();
	}
	
	public IndexDocument(Database database) {
		this();
		
		this.databaseId = database.getId();
	}
	
	public IndexDocument(Dataset dataset) {
		this();
		
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = dataset.getDatabaseId();
		   
		this.datasetId = dataset.getId();
		this.datasetUuid = dataset.getUuid();
	}
	

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public ObjectId getIndexStructureId() {
		return indexStructureId;
	}

	public void setIndexStructureId(ObjectId indexStructureId) {
		this.indexStructureId = indexStructureId;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public ObjectId getElasticConfigurationId() {
		return elasticConfigurationId;
	}

	public void setElasticConfigurationId(ObjectId elasticConfigurationId) {
		this.elasticConfigurationId = elasticConfigurationId;
	}

	@Override
	public List<IndexState> getCreate() {
		return create;
	}

	@Override
	public void setCreate(List<IndexState> create) {
		this.create = create;
	}

	@Override
	public IndexState getCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId) {
		if (create != null) {
			for (IndexState s : create) {
				if (s.getElasticConfigurationId().equals(elasticConfigurationId)) {
					return s;
				}
			}
		} else {
			create = new ArrayList<>();
		}
		
		IndexState s = new IndexState();
		s.setCreateState(CreatingState.NOT_CREATED);
		s.setElasticConfigurationId(elasticConfigurationId);
		create.add(s);
		
		return s;
	}

	@Override
	public IndexState checkCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId) {
		if (create != null) {		
			for (IndexState s : create) {
				if (s.getElasticConfigurationId().equals(elasticConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}
	
	@Override
	public void deleteCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId) {
		if (create != null) {
			for (int i = 0; i < create.size(); i++) {
				if (create.get(i).getElasticConfigurationId().equals(elasticConfigurationId)) {
					create.remove(i);
					break;
				}
			}
		
			if (create.size() == 0) {
				create = null;
			}
		}
	}

//	public boolean getIdefault() {
//		return idefault;
//	}
//
//	public void setIdefault(boolean idefault) {
//		this.idefault = idefault;
//	}


	@Override
	public int getOrder() {
		return order;
	}

	@Override
	public void setOrder(int order) {
		this.order = order;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	
	@Override
	public int getGroup() {
		return group;
	}

	@Override
	public void setGroup(int group) {
		this.group = group;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}