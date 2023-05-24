package ac.software.semantic.model;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.constants.IndexingState;


//@Document(collection = "IndexDocuments")
//public class IndexDocument {
//   
//   @Id
//   @JsonIgnore
//   private ObjectId id;
//
//   @JsonIgnore
//   private ObjectId userId;
//   
//   @JsonIgnore
//   private String datasetUuid;
//
//   @JsonIgnore
//   private ObjectId datasetId;
//
//   private String uuid;
//   
////   private List<List<String>> onProperties;
////   private ObjectId indexStructureId;
//   
//   private IndexingState indexState;
//   
//   private Date indexStartedAt;
//   private Date indexCompletedAt;
//   
//   private String host;
//   
//   public IndexDocument() {
//   }
//   
////   public IndexDocument(ObjectId userId, ObjectId datasetId, String datasetUuid, String uuid, List<List<String>> onProperties, String host) {
////       this.userId = userId;
////       this.datasetId = datasetId;
////       this.datasetUuid = datasetUuid;
////       this.uuid = uuid;
////
////       this.onProperties = onProperties;
////
////       this.indexState = IndexingState.NOT_INDEXED;
////       
////       this.host = host;
////   }
//   
//
//   public ObjectId getId() {
//       return id;
//   }
//   
//
//	public String getDatasetUuid() {
//		return datasetUuid;
//	}
//
//	public void setDatasetUuid(String datasetUuid) {
//		this.datasetUuid = datasetUuid;
//	}
//
//	public Date getIndexStartedAt() {
//		return indexStartedAt;
//	}
//
//	public void setIndexStartedAt(Date startedAt) {
//		this.indexStartedAt = startedAt;
//	}
//
//	public Date getIndexCompletedAt() {
//		return indexCompletedAt;
//	}
//
//	public void setIndexCompletedAt(Date completedAt) {
//		this.indexCompletedAt = completedAt;
//	}
//
//	public ObjectId getUserId() {
//		return userId;
//	}
//
//	public void setUserId(ObjectId userId) {
//		this.userId = userId;
//	}
//
//
////	public List<List<String>> getOnProperties() {
////		return onProperties;
////	}
////	
////	public void setOnProperties(List<List<String>> onProperties) {
////		this.onProperties = onProperties;
////	}
//
//	public String getUuid() {
//		return uuid;
//	}
//
//	public void setUuid(String uuid) {
//		this.uuid = uuid;
//	}
//
//	public IndexingState getIndexState() {
//		return indexState;
//	}
//
//	public void setIndexState(IndexingState indexState) {
//		this.indexState = indexState;
//	}
//
//	public String getHost() {
//		return host;
//	}
//
//	public void setHost(String host) {
//		this.host = host;
//	}
//
//	public ObjectId getDatasetId() {
//		return datasetId;
//	}
//
//	public void setDatasetId(ObjectId datasetId) {
//		this.datasetId = datasetId;
//	}
//
//
//}