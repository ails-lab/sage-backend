package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.state.MappingState;


public class IndexDocumentResponse {
   
   private String id;
   private String uuid;

   private List<List<String>> onProperties;
   
   private IndexingState indexState;
   
//   private String host;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date indexStartedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date indexCompletedAt;
   
   public IndexDocumentResponse() {
   }
   
   public String getId() {
       return id;
   }
   
	public void setId(String id) {
		this.id = id;
	}

	public Date getIndexStartedAt() {
		return indexStartedAt;
	}

	public void setIndexStartedAt(Date startedAt) {
		this.indexStartedAt = startedAt;
	}

	public Date getIndexCompletedAt() {
		return indexCompletedAt;
	}

	public void setIndexCompletedAt(Date completedAt) {
		this.indexCompletedAt = completedAt;
	}

	public List<List<String>> getOnProperties() {
		return onProperties;
	}

	public void setOnProperties(List<List<String>> onProperties) {
		this.onProperties = onProperties;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public IndexingState getIndexState() {
		return indexState;
	}

	public void setIndexState(IndexingState indexState) {
		this.indexState = indexState;
	}

//	public String getHost() {
//		return host;
//	}
//
//	public void setHost(String host) {
//		this.host = host;
//	}

}