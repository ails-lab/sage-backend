package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.state.MappingState;


public class VocabularizerResponse {
   
   private String id;
   private String uuid;

   private List<String> onProperty;
   private String name;
   private String separator;
   
   private MappingState executeState;
   private DatasetState publishState;
   
   private int count;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date executeStartedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date executeCompletedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)   
   private Date publishStartedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date publishCompletedAt;
   
   private IndexingState indexState;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date indexStartedAt;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private Date indexCompletedAt;
   
   public VocabularizerResponse() {
   }
   
   public String getId() {
       return id;
   }
   
	public void setId(String id) {
		this.id = id;
	}

	public Date getExecuteStartedAt() {
		return executeStartedAt;
	}

	public void setExecuteStartedAt(Date startedAt) {
		this.executeStartedAt = startedAt;
	}

	public Date getExecuteCompletedAt() {
		return executeCompletedAt;
	}

	public void setExecuteCompletedAt(Date completedAt) {
		this.executeCompletedAt = completedAt;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
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

	public Date getPublishStartedAt() {
		return publishStartedAt;
	}

	public void setPublishStartedAt(Date publishStartedAt) {
		this.publishStartedAt = publishStartedAt;
	}

	public Date getPublishCompletedAt() {
		return publishCompletedAt;
	}

	public void setPublishCompletedAt(Date publishCompletedAt) {
		this.publishCompletedAt = publishCompletedAt;
	}

	public MappingState getExecuteState() {
		return executeState;
	}

	public void setExecuteState(MappingState executeState) {
		this.executeState = executeState;
	}

	public DatasetState getPublishState() {
		return publishState;
	}

	public void setPublishState(DatasetState publishState) {
		if (publishState == null) {
			this.publishState = DatasetState.UNPUBLISHED;
		} else {
			this.publishState = publishState;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public IndexingState getIndexState() {
		return indexState;
	}

	public void setIndexState(IndexingState indexState) {
		if (indexState == null) {
			this.indexState = IndexingState.NOT_INDEXED;
		} else {
			this.indexState = indexState;
		}
	}

	public Date getIndexStartedAt() {
		return indexStartedAt;
	}

	public void setIndexStartedAt(Date indexStartedAt) {
		this.indexStartedAt = indexStartedAt;
	}

	public Date getIndexCompletedAt() {
		return indexCompletedAt;
	}

	public void setIndexCompletedAt(Date indexCompletedAt) {
		this.indexCompletedAt = indexCompletedAt;
	}		
	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}