package ac.software.semantic.payload.response;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IndexDocumentResponse implements Response, CreateResponse, MultiUserResponse {
   
   private String id;
   private String uuid;
   
   private String name;
   
   private String indexStructureId;
   private String indexStructureIdentifier;
   private String elasticConfiguration;

   private ResponseTaskObject createState;
   
	private Date createdAt;
	private Date updatedAt;
	
   private int order;
   
   private boolean ownedByUser;
   
//   @JsonProperty("default")
//   private boolean idefault;
   
   public IndexDocumentResponse() {
   }
   
   public String getId() {
       return id;
   }
   
	public void setId(String id) {
		this.id = id;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getIndexStructureIdentifier() {
		return indexStructureIdentifier;
	}

	public void setIndexStructureIdentifier(String indexStructureIdentifier) {
		this.indexStructureIdentifier = indexStructureIdentifier;
	}

	public String getElasticConfiguration() {
		return elasticConfiguration;
	}

	public void setElasticConfiguration(String elasticConfiguration) {
		this.elasticConfiguration = elasticConfiguration;
	}

	public String getIndexStructureId() {
		return indexStructureId;
	}

	public void setIndexStructureId(String indexStructureId) {
		this.indexStructureId = indexStructureId;
	}

//	public boolean isIdefault() {
//		return idefault;
//	}
//
//	public void setIdefault(boolean idefault) {
//		this.idefault = idefault;
//	}

	public ResponseTaskObject getCreateState() {
		return createState;
	}

	public void setCreateState(ResponseTaskObject createState) {
		this.createState = createState;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}


	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}	
}