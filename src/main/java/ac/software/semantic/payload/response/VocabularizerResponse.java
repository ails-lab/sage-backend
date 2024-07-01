//package ac.software.semantic.payload.response;
//
//import java.util.Date;
//import java.util.List;
//
//import com.fasterxml.jackson.annotation.JsonInclude;
//
//public class VocabularizerResponse implements Response, ExecuteResponse, PublishResponse {
//   
//   private String id;
//   private String uuid;
//
//   private List<String> onProperty;
//   private String name;
//   private String separator;
//   
//   private ResponseTaskObject executeState;
//   private ResponseTaskObject publishState;
//   
////   private IndexingState indexState;
//   
//   @JsonInclude(JsonInclude.Include.NON_NULL)
//   private Date indexStartedAt;
//   
//   @JsonInclude(JsonInclude.Include.NON_NULL)
//   private Date indexCompletedAt;
//   
//   public VocabularizerResponse() {
//   }
//   
//   public String getId() {
//       return id;
//   }
//   
//	public void setId(String id) {
//		this.id = id;
//	}
//	public String getSeparator() {
//		return separator;
//	}
//
//	public void setSeparator(String separator) {
//		this.separator = separator;
//	}
//
//	public List<String> getOnProperty() {
//		return onProperty;
//	}
//
//	public void setOnProperty(List<String> onProperty) {
//		this.onProperty = onProperty;
//	}
//
//	public String getUuid() {
//		return uuid;
//	}
//
//	public void setUuid(String uuid) {
//		this.uuid = uuid;
//	}
//
//	public String getName() {
//		return name;
//	}
//
//	public void setName(String name) {
//		this.name = name;
//	}
//
//	public ResponseTaskObject getExecuteState() {
//		return executeState;
//	}
//
//	public void setExecuteState(ResponseTaskObject executeState) {
//		this.executeState = executeState;
//	}
//
//	public ResponseTaskObject getPublishState() {
//		return publishState;
//	}
//
//	public void setPublishState(ResponseTaskObject publishState) {
//		this.publishState = publishState;
//	}
//}