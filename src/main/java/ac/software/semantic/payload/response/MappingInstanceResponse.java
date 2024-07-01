package ac.software.semantic.payload.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ParameterBinding;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MappingInstanceResponse implements Response, ExecutePublishResponse, ValidateResponse {

	   private String id;

	   private String uuid;
	   private String identifier;
	   
	   private List<ParameterBinding> binding;
	   
	   private boolean active;

	   private ResponseTaskObject executeState;
	   private ResponseTaskObject publishState;
	   private ResponseTaskObject validateState;

	   private Boolean publishedFromCurrentFileSystem;
	   private Boolean newExecution;
	   
	   private List<String> dataFiles;
	   
	   public MappingInstanceResponse() {  
	   }
	   
	   public boolean hasBinding() {
		   return binding != null && binding.size() > 0; 
	   }
	
		public List<ParameterBinding> getBinding() {
			return binding;
		}

		public void setBinding(List<ParameterBinding> binding) {
			this.binding = binding;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Boolean isNewExecution() {
			return newExecution;
		}

		public void setNewExecution(Boolean newExecution) {
			this.newExecution = newExecution;
		}

		public Boolean isPublishedFromCurrentFileSystem() {
			return publishedFromCurrentFileSystem;
		}

		public void setPublishedFromCurrentFileSystem(Boolean publishedFromCurrentFileSystem) {
			this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
		}

		public List<String> getDataFiles() {
			return dataFiles;
		}

		public void setDataFiles(List<String> dataFiles) {
			this.dataFiles = dataFiles;
		}

		public ResponseTaskObject getExecuteState() {
			return executeState;
		}

		public void setExecuteState(ResponseTaskObject executeState) {
			this.executeState = executeState;
		}

		public ResponseTaskObject getPublishState() {
			return publishState;
		}

		public void setPublishState(ResponseTaskObject publishState) {
			this.publishState = publishState;
		}

		public ResponseTaskObject getValidateState() {
			return validateState;
		}

		public void setValidateState(ResponseTaskObject validateState) {
			this.validateState = validateState;
		}

		public boolean isActive() {
			return active;
		}

		public void setActive(boolean active) {
			this.active = active;
		}

		public String getIdentifier() {
			return identifier;
		}

		public void setIdentifier(String identifier) {
			this.identifier = identifier;
		}

		public String getUuid() {
			return uuid;
		}

		public void setUuid(String uuid) {
			this.uuid = uuid;
		}

}
