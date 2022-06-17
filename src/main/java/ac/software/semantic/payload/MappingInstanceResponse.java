package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.ParameterBinding;

public class MappingInstanceResponse {

	   private String id;
	   
	   private MappingState executeState;
	   private DatasetState publishState;
	   
	   private Date executeStartedAt;
	   private Date executeCompletedAt;

	   private Date publishStartedAt;
	   private Date publishCompletedAt;
	   
	   private List<ParameterBinding> binding;
	   
	   private int count;
	   
//	   private String uuid;
	   
	   public MappingInstanceResponse() {  
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
			this.publishState = publishState;
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

		public int getCount() {
			return count;
		}

		public void setCount(int count) {
			this.count = count;
		}

}
