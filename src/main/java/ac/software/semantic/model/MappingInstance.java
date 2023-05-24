package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.MappingPublishState;

public class MappingInstance extends MappingExecutePublishDocument<MappingPublishState> {
	   @Id
	   private ObjectId id;

	   private List<ParameterBinding> binding;
	   
	   private List<String> dataFiles;
	   
//	   private String uuid;
	   
	   public MappingInstance() {
		   id = new ObjectId();
//		   execute = new ArrayList<>();
//		   publish = new ArrayList<>();
		   
		   binding = new ArrayList<>();
	   }
	   
	   public MappingInstance(List<String> parameters) {   
		   binding = new ArrayList<>();
		   for (String s : parameters) {
			   binding.add(new ParameterBinding(s, ""));
		   }
		   
	   }

	   public ObjectId getId() {
	       return id;
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
		public List<String> getDataFiles() {
			return dataFiles;
		}

		public void setDataFiles(List<String> dataFiles) {
			this.dataFiles = dataFiles;
		}
		
		public void addDataFile(String dataFile) {
			if (this.dataFiles == null) {
				this.dataFiles = new ArrayList<>();
			}
			
			this.dataFiles.add(dataFile);
		}
		
		
		public void removeDataFile(String dataFile) {
			if (dataFiles != null) {
				dataFiles.remove(dataFile);
				
				if (dataFiles.size() == 0) {
					dataFiles = null;
				}
			}
		}		
}
