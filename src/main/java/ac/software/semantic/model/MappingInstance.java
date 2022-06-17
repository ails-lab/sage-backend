package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

public class MappingInstance {
	   @Id
	   private ObjectId id;
	   
	   private List<ExecuteState> execute;
	   private List<PublishState> publish;

	   private List<ParameterBinding> binding;
	   
//	   private String uuid;
	   
	   public MappingInstance() {
		   id = new ObjectId();
		   execute = new ArrayList<>();
		   setPublish(new ArrayList<>());
		   
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
	

		public List<ParameterBinding> getBinding() {
			return binding;
		}

		public void setBinding(List<ParameterBinding> binding) {
			this.binding = binding;
		}

		public List<PublishState> getPublish() {
			return publish;
		}

		public void setPublish(List<PublishState> publish) {
			this.publish = publish;
		}

		public PublishState getPublishState(ObjectId databaseConfigurationId) {
			if (publish != null) {
				for (PublishState s : publish) {
					if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
						return s;
					}
				}
			} else {
				publish = new ArrayList<>();
			}
			
			PublishState s = new PublishState();
			s.setPublishState(DatasetState.UNPUBLISHED);
			s.setDatabaseConfigurationId(databaseConfigurationId);
			publish.add(s);
			
			return s;
		}

		public PublishState checkPublishState(ObjectId databaseConfigurationId) {
			if (publish != null) {
				for (PublishState s : publish) {
					if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
						return s;
					}
				}
			}
			
			return null;
		}

		public List<ExecuteState> getExecute() {
			return execute;
		}

		public void setExecute(List<ExecuteState> execute) {
			this.execute = execute;
		}	
		
		public ExecuteState getExecuteState(ObjectId databaseConfigurationId) {
			if (execute != null) {
				for (ExecuteState s : execute) {
					if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
						return s;
					}
				}
			} else {
				execute = new ArrayList<>();
			}
			
			ExecuteState s = new ExecuteState();
			s.setExecuteState(MappingState.NOT_EXECUTED);
			s.setDatabaseConfigurationId(databaseConfigurationId);
			execute.add(s);
			
			return s;
		}

		public ExecuteState checkExecuteState(ObjectId databaseConfigurationId) {
			if (execute != null) {		
				for (ExecuteState s : execute) {
					if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
						return s;
					}
				}
			}
			
			return null;
		}		
		
		public void deleteExecuteState(ObjectId databaseConfigurationId) {
			if (execute != null) {
				for (int i = 0; i < execute.size(); i++) {
					if (execute.get(i).getDatabaseConfigurationId().equals(databaseConfigurationId)) {
						execute.remove(i);
						break;
					}
				}
			} else {
				execute = new ArrayList<>();
			}
		}
		
		public synchronized void removePublishState(PublishState ps) {
			if (publish != null) {
				publish.remove(ps);
			} 
			
		}		
}
