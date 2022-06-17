package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;

@Document(collection = "AnnotationEditGroups")
public class AnnotationEditGroup {
   @Id
   private ObjectId id;

   private String uuid;
   
   @JsonIgnore
   private ObjectId userId;

   private String datasetUuid;
   
   private List<String> onProperty;
   private String asProperty;
   
//   private List<ExecuteState> execute; // should be removed -> moved to corresponding annotationValidation
//   private List<PublishState> publish; // should be removed -> moved to corresponding annotationValidation
   
   public AnnotationEditGroup() {
   }
   
//   public AnnotationEditGroup(String datasetUuid, List<String> onProperty, String asProperty, ObjectId userId) {
//       this.datasetUuid = datasetUuid;
//       this.onProperty = onProperty;
//       this.asProperty = asProperty;
//       this.userId = userId;
//       
////       execute = new ArrayList<>();
////       publish = new ArrayList<>();
//   }

   	public ObjectId getId() {
   		return id;
   	}

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public List<String> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<String> onProperty) {
		this.onProperty = onProperty;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}


//	public List<PublishState> getPublish() {
//		return publish;
//	}
//
//	public void setPublish(List<PublishState> publish) {
//		this.publish = publish;
//	}
//	
//	public PublishState getPublishState(ObjectId databaseConfigurationId) {
//		if (publish != null) {
//			for (PublishState s : publish) {
//				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
//					return s;
//				}
//			}
//		} else {
//			publish = new ArrayList<>();
//		}
//		
//		PublishState s = new PublishState();
//		s.setPublishState(DatasetState.UNPUBLISHED);
//		s.setDatabaseConfigurationId(databaseConfigurationId);
//		publish.add(s);
//		
//		return s;	
//	}
//	
//	public PublishState checkPublishState(ObjectId databaseConfigurationId) {
//		if (publish != null) {
//			for (PublishState s : publish) {
//				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
//					return s;
//				}
//			}
//		}
//		
//		return null;
//	}	
//
//	public List<ExecuteState> getExecute() {
//		return execute;
//	}
//
//	public void setExecute(List<ExecuteState> execute) {
//		this.execute = execute;
//	}	
//	
//	public ExecuteState getExecuteState(ObjectId databaseConfigurationId) {
//		if (execute != null) {
//			for (ExecuteState s : execute) {
//				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
//					return s;
//				}
//			}
//		} else {
//			execute = new ArrayList<>();
//		}
//		
//		ExecuteState s = new ExecuteState();
//		s.setExecuteState(MappingState.NOT_EXECUTED);
//		s.setDatabaseConfigurationId(databaseConfigurationId);
//		execute.add(s);
//		
//		return s;
//	}
//
//	public ExecuteState checkExecuteState(ObjectId databaseConfigurationId) {
//		if (execute != null) {		
//			for (ExecuteState s : execute) {
//				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
//					return s;
//				}
//			}
//		}
//		
//		return null;
//	}

	public String getOnPropertyAsString() {
		return onPropertyListAsString(this.getOnProperty());
	}

	public static String onPropertyListAsString(List<String> path) {
		// TODO: check if in right order.
		String spath = "";
		for (int i = path.size() - 1; i >= 0; i--) {
			if (i < path.size() - 1) {
				spath += "/";
			}
			spath += "<" + path.get(i) + ">";
		}

		return spath;
	}
	
	public static String annotatorFilter(String var, List<String> annotatorUuids) {
		String annfilter = "";
		
		for (String uuid : annotatorUuids) {
			annfilter += "<" + SEMAVocabulary.getAnnotator(uuid).toString() + "> ";
		}
	
		if (annfilter.length() > 0) {
			annfilter = "?" + var + " <https://www.w3.org/ns/activitystreams#generator> ?generator . VALUES ?generator { " + annfilter + " } . ";
		}
		
		return annfilter;
	}
	
	public static String generatorFilter(String var, List<String> generatorUris) {
		String annfilter = "";
		
		for (String uri : generatorUris) {
			annfilter += uri + " ";
		}
	
		if (annfilter.length() > 0) {
			annfilter = "?" + var + " <https://www.w3.org/ns/activitystreams#generator> ?generator . VALUES ?generator { " + annfilter + " } . ";
		}
		
		return annfilter;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
}
