package ac.software.semantic.model;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
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
   
   private Date lastPublicationStateChange; // last time an annotator was published/unpublished
   
   private Boolean autoexportable;
   
   public AnnotationEditGroup() {
   }

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
	
	public boolean isAutoexportable() {
		return autoexportable != null && autoexportable;
	}


	public static String onPropertyListAsString(List<String> path) {
		// TODO: legacy was inversed [apollonis]
		String spath = "";
//		for (int i = path.size() - 1; i >= 0; i--) {
//			if (i < path.size() - 1) {
//				spath += "/";
//			}
//			spath += "<" + path.get(i) + ">";
//		}
		
		boolean clazz = false;
		for (int i = 0 ; i < path.size(); i++) {
			if (!clazz && i < path.size() - 1) {
				spath += "/";
			}
			spath += "<" + path.get(i) + ">";
			
			if (path.get(i).equals(RDFSVocabulary.class.toString())) {
				clazz = true;
			} else {
				clazz = false;
			}
		}

		return spath;
	}
	
	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Date getLastPublicationStateChange() {
		return lastPublicationStateChange;
	}

	public void setLastPublicationStateChange(Date lastPublicationStateChange) {
		this.lastPublicationStateChange = lastPublicationStateChange;
	}

	public Boolean getAutoexportable() {
		return autoexportable;
	}

	public void setAutoexportable(Boolean autoexportable) {
		this.autoexportable = autoexportable;
	}

}
