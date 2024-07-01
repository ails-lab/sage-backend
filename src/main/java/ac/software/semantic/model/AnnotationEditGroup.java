package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "AnnotationEditGroups")
public class AnnotationEditGroup implements EnclosingDocument {

	@Id
	private ObjectId id;

	@JsonIgnore
	private ObjectId databaseId;

	private String uuid;

	@JsonIgnore
	private ObjectId userId;

	@Indexed
	private ObjectId datasetId;
	
	private String datasetUuid;

	private List<String> onProperty;
	private String onClass;
	private List<String> keys;
	
	private String asProperty;

	private Date lastPublicationStateChange; // last time an annotator was published/unpublished

	private Boolean autoexportable;
	
	private Date createdAt;
	private Date updatedAt;
	
	private String tag;
//	private String sparqlClause;
	
	private List<ObjectId> annotatorId;

	private AnnotationEditGroup() {
	}

	public AnnotationEditGroup(Dataset dataset) {
	   this();
	   
	   this.uuid = UUID.randomUUID().toString();
	   
	   this.databaseId = dataset.getDatabaseId();
		  
	   this.datasetId = dataset.getId();
	   this.datasetUuid = dataset.getUuid();
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
	
	public String getTripleStoreGraph(SEMRVocabulary resourceVocabulary) {
		if (asProperty != null) {
			return asProperty;
		} else {
			return resourceVocabulary.getDatasetAnnotationsAsResource(datasetUuid).toString();
		}
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
		for (int i = 0; i < path.size(); i++) {
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

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
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

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public List<ObjectId> getAnnotatorId() {
		return annotatorId;
	}

	public void setAnnotatorId(List<ObjectId> annotatorId) {
		this.annotatorId = annotatorId;
	}
	
	public void addAnnotatorId(ObjectId id) {
		if (annotatorId == null) {
			annotatorId = new ArrayList<>();
		}
		
		if (!annotatorId.contains(id)) {
			annotatorId.add(id);
		}
	}

	public void removeAnnotatorId(ObjectId id) {
		if (annotatorId != null) {
			annotatorId.remove(id);
			
			if (annotatorId.size() == 0) {
				annotatorId = null;
			}
		}
		
		
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

	public List<String> getKeys() {
		return keys;
	}

	public void setKeys(List<String> keys) {
		this.keys = keys;
	}

//	public String getSparqlClause() {
//		return sparqlClause;
//	}
//
//	public void setSparqlClause(String sparqlClause) {
//		this.sparqlClause = sparqlClause;
//	}
}
