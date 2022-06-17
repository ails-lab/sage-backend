package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Document(collection = "AnnotationEdits")
public class AnnotationEdit {
   @Id
   private ObjectId id;

   @JsonIgnore
   private ObjectId userId; //should be deleted

   // SHOULD BE DELETED <---
   private String datasetUuid;
   private List<String> onProperty;
   private String propertyValue;
   private String asProperty;
   // --->
   
   private AnnotationEditValue onValue;
   
   private String annotationValue;
   
   private AnnotationEditType editType;
   
   private ObjectId annotationEditGroupId;
   
   private ObjectId pagedAnnotationValidationId;
//   private ObjectId pagedAnnotationValidationPageId;
   
   private int start;
   private int end;

   private List<ObjectId> acceptedByUserId;
   private List<ObjectId> rejectedByUserId;
   private List<ObjectId> addedByUserId;
   
   public AnnotationEdit() { 
	   acceptedByUserId = new ArrayList<>();
	   rejectedByUserId = new ArrayList<>();
	   addedByUserId = new ArrayList<>();
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

	public String getPropertyValue() {
		return propertyValue;
	}

	public void setPropertyValue(String propertyValue) {
		this.propertyValue = propertyValue;
	}

	public AnnotationEditType getEditType() {
		return editType;
	}

	public void setEditType(AnnotationEditType editType) {
		this.editType = editType;
	}

	public String getAnnotationValue() {
		return annotationValue;
	}

	public void setAnnotationValue(String annotationValue) {
		this.annotationValue = annotationValue;
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

	public ObjectId getAnnotationEditGroupId() {
		return annotationEditGroupId;
	}

	public void setAnnotationEditGroupId(ObjectId annotationEditGroupId) {
		this.annotationEditGroupId = annotationEditGroupId;
	}

	public AnnotationEditValue getOnValue() {
		return onValue;
	}

	public void setOnValue(AnnotationEditValue value) {
		this.onValue = value;
	}

	public ObjectId getPagedAnnotationValidationId() {
		return pagedAnnotationValidationId;
	}

	public void setPagedAnnotationValidationId(ObjectId pagedAnnotationValidationId) {
		this.pagedAnnotationValidationId = pagedAnnotationValidationId;
	}

//	public ObjectId getPagedAnnotationValidationPageId() {
//		return pagedAnnotationValidationPageId;
//	}
//
//	public void setPagedAnnotationValidationPageId(ObjectId pagedAnnotationValidationPageId) {
//		this.pagedAnnotationValidationPageId = pagedAnnotationValidationPageId;
//	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	public List<ObjectId> getAcceptedByUserId() {
		return acceptedByUserId;
	}

	public void setAcceptedByUserId(List<ObjectId> acceptedByUserId) {
		this.acceptedByUserId = acceptedByUserId;
	}

	public List<ObjectId> getRejectedByUserId() {
		return rejectedByUserId;
	}

	public void setRejectedByUserId(List<ObjectId> rejectedByUserId) {
		this.rejectedByUserId = rejectedByUserId;
	}

	public List<ObjectId> getAddedByUserId() {
		return addedByUserId;
	}

	public void setAddedByUserId(List<ObjectId> addedByUserId) {
		this.addedByUserId = addedByUserId;
	}

}
