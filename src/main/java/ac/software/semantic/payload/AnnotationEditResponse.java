package ac.software.semantic.payload;

import org.springframework.data.annotation.Id;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.controller.NegativeFilter;
import ac.software.semantic.model.AnnotationEditType;
import ac.software.semantic.model.AnnotationEditValue;

public class AnnotationEditResponse {
   @Id
   private String id;

//   private String datasetId;
   
//   private String onProperty;
//   private String propertyValue;
   private AnnotationEditValue propertyValue;
   
   private String annotationValue;
   
   private AnnotationEditType editType;
   
   private int start;
   private int end;
   
   private int count;
   
   public AnnotationEditResponse() {
   }

	public AnnotationEditResponse(RefractoredAnnotationEditDetails edit, AnnotationEditValue val) {
		this.id = edit.getId();
		this.propertyValue = val;
		this.annotationValue = edit.getAnnotationValue();
		this.editType = edit.getEditType();
		this.start = edit.getStart();
		this.end = edit.getEnd();
		this.count = edit.getCount();
	}

	public String getId() {
   		return id;
   	}

   	public void setId(String id) {
   		this.id = id;
   	}
   	
//	public String getDatasetId() {
//		return datasetId;
//	}
//
//	public void setDatasetId(String datasetId) {
//		this.datasetId = datasetId;
//	}
//
//	public String getOnProperty() {
//		return onProperty;
//	}
//
//	public void setPropertyUri(String onProperty) {
//		this.onProperty = onProperty;
//	}
//
//	public String getPropertyValue() {
//		return propertyValue;
//	}
//
//	public void setPropertyValue(String propertyValue) {
//		this.propertyValue = propertyValue;
//	}

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

	public AnnotationEditValue getPropertyValue() {
		return propertyValue;
	}

	public void setPropertyValue(AnnotationEditValue propertyValue) {
		this.propertyValue = propertyValue;
	}

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

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}


}
