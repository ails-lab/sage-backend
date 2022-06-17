package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;

@Document(collection = "PagedAnnotationValidationPage")
public class PagedAnnotationValidationPage {
	@Id
	private ObjectId id;

	private ObjectId pagedAnnotationValidationId;
	private ObjectId annotationEditGroupId;
	
	private AnnotationValidationRequest mode;
	private int page;

	private int annotationsCount;
	private int addedCount;

	// all the below refer to the annotationsCount not to addedCount
	private int validatedCount;
	private int unvalidatedCount;

	private int acceptedCount;
	private int rejectedCount;
	private int neutralCount;
	

	private boolean isAssigned;
	

	public PagedAnnotationValidationPage() {
	}

	public ObjectId getId() {
		return id;
	}

	public ObjectId getPagedAnnotationValidationId() {
		return pagedAnnotationValidationId;
	}

	public void setPagedAnnotationValidationId(ObjectId pagedAnnotationValidationId) {
		this.pagedAnnotationValidationId = pagedAnnotationValidationId;
	}

	public boolean isAssigned() {
		return isAssigned;
	}

	public void setAssigned(boolean assigned) {
		isAssigned = assigned;
	}

	public int getAnnotationsCount() {
		return annotationsCount;
	}

	public void setAnnotationsCount(int annotationsCount) {
		this.annotationsCount = annotationsCount;
	}

	public AnnotationValidationRequest getMode() {
		return mode;
	}

	public void setMode(AnnotationValidationRequest mode) {
		this.mode = mode;
	}

	public int getPage() {
		return page;
	}

	public void setPage(int page) {
		this.page = page;
	}

	public ObjectId getAnnotationEditGroupId() {
		return annotationEditGroupId;
	}

	public void setAnnotationEditGroupId(ObjectId annotationEditGroupId) {
		this.annotationEditGroupId = annotationEditGroupId;
	}

//	public int getAcceptedCount() {
//		return acceptedCount;
//	}
//
//	public void setAcceptedCount(int acceptedCount) {
//		this.acceptedCount = acceptedCount;
//	}
//
//	public int getRejectedCount() {
//		return rejectedCount;
//	}
//
//	public void setRejectedCount(int rejectedCount) {
//		this.rejectedCount = rejectedCount;
//	}

	public int getAddedCount() {
		return addedCount;
	}

	public void setAddedCount(int addedCount) {
		this.addedCount = addedCount;
	}

	public int getValidatedCount() {
		return validatedCount;
	}

	public void setValidatedCount(int validatedCount) {
		this.validatedCount = validatedCount;
	}

	public int getUnvalidatedCount() {
		return unvalidatedCount;
	}

	public void setUnvalidatedCount(int unvalidatedCount) {
		this.unvalidatedCount = unvalidatedCount;
	}

	public int getAcceptedCount() {
		return acceptedCount;
	}

	public void setAcceptedCount(int acceptedCount) {
		this.acceptedCount = acceptedCount;
	}

	public int getRejectedCount() {
		return rejectedCount;
	}

	public void setRejectedCount(int rejectedCount) {
		this.rejectedCount = rejectedCount;
	}

	public int getNeutralCount() {
		return neutralCount;
	}

	public void setNeutralCount(int neutralCount) {
		this.neutralCount = neutralCount;
	}


}
