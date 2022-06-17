package ac.software.semantic.payload;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;

public class PagedAnnotationValidationPageResponse {
	@Id
	private String id;

	private String pagedAnnotationValidationId;
	private String annotationEditGroupId;
	
	private AnnotationValidationRequest mode;
	private int page;

	private int annotationsCount;

	private int addedCount;
	private int validatedCount;
	private int unvalidatedCount;
	
	public PagedAnnotationValidationPageResponse() {
	}

	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}

	public String getPagedAnnotationValidationId() {
		return pagedAnnotationValidationId;
	}

	public void setPagedAnnotationValidationId(String pagedAnnotationValidationId) {
		this.pagedAnnotationValidationId = pagedAnnotationValidationId;
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

	public String getAnnotationEditGroupId() {
		return annotationEditGroupId;
	}

	public void setAnnotationEditGroupId(String annotationEditGroupId) {
		this.annotationEditGroupId = annotationEditGroupId;
	}

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


}
