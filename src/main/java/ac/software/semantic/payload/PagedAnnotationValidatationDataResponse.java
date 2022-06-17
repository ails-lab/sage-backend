package ac.software.semantic.payload;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;

public class PagedAnnotationValidatationDataResponse {

	private String id;
	
	private AnnotationValidationRequest mode;
	
	private List<ValueAnnotation> data;
	private int totalPages;
	private int currentPage;
	
	private String pagedAnnotationValidationId;
	private String pagedAnnotationValidationPageId;
	private String errorMessage;
	private String lockId;
	private String filter;

	public PagedAnnotationValidatationDataResponse() {
		
	}

	public PagedAnnotationValidatationDataResponse(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public List<ValueAnnotation> getData() {
		return data;
	}

	public void setData(List<ValueAnnotation> data) {
		this.data = data;
	}

	public String getLockId() {
		return lockId;
	}

	public void setLockId(String lockId) {
		this.lockId = lockId;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public int getTotalPages() {
		return totalPages;
	}

	public void setTotalPages(int totalPages) {
		this.totalPages = totalPages;
	}

	public int getCurrentPage() {
		return currentPage;
	}

	public void setCurrentPage(int currentPage) {
		this.currentPage = currentPage;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public AnnotationValidationRequest getMode() {
		return mode;
	}

	public void setMode(AnnotationValidationRequest mode) {
		this.mode = mode;
	}

	public String getPagedAnnotationValidationId() {
		return pagedAnnotationValidationId;
	}

	public void setPagedAnnotationValidationId(String pagedAnnotationValidationId) {
		this.pagedAnnotationValidationId = pagedAnnotationValidationId;
	}

	public String getPagedAnnotationValidationPageId() {
		return pagedAnnotationValidationPageId;
	}

	public void setPagedAnnotationValidationPageId(String pagedAnnotationValidationPageId) {
		this.pagedAnnotationValidationPageId = pagedAnnotationValidationPageId;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
