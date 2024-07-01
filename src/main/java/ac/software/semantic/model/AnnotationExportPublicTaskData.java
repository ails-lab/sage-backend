package ac.software.semantic.model;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnnotationExportPublicTaskData {

	private List<ObjectId> annotatorDocumentId;
	private List<ObjectId> pagedAnnotationValidationId;
	private List<ObjectId> filterAnnotationValidationId;
	
	private Date lastUpdatedAt;

	public List<ObjectId> getAnnotatorDocumentId() {
		return annotatorDocumentId;
	}

	public void setAnnotatorDocumentId(List<ObjectId> annotatorDocumentId) {
		this.annotatorDocumentId = annotatorDocumentId;
	}

	public List<ObjectId> getFilterAnnotationValidationId() {
		return filterAnnotationValidationId;
	}

	public void setFilterAnnotationValidationId(List<ObjectId> filterAnnotationValidationId) {
		this.filterAnnotationValidationId = filterAnnotationValidationId;
	}

	public List<ObjectId> getPagedAnnotationValidationId() {
		return pagedAnnotationValidationId;
	}

	public void setPagedAnnotationValidationId(List<ObjectId> pagedAnnotationValidationId) {
		this.pagedAnnotationValidationId = pagedAnnotationValidationId;
	}

	public Date getLastUpdatedAt() {
		return lastUpdatedAt;
	}

	public void setLastUpdatedAt(Date lastUpdatedAt) {
		this.lastUpdatedAt = lastUpdatedAt;
	}
}
