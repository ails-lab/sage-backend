package ac.software.semantic.model;

import ac.software.semantic.model.constants.type.AnnotationValidationMode;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Date;

@Document(collection = "PagedAnnotationValidationPageLocks")
@CompoundIndexes({
        @CompoundIndex(name="unique_comp_idx", def = "{'pagedAnnotationValidationId':1, 'annotationEditGroupId': 1, 'page': 1, 'mode': 1}", unique = true)
})
public class PagedAnnotationValidationPageLocks {
    @Id
    private ObjectId id;
    
	@JsonIgnore
	private ObjectId databaseId;
	
    private ObjectId pagedAnnotationValidationId;
    private ObjectId annotationEditGroupId;
    private int page;
    private AnnotationValidationMode mode;
    private Date createdAt;
    private ObjectId userId;

    public PagedAnnotationValidationPageLocks(ObjectId databaseId) {
    	this.databaseId = databaseId;
    }

    public ObjectId getPagedAnnotationValidationId() {
        return pagedAnnotationValidationId;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public void setPagedAnnotationValidationId(ObjectId pagedAnnotationValidationId) {
        this.pagedAnnotationValidationId = pagedAnnotationValidationId;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public AnnotationValidationMode getMode() {
        return mode;
    }

    public void setMode(AnnotationValidationMode mode) {
        this.mode = mode;
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

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}
}
