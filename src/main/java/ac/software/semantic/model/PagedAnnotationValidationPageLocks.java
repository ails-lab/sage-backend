package ac.software.semantic.model;

import ac.software.semantic.controller.APIAnnotationEditGroupController;
import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


import java.util.Date;

@Document(collection = "PagedAnnotationValidationPageLocks")
@CompoundIndexes({
        @CompoundIndex(name="unique_comp_idx", def = "{'pagedAnnotationValidationId':1, 'annotationEditGroupId': 1, 'page': 1, 'mode': 1}", unique = true)
})
public class PagedAnnotationValidationPageLocks {
    @Id
    private ObjectId id;
    private ObjectId pagedAnnotationValidationId;
    private ObjectId annotationEditGroupId;
    private int page;
    private AnnotationValidationRequest mode;
    private Date createdAt;
    private ObjectId userId;

    public PagedAnnotationValidationPageLocks() {
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

    public AnnotationValidationRequest getMode() {
        return mode;
    }

    public void setMode(AnnotationValidationRequest mode) {
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
}
