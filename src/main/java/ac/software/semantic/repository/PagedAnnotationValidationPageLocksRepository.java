package ac.software.semantic.repository;

import ac.software.semantic.controller.APIAnnotationEditGroupController;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.PagedAnnotationValidationPageLocks;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface PagedAnnotationValidationPageLocksRepository extends MongoRepository<PagedAnnotationValidationPageLocks, String> {

    Optional<PagedAnnotationValidationPageLocks> findById(ObjectId id);

    Optional<PagedAnnotationValidationPageLocks> findByPagedAnnotationValidationIdAndPageAndMode(ObjectId pavId, int page, APIAnnotationEditGroupController.AnnotationValidationRequest mode);

    Optional<PagedAnnotationValidationPageLocks> findByPagedAnnotationValidationIdAndPageAndModeAndUserId(ObjectId pavId, int page, APIAnnotationEditGroupController.AnnotationValidationRequest mode, ObjectId userId);

    Optional<PagedAnnotationValidationPageLocks> findByUserId(ObjectId userId);

    List<PagedAnnotationValidationPageLocks> findByCreatedAtLessThan(Date date);
}
