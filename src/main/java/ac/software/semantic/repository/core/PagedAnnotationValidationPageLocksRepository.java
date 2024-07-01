package ac.software.semantic.repository.core;

import ac.software.semantic.model.PagedAnnotationValidationPageLocks;
import ac.software.semantic.model.constants.type.AnnotationValidationMode;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface PagedAnnotationValidationPageLocksRepository extends MongoRepository<PagedAnnotationValidationPageLocks, String> {

    Optional<PagedAnnotationValidationPageLocks> findById(ObjectId id);

    Optional<PagedAnnotationValidationPageLocks> findByPagedAnnotationValidationIdAndPageAndMode(ObjectId pavId, int page, AnnotationValidationMode mode);

    Optional<PagedAnnotationValidationPageLocks> findByPagedAnnotationValidationIdAndPageAndModeAndUserId(ObjectId pavId, int page, AnnotationValidationMode mode, ObjectId userId);

    Optional<PagedAnnotationValidationPageLocks> findByUserIdAndDatabaseId(ObjectId userId, ObjectId databaseId);
    
    List<PagedAnnotationValidationPageLocks> findByAnnotationEditGroupId(ObjectId aegId);

    List<PagedAnnotationValidationPageLocks> findByCreatedAtLessThan(Date date);
}
