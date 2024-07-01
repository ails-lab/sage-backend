package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.constants.type.AnnotationValidationMode;


@Repository
public interface PagedAnnotationValidationPageRepository extends MongoRepository<PagedAnnotationValidationPage, String> {

	Optional<PagedAnnotationValidationPage> findById(ObjectId pavpId);
	
   Optional<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndPage(ObjectId pavId, AnnotationValidationMode mode, int page);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndMode(ObjectId pavId, AnnotationValidationMode mode);
   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndIsAssignedAndPageGreaterThanOrderByPageAsc(ObjectId pavId, AnnotationValidationMode mode, boolean isAssigned, int page);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeOrderByPageDesc(ObjectId pavId, AnnotationValidationMode mode);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndIsAssignedAndValidatedCountAndPageGreaterThan(ObjectId pavId, AnnotationValidationMode mode, boolean isAssigned, int validatedCount, int page);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndPageGreaterThanOrderByPageAsc(ObjectId pavId, AnnotationValidationMode mode, int page);

   void deleteByPagedAnnotationValidationId(ObjectId pavId);
}


