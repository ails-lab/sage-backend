package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.model.PagedAnnotationValidationPage;


@Repository
public interface PagedAnnotationValidationPageRepository extends MongoRepository<PagedAnnotationValidationPage, String> {

	Optional<PagedAnnotationValidationPage> findById(ObjectId pavpId);
	
   Optional<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndPage(ObjectId pavId, AnnotationValidationRequest mode, int page);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndMode(ObjectId pavId, AnnotationValidationRequest mode);
   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndIsAssignedAndPageGreaterThanOrderByPageAsc(ObjectId pavId, AnnotationValidationRequest mode, boolean isAssigned, int page);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeOrderByPageDesc(ObjectId pavId, AnnotationValidationRequest mode);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndIsAssignedAndValidatedCountAndPageGreaterThan(ObjectId pavId, AnnotationValidationRequest mode, boolean isAssigned, int validatedCount, int page);

   List<PagedAnnotationValidationPage> findByPagedAnnotationValidationIdAndModeAndPageGreaterThanOrderByPageAsc(ObjectId pavId, AnnotationValidationRequest mode, int page);

   void deleteByPagedAnnotationValidationId(ObjectId pavId);
}


