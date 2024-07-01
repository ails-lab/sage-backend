package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.repository.DocumentRepository;

@Repository
public interface PagedAnnotationValidationRepository extends DocumentRepository<PagedAnnotationValidation> {

   List<PagedAnnotationValidation> findByAnnotationEditGroupId(ObjectId aegId);
   
   List<PagedAnnotationValidation> findByDatasetUuid(String datasetUuid);
   
   List<PagedAnnotationValidation> findByDatasetId(ObjectId datasetId);
   
   void deleteById(ObjectId Id);
   

}


