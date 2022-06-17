package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.PagedAnnotationValidation;

@Repository
public interface PagedAnnotationValidationRepository extends MongoRepository<PagedAnnotationValidation, String> {

   List<PagedAnnotationValidation> findByAnnotationEditGroupId(ObjectId aegId);
   
   Optional<PagedAnnotationValidation> findById(ObjectId Id);
   
   List<PagedAnnotationValidation> findByDatasetUuid(String datasetUuid);
   
   void deleteById(ObjectId Id);
   

}


