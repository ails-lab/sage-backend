package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.FilterAnnotationValidation;

@Repository
public interface FilterAnnotationValidationRepository extends MongoRepository<FilterAnnotationValidation, String> {

   List<FilterAnnotationValidation> findByAnnotationEditGroupId(ObjectId aegId);
   
   Optional<FilterAnnotationValidation> findById(ObjectId Id);
   
   List<FilterAnnotationValidation> findByDatasetUuid(String datasetUuid);
   
   void deleteById(ObjectId Id);
   

}


