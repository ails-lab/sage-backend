package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.repository.DocumentRepository;

@Repository
public interface FilterAnnotationValidationRepository extends DocumentRepository<FilterAnnotationValidation> {

   List<FilterAnnotationValidation> findByAnnotationEditGroupId(ObjectId aegId);
   
   List<FilterAnnotationValidation> findByDatasetUuid(String datasetUuid);
   
   void deleteById(ObjectId Id);
   

}


