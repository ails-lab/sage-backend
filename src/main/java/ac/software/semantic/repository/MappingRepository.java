package ac.software.semantic.repository;


import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.MappingDocument;

@Repository
public interface MappingRepository extends MongoRepository<MappingDocument, String> {

   List<MappingDocument> findByUserId(ObjectId userId);
   
//   List<MappingDocument> findByIdAndUserId(ObjectId id, ObjectId userId);
   
   List<MappingDocument> findByDatasetIdAndUserId(ObjectId datasetId, ObjectId userId);
   
   Optional<MappingDocument> findById(ObjectId Id, ObjectId userId);
   
   Optional<MappingDocument> findByIdAndUserId(ObjectId Id, ObjectId userId);

   List<MappingDocument> findByUserIdAndDatasetIdAndType(ObjectId id, ObjectId datasetId, String type);

}


