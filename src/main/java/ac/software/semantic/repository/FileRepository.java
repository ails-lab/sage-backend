package ac.software.semantic.repository;


import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.MappingDocument;

@Repository
public interface FileRepository extends MongoRepository<FileDocument, String> {

   List<FileDocument> findByUserId(ObjectId userId);
   
   Optional<FileDocument> findByIdAndUserId(ObjectId id, ObjectId userId);
   
//   List<FileDocument> findByDatasetIdAndUserId(ObjectId datasetId, ObjectId userId);

   @Query(value = "{ 'datasetId' : ?0, 'execute.databaseConfigurationId' : ?1, 'userId' : ?2 }")
   List<FileDocument> findByDatasetIdAndFileSystemConfigurationIdAndUserId(ObjectId datasetId, ObjectId fscId, ObjectId userId);
   
   Optional<FileDocument> findById(ObjectId Id);
   
   

}


