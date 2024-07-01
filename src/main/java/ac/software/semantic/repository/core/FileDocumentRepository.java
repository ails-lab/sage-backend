package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.FileDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomFileRepository;

@Repository
public interface FileDocumentRepository extends DocumentRepository<FileDocument>, CustomFileRepository {

   List<FileDocument> findByDatabaseId(ObjectId databaseId);
	
   List<FileDocument> findByUserId(ObjectId userId);
   
   List<FileDocument> findByDatasetId(ObjectId datasetId);

   @Query(value = "{ 'datasetId' : { $in: ?0 }, 'execute.databaseConfigurationId' : ?1, 'userId' : ?2 }", sort = "{order : 1}")
   List<FileDocument> findByDatasetIdInAndFileSystemConfigurationIdAndUserId(List<ObjectId> datasetId, ObjectId fscId, ObjectId userId);
//   @Query(value = "{ 'datasetId' : { $in: ?0 }, 'execute.databaseConfigurationId' : ?1, 'userId' : ?2 }", sort = "{order : 1}")
//   Page<FileDocument> findByDatasetIdInAndFileSystemConfigurationIdAndUserId(List<ObjectId> datasetId, ObjectId fscId, ObjectId userId, Pageable page);
//
//   @Query(value = "{ 'databaseId' : ?0, 'execute.databaseConfigurationId' : ?1, 'userId' : ?2 }")
//   List<FileDocument> findByDatabaseIdAndFileSystemConfigurationIdAndUserId(ObjectId databaseId, ObjectId fscId, ObjectId userId);
//   @Query(value = "{ 'databaseId' : ?0, 'execute.databaseConfigurationId' : ?1, 'userId' : ?2 }")
//   Page<FileDocument> findByDatabaseIdAndFileSystemConfigurationIdAndUserId(ObjectId databaseId, ObjectId fscId, ObjectId userId, Pageable page);

   Optional<FileDocument> findById(ObjectId Id);

   
}


