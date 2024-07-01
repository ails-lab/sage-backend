package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomMappingRepository;

@Repository
public interface MappingDocumentRepository extends DocumentRepository<MappingDocument>, CustomMappingRepository {

   Optional<MappingDocument> findByDatasetIdAndUuid(ObjectId datasetId, String uuid);
   
   Optional<MappingDocument> findByUuid(String uuid);
   
   Optional<MappingDocument> findByDatasetIdAndIdentifier(ObjectId datasetId, String identifier);
   List<MappingDocument> findByDatasetIdAndIdentifierIn(ObjectId datasetId, String[] identifier);
   
   List<MappingDocument> findByD2rmlId(ObjectId d2rmlId);
   
   Optional<MappingDocument> findByDatasetIdAndName(ObjectId datasetId, String name);
   
//   List<MappingDocument> findByUserId(ObjectId userId);
   
//   List<MappingDocument> findByDatabaseId(ObjectId databaseId);

   default List<MappingDocument> findByDatasetId(ObjectId datasetId) {
	   return findByDatasetIdOrderByOrderAsc(datasetId);
   }
   
//   List<MappingDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
//   Page<MappingDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId, Pageable page);
   
   default List<MappingDocument> findByDatasetIdAndUserId(ObjectId datasetId, ObjectId userId) {
	   return findByDatasetIdAndUserIdOrderByOrderAsc(datasetId, userId);
   }

//   default Page<MappingDocument> findByDatasetIdAndUserId(ObjectId datasetId, ObjectId userId, Pageable page) {
//	   return findByDatasetIdAndUserIdOrderByOrderAsc(datasetId, userId, page);
//   }

   List<MappingDocument> findByDatasetIdOrderByOrderAsc(ObjectId datasetId);
   
   List<MappingDocument> findByDatasetIdAndUserIdOrderByOrderAsc(ObjectId datasetId, ObjectId userId);
//   Page<MappingDocument> findByDatasetIdAndUserIdOrderByOrderAsc(ObjectId datasetId, ObjectId userId, Pageable page);
   
//   List<MappingDocument> findByUserIdAndDatasetIdAndType(ObjectId id, ObjectId datasetId, String type);

}


