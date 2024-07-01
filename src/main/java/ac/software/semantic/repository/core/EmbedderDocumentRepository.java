package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.repository.DocumentRepository;

@Repository
public interface EmbedderDocumentRepository extends DocumentRepository<EmbedderDocument> {

   List<EmbedderDocument> findByUserId(ObjectId userId);

   Optional<EmbedderDocument> findByUuid(String uuid);

   List<EmbedderDocument> findByDatasetUuid(String datasetUuid);
   
   List<EmbedderDocument> findByDatasetUuidAndOnClass(String datasetUuid, String onClass);
   
   List<EmbedderDocument> findByDatasetUuidAndUserId(String datasetUuid, ObjectId userId);
   
//   @Query(value = "{ 'datasetId' : :#{#rp.getDataset().getId()}, 'userId' : ?1 }")
//   List<EmbedderDocument> findByRepositoryParameterAndUserId(@Param("rp") RepositoryParameter rp, ObjectId userId);
   
   List<EmbedderDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
   Page<EmbedderDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId, Pageable page);
   
   List<EmbedderDocument> findByDatasetIdInAndUserId(List<ObjectId> datasetId, ObjectId userId);
   Page<EmbedderDocument> findByDatasetIdInAndUserId(List<ObjectId> datasetId, ObjectId userId, Pageable page);

   
   
}


