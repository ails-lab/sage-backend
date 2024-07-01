package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomAnnotatorRepository;

@Repository
public interface AnnotatorDocumentRepository extends DocumentRepository<AnnotatorDocument>, CustomAnnotatorRepository {

   List<AnnotatorDocument> findByUserId(ObjectId userId);
   
   List<AnnotatorDocument> findByIdIn(List<ObjectId> userId);

   Optional<AnnotatorDocument> findByIdAndUserId(ObjectId Id, ObjectId userId);
   
   Optional<AnnotatorDocument> findByDatasetIdAndIdentifier(ObjectId Id, String identifier);
   
   List<AnnotatorDocument> findByDatasetIdAndTags(ObjectId Id, String tag);
   
   Optional<AnnotatorDocument> findByUuid(String uuid);

   List<AnnotatorDocument> findByDatasetUuid(String datasetUuid);
   
   List<AnnotatorDocument> findByDatabaseId(Object databaseId);
   
//   @Query(value = "{ 'datasetId' : :#{#rp.getDataset().getId()}, 'userId' : ?1 }")
//   List<AnnotatorDocument> findByRepositoryParameterAndUserId(@Param("rp") RepositoryParameter rp, ObjectId userId);
   
//   List<AnnotatorDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
//   Page<AnnotatorDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId, Pageable page);
   
//   List<AnnotatorDocument> findByDatasetIdAndUserId(ObjectId datasetId, ObjectId userId);
//   Page<AnnotatorDocument> findByDatasetIdAndUserId(ObjectId datasetId, ObjectId userId, Pageable page);
   
//   List<AnnotatorDocument> findByDatasetUuidAndUserId(String datasetUuid, ObjectId userId);
   
//   List<AnnotatorDocument> findByDatasetUuidAndOnPropertyAndUserId(String datasetUuid, String[] onProperty, ObjectId userId);
   
   List<AnnotatorDocument> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, String[] onProperty, String asProperty, ObjectId userId);
   
   List<AnnotatorDocument> findByDatasetIdAndOnPropertyAndTagsAndUserId(ObjectId datasetId, String[] onProperty, String tag, ObjectId userId);
   
   List<AnnotatorDocument> findByAnnotatorEditGroupId(ObjectId aegId);
   
   List<AnnotatorDocument> findByAnnotatorId(ObjectId annotatorId);
   
}


