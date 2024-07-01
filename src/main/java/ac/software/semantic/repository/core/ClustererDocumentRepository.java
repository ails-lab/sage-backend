package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ClustererDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomAnnotatorRepository;
import ac.software.semantic.repository.core.custom.CustomClustererRepository;

@Repository
public interface ClustererDocumentRepository extends DocumentRepository<ClustererDocument>, CustomClustererRepository {

   List<ClustererDocument> findByUserId(ObjectId userId);
   
//   List<ClustererDocument> findByIdIn(List<ObjectId> userId);

   Optional<ClustererDocument> findByIdAndUserId(ObjectId Id, ObjectId userId);
   
//   Optional<ClustererDocument> findByDatasetIdAndIdentifier(ObjectId Id, String identifier);
//   List<ClustererDocument> findByDatasetIdAndTags(ObjectId Id, String tag);
   
   Optional<ClustererDocument> findByUuid(String uuid);

   List<ClustererDocument> findByDatasetUuid(String datasetUuid);
   
   List<ClustererDocument> findByDatabaseId(Object databaseId);
   
}


