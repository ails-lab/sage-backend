package ac.software.semantic.repository.core;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.ComparatorDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomComparatorRepository;

@Repository
public interface ComparatorDocumentRepository extends DocumentRepository<ComparatorDocument>, CustomComparatorRepository {

   Optional<ComparatorDocument> findById(ObjectId id);
   
   Optional<ComparatorDocument> findByIdentifierAndDatabaseId(String identifier, ObjectId databaseId);
   
//   Optional<ComparatorDocument> findByDatasetIdAndIdentifier(ObjectId datasetId, String identifier);
   
   
}


