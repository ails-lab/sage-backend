package ac.software.semantic.repository;


import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.MappingDocument;

@Repository
public interface AnnotatorDocumentRepository extends MongoRepository<AnnotatorDocument, String> {

	Optional<AnnotatorDocument> findById(ObjectId id);
	
   List<AnnotatorDocument> findByUserId(ObjectId userId);

   Optional<AnnotatorDocument> findByIdAndUserId(ObjectId Id, ObjectId userId);
   
   Optional<AnnotatorDocument> findByUuid(String uuid);

   List<AnnotatorDocument> findByDatasetUuid(String datasetUuid);
   
   List<AnnotatorDocument> findByDatasetUuidAndUserId(String datasetUuid, ObjectId userId);
   
   List<AnnotatorDocument> findByDatasetUuidAndOnPropertyAndUserId(String datasetUuid, String[] onProperty, ObjectId userId);
   
   List<AnnotatorDocument> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, String[] onProperty, String asProperty, ObjectId userId);
   
   List<AnnotatorDocument> findByAnnotatorEditGroupId(ObjectId aegId);
   
}


