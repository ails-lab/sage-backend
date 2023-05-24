package ac.software.semantic.repository;


import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.MappingDocument;

@Repository
public interface EmbedderDocumentRepository extends MongoRepository<EmbedderDocument, String> {

	Optional<EmbedderDocument> findById(ObjectId id);
	
   List<EmbedderDocument> findByUserId(ObjectId userId);

   Optional<EmbedderDocument> findByIdAndUserId(ObjectId Id, ObjectId userId);
   
   Optional<EmbedderDocument> findByUuid(String uuid);

   List<EmbedderDocument> findByDatasetUuid(String datasetUuid);
   
   List<EmbedderDocument> findByDatasetUuidAndOnClass(String datasetUuid, String onClass);
   
   List<EmbedderDocument> findByDatasetUuidAndUserId(String datasetUuid, ObjectId userId);
   
   
}


