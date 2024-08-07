//package ac.software.semantic.repository.core;
//
//
//import java.util.List;
//import java.util.Optional;
//
//import org.bson.types.ObjectId;
//import org.springframework.data.mongodb.repository.MongoRepository;
//import org.springframework.stereotype.Repository;
//
//import ac.software.semantic.model.AnnotatorDocument;
//import ac.software.semantic.model.MappingDocument;
//import ac.software.semantic.model.VocabularizerDocument;
//
//@Repository
//public interface VocabularizerRepository extends MongoRepository<VocabularizerDocument, String> {
//
//   List<VocabularizerDocument> findByUserId(ObjectId userId);
//
//   Optional<VocabularizerDocument> findByIdAndUserId(ObjectId Id, ObjectId userId);
//
//   List<VocabularizerDocument> findByDatasetUuidAndUserId(String datasetUuid, ObjectId userId);
//
//}


