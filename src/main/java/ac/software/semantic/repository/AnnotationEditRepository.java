package ac.software.semantic.repository;


import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditType;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.MappingDocument;

@Repository
public interface AnnotationEditRepository extends MongoRepository<AnnotationEdit, String> {

   List<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, List<String> onProperty, String asProperty, ObjectId userId);
   
   @Query(value = "{ 'datasetUuid' : ?0, 'onProperty' : ?1, 'asProperty' : ?2, 'userId' : ?3, 'onValue.lexicalForm' : ?4, 'onValue.language' : ?5, 'onValue.datatype' : ?6 }")
   List<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserIdAndLiteralValue(String datasetUuid, List<String> onProperty, String asProperty, ObjectId userId, String lexicalForm, String language, String datatype);

   @Query(value = "{ 'datasetUuid' : ?0, 'onProperty' : ?1, 'asProperty' : ?2, 'onValue.lexicalForm' : ?3, 'onValue.language' : ?4, 'onValue.datatype' : ?5, 'annotationValue' : ?6, 'start' : ?7, 'end' : ?8 }")
   Optional<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndLiteralValueAndAnnotationValueAndStartAndEnd(String datasetUuid, List<String> onProperty, String asProperty, String lexicalForm, String language, String datatype, String value, int start, int end);

   @Query(value = "{ 'annotationEditGroupId' : ?0,  'onValue.lexicalForm' : ?1, 'onValue.language' : ?2, 'onValue.datatype' : ?3, 'annotationValue' : ?4, 'start' : ?5, 'end' : ?6 }")
   Optional<AnnotationEdit> findByAnnotationEditGroupIdAndLiteralValueAndAnnotationValueAndStartAndEnd(ObjectId annotationEditGroupId, String lexicalForm, String language, String datatype, String value, int start, int end);

   @Query(value = "{ 'datasetUuid' : ?0, 'onProperty' : ?1, 'asProperty' : ?2, 'onValue.lexicalForm' : ?3, 'onValue.language' : ?4, 'onValue.datatype' : ?5,  'addedByUserId': { '$exists': 'true', '$not': {'$size': 0} }  }")
   List<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndLiteralValueAndAdded(String datasetUuid, List<String> onProperty, String asProperty, String lexicalForm, String language, String datatype);

   @Query(value = "{ 'annotationEditGroupId' : ?0, 'onValue.lexicalForm' : ?1, 'onValue.language' : ?2, 'onValue.datatype' : ?3,  'addedByUserId': { '$exists': 'true', '$not': {'$size': 0} }  }")
   List<AnnotationEdit> findByAnnotationEditGroupIdAndLiteralValueAndAdded(ObjectId annotationEditGroupId, String lexicalForm, String language, String datatype);

   @Query(value = "{ 'datasetUuid' : ?0, 'onProperty' : ?1, 'asProperty' : ?2, 'userId' : ?3, 'onValue.iri' : ?4 }")
   List<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserIdAndIriValue(String datasetUuid, List<String> onProperty, String asProperty, ObjectId userId, String iri);

   @Query(value = "{ 'datasetUuid' : ?0, 'onProperty' : ?1, 'asProperty' : ?2, 'onValue.iri' : ?3, 'annotationValue' : ?4, 'start' : ?5, 'end' : ?6 }")
   Optional<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndIriValueAndAnnotationValueAndStartAndEnd(String datasetUuid, List<String> onProperty, String asProperty, String iri, String value, int start, int end);

   @Query(value = "{ 'annotationEditGroupId' : ?0, 'onValue.iri' : ?1, 'annotationValue' : ?2, 'start' : ?3, 'end' : ?4 }")
   Optional<AnnotationEdit> findByAnnotationEditGroupIdAndIriValueAndAnnotationValueAndStartAndEnd(ObjectId annotationEditGroupId, String iri, String value, int start, int end);

   @Query(value = "{ 'datasetUuid' : ?0, 'onProperty' : ?1, 'asProperty' : ?2, 'onValue.iri' : ?3, 'addedByUserId': { '$exists': 'true', '$not': {'$size': 0} }  }")
   List<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndIriValueAndAdded(String datasetUuid, List<String> onProperty, String asProperty, String iri);

   @Query(value = "{ 'annotationEditGroupId' : ?0, 'onValue.iri' : ?1, 'addedByUserId': { '$exists': 'true', '$not': {'$size': 0} }  }")
   List<AnnotationEdit> findByAnnotationEditGroupIdAndIriValueAndAdded(ObjectId aegId, String iri);

   @Query(value = "{ 'annotationEditGroupId' : ?0, 'userId' : ?1, 'onValue.iri' : ?2, 'annotationValue' : ?3, 'editType' : ?4 }")
   List<AnnotationEdit> findByAnnotationEditGroupIdAndUserIdAndIriValueAndAnnotationValueAndEditType(ObjectId aegId, ObjectId userId, String iri, String annotationValue, AnnotationEditType type);

   @Query(value = "{ 'annotationEditGroupId' : ?0, 'userId' : ?1, 'onValue.lexicalForm' : ?2, 'onValue.language' : ?3, 'onValue.datatype' : ?4, 'annotationValue' : ?5, 'editType' : ?6 }")
   List<AnnotationEdit> findByAnnotationEditGroupIdAndUserIdAndLiteralValueAndAnnotationValueAndEditType(ObjectId aegId, ObjectId userId, String lexicalForm, String language, String datatype, String annotationValue, AnnotationEditType type);
   
   List<AnnotationEdit> findByDatasetUuidAndOnPropertyAndAsPropertyAndEditTypeAndUserId(String datasetUuid, List<String> onProperty, String asProperty, AnnotationEditType editType, ObjectId userId);
   
//   List<AnnotationEdit> findByDatasetUuidAndOnPropertyAndPropertyValueAndAsPropertyAndAnnotationValueAndUserId(String datasetUuid, List<String> onProperty, String propertyValue, String asProperty, String annotationValue, ObjectId userId);

   void deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, List<String> onProperty, String asProperty, ObjectId userId);

   
   Optional<AnnotationEdit> findById(ObjectId Id);
   
   List<AnnotationEdit> findByAnnotationEditGroupId(ObjectId aegId);

   List<AnnotationEdit> findByPagedAnnotationValidationId(ObjectId pavId);
   
   
//   List<AnnotationEdit> findByPagedAnnotationValidationIdAndAnnotationEditGroupId(ObjectId pavId, ObjectId aegId);
//   List<AnnotatorDocument> findByDatasetIdAndOnPropertyAndUserId(ObjectId datasetId, String onProperty, ObjectId userId);

}


