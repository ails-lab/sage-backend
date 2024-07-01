package ac.software.semantic.repository.core;


import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.repository.DocumentRepository;

@Repository
public interface AnnotationEditGroupRepository extends DocumentRepository<AnnotationEditGroup> {

//	List<AnnotationEditGroup> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, String[] onProperty, String asProperty, ObjectId userId);
	Optional<AnnotationEditGroup> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, List<String> onProperty, String asProperty, ObjectId userId);
	Optional<AnnotationEditGroup> findByDatasetUuidAndOnClassAndAsPropertyAndUserId(String datasetUuid, String onClass, String asProperty, ObjectId userId);
	
	Optional<AnnotationEditGroup> findByDatasetIdAndOnPropertyAndTagAndUserId(ObjectId datasetId, List<String> onProperty, String tag, ObjectId userId);
	Optional<AnnotationEditGroup> findByDatasetIdAndOnPropertyAndTagExistsAndUserId(ObjectId datasetId, List<String> onProperty, boolean tag, ObjectId userId);
	
//	Optional<AnnotationEditGroup> findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagAndUserId(ObjectId datasetId, String onClass, List<String> keys, String sparqlClause, String tag, ObjectId userId);
//	Optional<AnnotationEditGroup> findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndTagExistsAndUserId(ObjectId datasetId, String onClass, List<String> keys, String sparqlClause, boolean tag, ObjectId userId);
	Optional<AnnotationEditGroup> findByDatasetIdAndOnClassAndKeysAndTagAndUserId(ObjectId datasetId, String onClass, List<String> keys, String tag, ObjectId userId);
	Optional<AnnotationEditGroup> findByDatasetIdAndOnClassAndKeysAndTagExistsAndUserId(ObjectId datasetId, String onClass, List<String> keys, boolean tag, ObjectId userId);
	
//	List<AnnotationEditGroup> findByDatasetIdAndOnClassAndKeysAndSparqlClauseAndAnnotatorIdAndUserId(ObjectId datasetId, String onClass, List<String> keys, String sparqlClause, ObjectId annotatorId, ObjectId userId);
	List<AnnotationEditGroup> findByDatasetIdAndOnClassAndKeysAndAnnotatorIdAndUserId(ObjectId datasetId, String onClass, List<String> keys, ObjectId annotatorId, ObjectId userId);
	List<AnnotationEditGroup> findByDatasetIdAndOnClassAndAnnotatorIdAndUserId(ObjectId datasetId, String onClass, ObjectId annotatorId, ObjectId userId);

	Optional<AnnotationEditGroup> findByDatasetUuidAndOnPropertyAndAsProperty(String datasetUuid, List<String> onProperty, String asProperty);

	List<AnnotationEditGroup> findByDatasetUuidAndOnProperty(String datasetUuid, List<String> onProperty);

	List<AnnotationEditGroup> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
	Page<AnnotationEditGroup> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId, Pageable page);
	
	List<AnnotationEditGroup> findByDatasetIdInAndUserId(List<ObjectId> datasetId, ObjectId userId);
	Page<AnnotationEditGroup> findByDatasetIdInAndUserId(List<ObjectId> datasetId, ObjectId userId, Pageable page);
	
	List<AnnotationEditGroup> findByDatasetUuidAndUserId(String datasetUuid, ObjectId userId);
	
	List<AnnotationEditGroup> findByDatasetUuid(String datasetUuid);
	
	List<AnnotationEditGroup> findByDatasetId(ObjectId datasetId);
	
	List<AnnotationEditGroup> findByDatasetUuidAndAutoexportable(String datasetUuid, boolean autoexportable);

    void deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, String[] onProperty, String asProperty, ObjectId userId);
    
    void deleteByDatasetIdAndOnPropertyAndTagAndUserId(ObjectId datasetId, String[] onProperty, String tag, ObjectId userId);
    
    List<AnnotationEditGroup> findByAnnotatorId(ObjectId annotatorId);
    
    void deleteById(ObjectId id);

}


