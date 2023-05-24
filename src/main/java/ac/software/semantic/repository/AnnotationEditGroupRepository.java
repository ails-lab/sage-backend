package ac.software.semantic.repository;


import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotationEditGroup;

@Repository
public interface AnnotationEditGroupRepository extends MongoRepository<AnnotationEditGroup, String> {

	Optional<AnnotationEditGroup> findById(ObjectId id);
   
//	List<AnnotationEditGroup> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, String[] onProperty, String asProperty, ObjectId userId);
	Optional<AnnotationEditGroup> findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, List<String> onProperty, String asProperty, ObjectId userId);

	Optional<AnnotationEditGroup> findByDatasetUuidAndOnPropertyAndAsProperty(String datasetUuid, List<String> onProperty, String asProperty);

	List<AnnotationEditGroup> findByDatasetUuidAndOnProperty(String datasetUuid, List<String> onProperty);

	List<AnnotationEditGroup> findByDatasetUuidAndUserId(String datasetUuid, ObjectId userId);
	
	List<AnnotationEditGroup> findByDatasetUuid(String datasetUuid);
	
	List<AnnotationEditGroup> findByDatasetUuidAndAutoexportable(String datasetUuid, boolean autoexportable);

    void deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(String datasetUuid, String[] onProperty, String asProperty, ObjectId userId);

}


