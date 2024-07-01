package ac.software.semantic.repository.core;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.TaskType;

@Repository
public interface TaskRepository extends MongoRepository<TaskDescription, String> {

	void deleteById(ObjectId id);
	
	Optional<TaskDescription> findById(ObjectId id);
	
	List<TaskDescription> findByDatabaseId(ObjectId databaseId);
	
	List<TaskDescription> findByDatabaseIdAndStateAndSystem(ObjectId databaseId, TaskState state, String system);
	
	List<TaskDescription> findByUserIdAndDatabaseIdAndSystemAndParentIdExistsOrderByCreateTimeDesc(ObjectId userId, ObjectId databaseId, String system, boolean parent);
	
	List<TaskDescription> findByUserIdAndDatabaseIdAndSystemAndParentIdExistsAndCreateTimeGreaterThanOrderByCreateTimeDesc(ObjectId userId, ObjectId databaseId, String system, boolean parent, Date date);

	@Query("{'datasetId' : ?0, 'tripleStoreConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByDatasetIdAndTripleStoreConfigurationId(ObjectId datasetId, ObjectId tripleStoreConfigurationId, TaskType type);

	@Query("{'datasetId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByDatasetIdAndFileSystemConfigurationId(ObjectId datasetId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'indexId' : ?0, 'elasticConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByIndexIdAndElasticConfigurationId(ObjectId indexId, ObjectId elasticConfigurationId, TaskType type);

//	@Query("{'datasetId' : ?0, 'tripleStoreConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }, 'indexStructureId' : ?3 }")
//	Optional<TaskDescription> findActiveByDatasetIdAndTripleStoreConfigurationIdAndIndexStructureId(ObjectId datasetId, ObjectId tripleStoreConfigurationId, TaskType type, ObjectId indexStructureId);

	@Query("{'mappingId' : ?0, 'mappingInstanceId' : ?1, 'fileSystemConfigurationId' : ?2, 'type' : ?3, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByMappingIdAndMappingInstanceIdAndFileSystemConfigurationId(ObjectId mappingId, ObjectId mappingInstanceId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'fileId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByFileIdAndFileSystemConfigurationId(ObjectId fileId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'annotatorId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByAnnotatorIdAndFileSystemConfigurationId(ObjectId annotatorId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'clustererId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByClustererIdAndFileSystemConfigurationId(ObjectId annotatorId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'embedderId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByEmbedderIdAndFileSystemConfigurationId(ObjectId embedderId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'pagedAnnotationValidationId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByPagedAnnotationValidationIdAndFileSystemConfigurationId(ObjectId pagedAnnotationValidationId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'filterAnnotationValidationId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByFilterAnnotationValidationIdAndFileSystemConfigurationId(ObjectId filterAnnotationValidationId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'userTaskId' : ?0, 'type' : ?1, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByUserTaskId(ObjectId userTaskId, TaskType type);

	@Query("{'distributionId' : ?0, 'type' : ?1, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByDistributionId(ObjectId userTaskId, TaskType type);

}


