package ac.software.semantic.repository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.constants.TaskState;
import ac.software.semantic.model.constants.TaskType;

@Repository
public interface TaskRepository extends MongoRepository<TaskDescription, String> {

	void deleteById(ObjectId id);
	
	List<TaskDescription> findByDatabaseIdAndStateAndSystem(ObjectId databaseId, TaskState state, String system);
	
	List<TaskDescription> findByUserIdAndDatabaseIdAndParentIdExistsOrderByCreateTimeDesc(ObjectId userId, ObjectId databaseId, boolean parent);
	
	List<TaskDescription> findByUserIdAndDatabaseIdAndParentIdExistsAndCreateTimeGreaterThanOrderByCreateTimeDesc(ObjectId userId, ObjectId databaseId, boolean parent, Date date);

	@Query("{'datasetId' : ?0, 'tripleStoreConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByDatasetIdAndTripleStoreConfigurationId(ObjectId datasetId, ObjectId tripleStoreConfigurationId, TaskType type);

	@Query("{'mappingId' : ?0, 'mappingInstanceId' : ?1, 'fileSystemConfigurationId' : ?2, 'type' : ?3, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByMappingIdAndMappingInstanceIdAndFileSystemConfigurationId(ObjectId mappingId, ObjectId mappingInstanceId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'annotatorId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByAnnotatorIdAndFileSystemConfigurationId(ObjectId annotatorId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'embedderId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByEmbedderIdAndFileSystemConfigurationId(ObjectId embedderId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'pagedAnnotationValidationId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByPagedAnnotationValidationIdAndFileSystemConfigurationId(ObjectId pagedAnnotationValidationId, ObjectId fileSystemConfigurationId, TaskType type);

	@Query("{'filterAnnotationValidationId' : ?0, 'fileSystemConfigurationId' : ?1, 'type' : ?2, 'state' : { $in: [ 'QUEUED', 'STARTED', 'STOPPING' ] }}")
	Optional<TaskDescription> findActiveByFilterAnnotationValidationIdAndFileSystemConfigurationId(ObjectId filterAnnotationValidationId, ObjectId fileSystemConfigurationId, TaskType type);

}


