package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.UserTaskDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomUserTaskRepository;

@Repository
public interface UserTaskDocumentRepository extends DocumentRepository<UserTaskDocument>, CustomUserTaskRepository {

//	List<UserTaskDocument> findByDatabaseId(ObjectId databaseId);
	
//	List<UserTaskDocument> findByDatabaseIdAndFileSystemConfigurationId(ObjectId databaseId, ObjectId fileSystemConfigurationId);
	
//	List<UserTaskDocument> findByDatasetId(ObjectId datasetId);
	
//	List<UserTaskDocument> findByDatasetIdInAndUserIdAndFileSystemConfigurationId(List<ObjectId> datasetId, ObjectId userId, ObjectId fileSystemConfigurationId);
//	List<UserTaskDocument> findByDatasetIdInAndUserIdAndFileSystemConfigurationId(List<ObjectId> datasetId, ObjectId userId, ObjectId fileSystemConfigurationId, Pageable page);
//	
//	List<UserTaskDocument> findByDatabaseIdAndUserIdAndFileSystemConfigurationId(ObjectId databaseId, ObjectId userId, ObjectId fileSystemConfigurationId);
//	List<UserTaskDocument> findByDatabaseIdAndUserIdAndFileSystemConfigurationId(ObjectId databaseId, ObjectId userId, ObjectId fileSystemConfigurationId, Pageable page);
}


