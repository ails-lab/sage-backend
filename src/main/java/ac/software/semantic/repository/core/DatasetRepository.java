package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomDatasetRepository;

@Repository
public interface DatasetRepository extends DocumentRepository<Dataset>, CustomDatasetRepository {

	List<Dataset> findByUserId(ObjectId userId);

	Optional<Dataset> findByUuidAndUserId(String uuid, ObjectId userId);
	
	Optional<Dataset> findByUuid(String uuid);
	Optional<Dataset> findById(String id);
	
	List<Dataset> findByDatabaseId(ObjectId databaseId);
	
	Optional<Dataset> findByIdentifierAndDatabaseId(String identifier, ObjectId databaseId);
	
	Optional<Dataset> findByIdentifierAndProjectIdAndDatabaseId(String identifier, ObjectId projectId, ObjectId databaseId);
	Optional<Dataset> findByIdentifierAndUserIdAndDatabaseId(String identifier, ObjectId userId, ObjectId databaseId);
	
    List<Dataset> findByScopeAndDatabaseId(DatasetScope scope, ObjectId databaseId);
	
	List<Dataset> findByUserIdAndDatabaseId(ObjectId userId, ObjectId databaseId);
//	Page<Dataset> findByUserIdAndDatabaseId(ObjectId userId, ObjectId databaseId, Pageable page);
//
//	List<Dataset> findByUserIdAndProjectId(ObjectId userId, ObjectId projectId);
//	Page<Dataset> findByUserIdAndProjectId(ObjectId userId, ObjectId projectId, Pageable page);
//
//	List<Dataset> findByUserIdAndTypeAndDatabaseId(ObjectId userId, DatasetType type, ObjectId databaseId);
//	Page<Dataset> findByUserIdAndTypeAndDatabaseId(ObjectId userId, DatasetType type, ObjectId databaseId, Pageable page);
//
//	List<Dataset> findByUserIdAndTypeAndProjectId(ObjectId userId, DatasetType type, ObjectId projectId);
//	Page<Dataset> findByUserIdAndTypeAndProjectId(ObjectId userId, DatasetType type, ObjectId projectId, Pageable page);

//	@Query("{ 'userId' : ?1, 'type' : ?2, 'databaseId' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	List<Dataset> findPublishedByUserIdAndTypeAndDatabaseId(Collection<ObjectId> tripleStoreId, ObjectId userId, DatasetType type, ObjectId databaseId);
//	@Query("{ 'userId' : ?1, 'type' : ?2, 'databaseId' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	Page<Dataset> findPublishedByUserIdAndTypeAndDatabaseId(Collection<ObjectId> tripleStoreId, ObjectId userId, DatasetType type, ObjectId databaseId, Pageable page);
//
//	@Query("{ 'userId' : ?1, 'type' : ?2, 'projectId' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	List<Dataset> findPublishedByUserIdAndTypeAndProjectId(Collection<ObjectId> tripleStoreId, ObjectId userId, DatasetType type, ObjectId projectId);
//	@Query("{ 'userId' : ?1, 'type' : ?2, 'projectId' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	Page<Dataset> findPublishedByUserIdAndTypeAndProjectId(Collection<ObjectId> tripleStoreId, ObjectId userId, DatasetType type, ObjectId projectId, Pageable page);
//
//	List<Dataset> findByTypeAndDatabaseIdAndPublik(DatasetType type, ObjectId databaseId, Boolean publik);
//	Page<Dataset> findByTypeAndDatabaseIdAndPublik(DatasetType type, ObjectId databaseId, Boolean publik, Pageable page);
//
//	List<Dataset> findByTypeAndProjectIdAndPublik(DatasetType type, ObjectId projectId, Boolean publik);
//	Page<Dataset> findByTypeAndProjectIdAndPublik(DatasetType type, ObjectId projectId, Boolean publik, Pageable page);
//
//	@Query("{ 'type' : ?1, 'databaseId' : ?2, 'public' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	List<Dataset> findPublishedByTypeAndDatabaseIdAndPublik(Collection<ObjectId> tripleStoreId, DatasetType type, ObjectId databaseId, Boolean publik);
//	@Query("{ 'type' : ?1, 'databaseId' : ?2, 'public' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	Page<Dataset> findPublishedByTypeAndDatabaseIdAndPublik(Collection<ObjectId> tripleStoreId, DatasetType type, ObjectId databaseId, Boolean publik, Pageable page);
//
//	@Query("{ 'type' : ?1, 'projectId' : ?2, 'public' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	List<Dataset> findPublishedByTypeAndProjectIdAndPublik(Collection<ObjectId> tripleStoreId, DatasetType type, ObjectId projectId, Boolean publik);
//	@Query("{ 'type' : ?1, 'projectId' : ?2, 'public' : ?3, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//	Page<Dataset> findPublishedByTypeAndProjectIdAndPublik(Collection<ObjectId> tripleStoreId, DatasetType type, ObjectId projectId, Boolean publik, Pageable page);
	
//    List<Dataset> findByUserIdAndScopeInAndTypeAndDatabaseId(ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId databaseId);
//    Page<Dataset> findByUserIdAndScopeInAndTypeAndDatabaseId(ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId databaseId, Pageable page);
//
//    List<Dataset> findByUserIdAndScopeInAndTypeAndProjectId(ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId projectId);
//    Page<Dataset> findByUserIdAndScopeInAndTypeAndProjectId(ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId projectId, Pageable page);
//
//    @Query("{ 'userId' : ?1, 'scope' : { $in: ?2 }, 'type' : ?3, 'databaseId' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    List<Dataset> findPublishedByUserIdAndScopeInAndTypeAndDatabaseId(Collection<ObjectId> tripleStoreId, ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId databaseId);
//    @Query("{ 'userId' : ?1, 'scope' : { $in: ?2 }, 'type' : ?3, 'databaseId' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    Page<Dataset> findPublishedByUserIdAndScopeInAndTypeAndDatabaseId(Collection<ObjectId> tripleStoreId, ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId databaseId, Pageable page);
//
//    @Query("{ 'userId' : ?1, 'scope' : { $in: ?2 }, 'type' : ?3, 'projectId' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    List<Dataset> findPublishedByUserIdAndScopeInAndTypeAndProjectId(Collection<ObjectId> tripleStoreId, ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId projectId);
//    @Query("{ 'userId' : ?1, 'scope' : { $in: ?2 }, 'type' : ?3, 'projectId' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    Page<Dataset> findPublishedByUserIdAndScopeInAndTypeAndProjectId(Collection<ObjectId> tripleStoreId, ObjectId userId, List<DatasetScope> scope, DatasetType type, ObjectId projectId, Pageable page);
//
//    List<Dataset> findByScopeInAndTypeAndDatabaseIdAndPublik(List<DatasetScope> scope, DatasetType type, ObjectId databaseId, Boolean publik);
//    Page<Dataset> findByScopeInAndTypeAndDatabaseIdAndPublik(List<DatasetScope> scope, DatasetType type, ObjectId databaseId, Boolean publik, Pageable page);
//
//    List<Dataset> findByScopeInAndTypeAndProjectIdAndPublik(List<DatasetScope> scope, DatasetType type, ObjectId projectId, Boolean publik);
//    Page<Dataset> findByScopeInAndTypeAndProjectIdAndPublik(List<DatasetScope> scope, DatasetType type, ObjectId projectId, Boolean publik, Pageable page);
//
//    @Query("{ 'scope' : { $in: ?1 }, 'type' : ?2, 'databaseId' : ?3, 'public' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    List<Dataset> findPublishedByScopeInAndTypeAndDatabaseIdAndPublik(Collection<ObjectId> tripleStoreId, List<DatasetScope> scope, DatasetType type, ObjectId databaseId, Boolean publik);
//    @Query("{ 'scope' : { $in: ?1 }, 'type' : ?2, 'databaseId' : ?3, 'public' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    Page<Dataset> findPublishedByScopeInAndTypeAndDatabaseIdAndPublik(Collection<ObjectId> tripleStoreId, List<DatasetScope> scope, DatasetType type, ObjectId databaseId, Boolean publik, Pageable page);
//
//    @Query("{ 'scope' : { $in: ?1 }, 'type' : ?2, 'projectId' : ?3, 'public' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    List<Dataset> findPublishedByScopeInAndTypeAndProjectIdAndPublik(Collection<ObjectId> tripleStoreId, List<DatasetScope> scope, DatasetType type, ObjectId projectId, Boolean publik);
//    @Query("{ 'scope' : { $in: ?1 }, 'type' : ?2, 'projectId' : ?3, 'public' : ?4, 'publish' : { $elemMatch: {'databaseConfigurationId' : { $in: ?0 }, publishState: { $in: ['PUBLISHED', 'PUBLISHED_PRIVATE', 'PUBLISHED_PUBLIC' ]}}}}")
//    Page<Dataset> findPublishedByScopeInAndTypeAndProjectIdAndPublik(Collection<ObjectId> tripleStoreId, List<DatasetScope> scope, DatasetType type, ObjectId projectId, Boolean publik, Pageable page);

    Long deleteByIdAndUserId(ObjectId id, ObjectId userId);
    
    List<Dataset> findByDatasets(ObjectId datasetid);

}


