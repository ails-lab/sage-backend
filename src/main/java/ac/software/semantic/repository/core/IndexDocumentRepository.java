package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomIndexRepository;

@Repository
public interface IndexDocumentRepository extends DocumentRepository<IndexDocument>, CustomIndexRepository {

	List<IndexDocument> findByDatabaseId(ObjectId databaseId);
	
	default List<IndexDocument> findByDatasetId(ObjectId datasetId) {
		return findByDatasetIdOrderByOrderAsc(datasetId);
	}
	
	List<IndexDocument> findByDatasetIdOrderByOrderAsc(ObjectId datasetId);
	
	List<IndexDocument> findByIndexStructureId(ObjectId datasetId);
	   
	Optional<IndexDocument> findByDatasetIdAndIndexStructureIdAndElasticConfigurationId(ObjectId datasetId, ObjectId indexStructureId, ObjectId elasticConfigurationId);

//	public default List<IndexDocument> findByDatasetIdAndUserIdAndElasticConfigurationIds(ObjectId datasetId, ObjectId userId, Collection<ObjectId> elasticConfigurationIds) {
//		return findByDatasetIdAndUserIdAndElasticConfigurationIdsOrderByOrderAsc(datasetId, userId, elasticConfigurationIds);
//	}

//	public default Page<IndexDocument> findByDatasetIdAndUserIdAndElasticConfigurationIds(ObjectId datasetId, ObjectId userId, Collection<ObjectId> elasticConfigurationIds, Pageable page) {
//		return findByDatasetIdAndUserIdAndElasticConfigurationIdsOrderByOrderAsc(datasetId, userId, elasticConfigurationIds, page);
//	}

//	@Query(value = "{ 'datasetId' : ?0, 'userId' : ?1, 'elasticConfigurationId' : { $in : ?2 } }", sort = "{ order : 1 }")
//	List<IndexDocument> findByDatasetIdAndUserIdAndElasticConfigurationIdsOrderByOrderAsc(ObjectId datasetId, ObjectId userId, Collection<ObjectId> elasticConfigurationIds);
//	@Query(value = "{ 'datasetId' : ?0, 'userId' : ?1, 'elasticConfigurationId' : { $in : ?2 } }", sort = "{ order : 1 }")
//	Page<IndexDocument> findByDatasetIdAndUserIdAndElasticConfigurationIdsOrderByOrderAsc(ObjectId datasetId, ObjectId userId, Collection<ObjectId> elasticConfigurationIds, Pageable page);

	public default List<IndexDocument> findByDatasetIdAndIndexStructureId(ObjectId datasetId, ObjectId indexStructureId) {
		return findByDatasetIdAndIndexStructureIdOrderByOrderAsc(datasetId, indexStructureId);
	}
	
	List<IndexDocument> findByDatasetIdAndIndexStructureIdOrderByOrderAsc(ObjectId datasetId, ObjectId indexStructureId);

//	@Query(value = "{ 'databaseId' : ?0, 'userId' : ?1, 'elasticConfigurationId' : { $in : ?2 } }", sort = "{ order : 1 }")
//	List<IndexDocument> findByDatabaseIdAndUserIdAndElasticConfigurationIds(ObjectId databaseId, ObjectId userId, Collection<ObjectId> elasticConfigurationIds);
//	@Query(value = "{ 'databaseId' : ?0, 'userId' : ?1, 'elasticConfigurationId' : { $in : ?2 } }", sort = "{ order : 1 }")
//	Page<IndexDocument> findByDatabaseIdAndUserIdAndElasticConfigurationIds(ObjectId databaseId, ObjectId userId, Collection<ObjectId> elasticConfigurationIds, Pageable page);

}


