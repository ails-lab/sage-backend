package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.IdentifiableDocumentRepository;

@Repository
public interface DistributionDocumentRepository extends DocumentRepository<DistributionDocument> {

	List<DistributionDocument> findByDatabaseId(ObjectId databaseId);
	
	List<DistributionDocument> findByDatasetIdInAndUserId(List<ObjectId> datasetId, ObjectId userId);
	Page<DistributionDocument> findByDatasetIdInAndUserId(List<ObjectId> datasetId, ObjectId userId, Pageable page);
	
	List<DistributionDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
	Page<DistributionDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId, Pageable page);

	Optional<DistributionDocument> findByDatasetUuidAndIdentifier(String uuid, String identifier);
	
	Optional<DistributionDocument> findByDatasetIdAndIdentifier(ObjectId id, String identifier);
	
	List<DistributionDocument> findByDatasetUuid(String uuid);
	
	List<DistributionDocument> findByDatasetId(ObjectId id);
	
	List<DistributionDocument> findByDatasetIdIn(List<ObjectId> id);
	Page<DistributionDocument> findByDatasetIdIn(List<ObjectId> id, Pageable page);
	
//	List<DistributionDocument> findByDatasetIdAndIndexStructureId(ObjectId datasetId, ObjectId indexStructureId);
//	
//	Optional<DistributionDocument> findByDatasetIdAndIndexStructureIdAndElasticConfigurationId(ObjectId datasetId, ObjectId indexStructureId, ObjectId elasticConfigurationId);
//	
//	List<DistributionDocument> findByIndexStructureIdAndElasticConfigurationId(ObjectId indexStructureId, ObjectId elasticConfigurationId);
	
}


