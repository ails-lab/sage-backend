package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetType;

@Repository
public interface DatasetRepository extends MongoRepository<Dataset, String> {

	Optional<Dataset> findById(ObjectId id);

	Optional<Dataset> findByIdentifierAndDatabaseId(String identifier, ObjectId databaseId);

	Optional<Dataset> findByIdAndUserId(ObjectId id, ObjectId userId);
	
	List<Dataset> findByUserId(ObjectId userId);

	Optional<Dataset> findByUuidAndUserId(String uuid, ObjectId userId);
	
	Optional<Dataset> findByUuid(String uuid);
	
	List<Dataset> findByDatabaseId(ObjectId databaseId);
	
	List<Dataset> findByUserIdAndDatabaseId(ObjectId userId, ObjectId databaseId);
	
	List<Dataset> findByUserIdAndTypeAndDatabaseId(ObjectId userId, DatasetType type, ObjectId databaseId);
	
    List<Dataset> findByUserIdAndTypeAndDatabaseId(ObjectId userId, String type, ObjectId databaseId);
    
    List<Dataset> findByScopeAndDatabaseId(DatasetScope scope, ObjectId databaseId);
    
    List<Dataset> findByUserIdAndScopeAndTypeAndDatabaseId(ObjectId userId, DatasetScope scope, DatasetType type, ObjectId databaseId);
    
    List<Dataset> findByUserIdAndScopeAndTypeAndDatabaseId(ObjectId userId, DatasetScope scope, String type, ObjectId databaseId);
    
    Long deleteByIdAndUserId(ObjectId id, ObjectId userId);
    
    List<Dataset> findByDatasets(ObjectId datasetid);

}


