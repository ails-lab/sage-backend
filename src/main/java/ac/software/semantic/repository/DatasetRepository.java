package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;

@Repository
public interface DatasetRepository extends MongoRepository<Dataset, String> {

	Optional<Dataset> findById(ObjectId id);
	
	Optional<Dataset> findByIdAndUserId(ObjectId id, ObjectId userId);

	List<Dataset> findByUserId(ObjectId userId);

	Optional<Dataset> findByUuidAndUserId(String uuid, ObjectId userId);
	
	Optional<Dataset> findByUuid(String uuid);
	
    List<Dataset> findByUserIdAndTypeAndDatabaseId(ObjectId userId, String type, ObjectId databaseId);
    
    Long deleteByIdAndUserId(ObjectId id, ObjectId userId);

}


