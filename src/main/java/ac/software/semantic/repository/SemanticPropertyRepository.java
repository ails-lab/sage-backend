package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.SemanticProperty;

@Repository
public interface SemanticPropertyRepository extends MongoRepository<SemanticProperty, String> {

	Optional<SemanticProperty> findByUri(String uri);
//	
//	Optional<Dataset> findOneByIdAndUserId(ObjectId id, ObjectId userId);
//	
//	Optional<Dataset> findOneByUuidAndUserId(String uuid, ObjectId userId);
//	
//    List<Dataset> findByUserIdAndType(ObjectId userId, String type);
//    
//    Long deleteByIdAndUserId(ObjectId id, ObjectId userId);

}


