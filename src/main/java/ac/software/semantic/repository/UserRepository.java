package ac.software.semantic.repository;

import java.util.Optional;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.User;

@Repository
public interface UserRepository extends MongoRepository<User, String> {
    
    // no need for findByUsername since we now find by email
    // Optional<User> findByUsername(String username);
    
    Optional<User> findById(String id);
    
    Optional<User> findById(ObjectId id);

    Optional<User> findByEmail(String email);

    List<User> findByValidatorListIn(List<ObjectId> validatorIds);
    
    List<User> findByValidatorList(ObjectId validatorId);
    
    List<User> findByDatabaseId(ObjectId databaseId);
}


