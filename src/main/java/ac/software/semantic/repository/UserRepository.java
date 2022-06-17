package ac.software.semantic.repository;

import java.util.Optional;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.User;
import ac.software.semantic.model.UserType;

@Repository
public interface UserRepository extends MongoRepository<User, String> {
    
    // no need for findByUsername since we now find by email
    // Optional<User> findByUsername(String username);
    
    Optional<User> findById(String id);

    Optional<User> findByEmail(String email);

    List<User> findByTypeAndIsPublic(UserType type, boolean isPublic);

    List<User> findByValidatorListIn(List<ObjectId> validatorIds);
}


