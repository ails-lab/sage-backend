package ac.software.semantic.repository.core;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.UserSession;

@Repository
public interface UserSessionRepository extends MongoRepository<UserSession, String> {
    
    Optional<UserSession> findById(ObjectId id);
    
    Optional<UserSession> findByUserIdAndDatabaseId(ObjectId userId, ObjectId databaseId);

}


