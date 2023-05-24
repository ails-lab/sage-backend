package ac.software.semantic.repository;

import java.util.Optional;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.constants.UserRoleType;

@Repository
public interface UserRoleRepository extends MongoRepository<UserRole, String> {
    
    Optional<UserRole> findById(ObjectId id);
    
    List<UserRole> findByDatabaseIdAndRole(ObjectId databaseId, UserRoleType role);
    
    Optional<UserRole> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
    
    Optional<UserRole> findByDatabaseIdAndUserIdAndRole(ObjectId databaseId, ObjectId userId, UserRoleType role);

}


