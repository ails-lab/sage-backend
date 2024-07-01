package ac.software.semantic.repository.core;

import java.util.Optional;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.User;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomUserRepository;

@Repository
public interface UserRepository extends DocumentRepository<User>, CustomUserRepository {
    
    // no need for findByUsername since we now find by email
    // Optional<User> findByUsername(String username);
    
    Optional<User> findById(String id);
    
    Optional<User> findByUuid(String uuid);
    
    @Override
    Optional<User> findById(ObjectId id);
    
    @Override
    default Optional<User> findByIdAndUserId(ObjectId id, ObjectId userId) {
    	return findById(id);
    }

    Optional<User> findByEmail(String email);

    List<User> findByDatabaseId(ObjectId databaseId);
    Page<User> findByDatabaseId(ObjectId databaseId, Pageable page);
    
    Optional<User> findByIdentifierAndDatabaseId(String identifier, ObjectId databaseId);
}


