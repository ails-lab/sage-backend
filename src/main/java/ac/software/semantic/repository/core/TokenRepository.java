package ac.software.semantic.repository.core;

import ac.software.semantic.model.ActionToken;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TokenRepository extends MongoRepository<ActionToken, String> {
    Optional<ActionToken> findByToken(String token);
}
