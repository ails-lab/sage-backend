package ac.software.semantic.repository;

import ac.software.semantic.model.SemanticProperty;
import ac.software.semantic.model.TokenPasswordReset;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TokenPasswordResetRepository extends MongoRepository<TokenPasswordReset, String> {
    Optional<TokenPasswordReset> findByToken(String token);
}
