package ac.software.semantic.repository.core;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.TripleStoreConfiguration;

@Repository
public interface TripleStoreConfigurationRepository extends MongoRepository<TripleStoreConfiguration, String> {

    Optional<TripleStoreConfiguration> findByName(String name);

}


