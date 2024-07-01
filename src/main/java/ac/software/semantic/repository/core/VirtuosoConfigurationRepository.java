package ac.software.semantic.repository.core;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.VirtuosoConfiguration;

@Repository
public interface VirtuosoConfigurationRepository extends MongoRepository<VirtuosoConfiguration, String> {

	@Query(value = "{ 'name' : ?0, '_class' : 'ac.software.semantic.model.VirtuosoConfiguration' }")
    Optional<VirtuosoConfiguration> findByName(String name);

}


