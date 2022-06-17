package ac.software.semantic.repository;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.VirtuosoConfiguration;

@Repository
public interface VirtuosoConfigurationRepository extends MongoRepository<VirtuosoConfiguration, String> {

    Optional<VirtuosoConfiguration> findByName(String name);

}


