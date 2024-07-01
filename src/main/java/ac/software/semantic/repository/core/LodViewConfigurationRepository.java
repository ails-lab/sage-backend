package ac.software.semantic.repository.core;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.LodViewConfiguration;

@Repository
public interface LodViewConfigurationRepository extends MongoRepository<LodViewConfiguration, String> {

	Optional<LodViewConfiguration> findByName(String name);
}


