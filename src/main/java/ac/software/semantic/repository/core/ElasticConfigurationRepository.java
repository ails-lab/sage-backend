package ac.software.semantic.repository.core;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.ElasticConfiguration;

@Repository
public interface ElasticConfigurationRepository extends MongoRepository<ElasticConfiguration, String> {

	Optional<ElasticConfiguration> findById(ObjectId id);
	
    Optional<ElasticConfiguration> findByName(String name);

}


