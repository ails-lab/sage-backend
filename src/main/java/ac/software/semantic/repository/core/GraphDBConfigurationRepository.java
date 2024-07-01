package ac.software.semantic.repository.core;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.BlazegraphConfiguration;
import ac.software.semantic.model.GraphDBConfiguration;

@Repository
public interface GraphDBConfigurationRepository extends MongoRepository<GraphDBConfiguration, String> {

	@Query(value = "{ 'name' : ?0, '_class' : 'ac.software.semantic.model.GraphDBConfiguration' }")
    Optional<GraphDBConfiguration> findByName(String name);

}


