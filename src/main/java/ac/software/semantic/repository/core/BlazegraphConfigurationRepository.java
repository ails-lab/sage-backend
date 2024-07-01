package ac.software.semantic.repository.core;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.BlazegraphConfiguration;

@Repository
public interface BlazegraphConfigurationRepository extends MongoRepository<BlazegraphConfiguration, String> {

	@Query(value = "{ 'name' : ?0, '_class' : 'ac.software.semantic.model.BlazegraphConfiguration' }")
    Optional<BlazegraphConfiguration> findByName(String name);

}


