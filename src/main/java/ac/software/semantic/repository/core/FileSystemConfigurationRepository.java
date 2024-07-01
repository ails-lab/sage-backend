package ac.software.semantic.repository.core;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;

@Repository
public interface FileSystemConfigurationRepository extends MongoRepository<FileSystemConfiguration, String> {

    Optional<FileSystemConfiguration> findByName(String name);

}


