package ac.software.semantic.repository;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Database;

@Repository
public interface DatabaseRepository extends MongoRepository<Database, String> {

    Optional<Database> findByName(String name);

}


