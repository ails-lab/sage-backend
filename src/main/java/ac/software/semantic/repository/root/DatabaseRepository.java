package ac.software.semantic.repository.root;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Database;

@Repository
public interface DatabaseRepository extends MongoRepository<Database, String> {

	Optional<Database> findById(ObjectId id);
	
    Optional<Database> findByName(String name);

}


