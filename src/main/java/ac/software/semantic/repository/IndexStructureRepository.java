package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.IndexStructure;

@Repository
public interface IndexStructureRepository extends MongoRepository<IndexStructure, String> {

    Optional<IndexStructure> findById(ObjectId id);
    
    Optional<IndexStructure> findByDatabaseIdAndIdentifier(ObjectId databaseId, String identifier);
    
    List<IndexStructure> findByDatabaseId(ObjectId databaseId);

}


