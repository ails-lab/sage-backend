package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Vocabulary;

@Repository
public interface VocabularyRepository extends MongoRepository<Vocabulary, String> {

    Optional<Vocabulary> findByName(String name);
    
    List<Vocabulary> findByDatabaseId(ObjectId databaseId);

}


