package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomIndexStructureRepository;

@Repository
public interface IndexStructureRepository extends DocumentRepository<IndexStructure>, CustomIndexStructureRepository {

    Optional<IndexStructure> findById(ObjectId id);
    
    Optional<IndexStructure> findByIdentifierAndDatabaseId(String identifier, ObjectId databaseId);
    
    List<IndexStructure> findByDatabaseId(ObjectId databaseId);

}


