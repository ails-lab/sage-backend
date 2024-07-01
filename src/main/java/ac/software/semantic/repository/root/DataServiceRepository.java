package ac.software.semantic.repository.root;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataService.DataServiceType;

@Repository
public interface DataServiceRepository extends MongoRepository<DataService, String> {

    List<DataService> findByDatabaseIdAndTypeOrderByTitleAsc(ObjectId databaseId, DataServiceType type);
    
    List<DataService> findByDatabaseId(ObjectId databaseId);
    
    Optional<DataService> findByIdentifierAndType(String identifier, DataServiceType type);

}


