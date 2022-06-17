package ac.software.semantic.repository;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataService.DataServiceType;

@Repository
public interface DataServiceRepository extends MongoRepository<DataService, String> {

    List<DataService> findByDatabaseIdAndType(ObjectId databaseId, DataServiceType type);
    
    Optional<DataService> findByIdentifierAndType(String identifier, DataServiceType type);

}


