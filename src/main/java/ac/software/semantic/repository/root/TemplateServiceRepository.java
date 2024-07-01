package ac.software.semantic.repository.root;

import ac.software.semantic.model.TemplateService;
import ac.software.semantic.model.constants.type.TemplateType;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TemplateServiceRepository extends MongoRepository<TemplateService, String> {
    Optional<TemplateService> findById(ObjectId id);

    List<TemplateService> findAllByCreatorIdAndType(ObjectId creatorId, TemplateType type);

    Long deleteById(ObjectId id);

    Optional<TemplateService> findByTypeAndName(TemplateType type, String name);
    
    List<TemplateService> findByTypeAndDatabaseIdOrderByName(TemplateType type, ObjectId databaseId);
    
    List<TemplateService> findByTypeAndTemplateIdOrderByName(TemplateType type, ObjectId templateId);
    
    Optional<TemplateService> findByIdAndTypeAndDatabaseId(ObjectId id, TemplateType type, ObjectId databaseId);
    
    Optional<TemplateService> findByNameAndTypeAndDatabaseId(String name, TemplateType type, ObjectId databaseId);
    
    Optional<TemplateService> findByNameAndTypeAndTemplateId(String name, TemplateType type, ObjectId templateId);

}
