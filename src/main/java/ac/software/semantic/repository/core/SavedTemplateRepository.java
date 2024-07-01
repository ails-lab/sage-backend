package ac.software.semantic.repository.core;

import ac.software.semantic.model.SavedTemplate;
import ac.software.semantic.model.constants.type.TemplateType;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SavedTemplateRepository extends MongoRepository<SavedTemplate, String> {
    Optional<SavedTemplate> findById(ObjectId id);

    List<SavedTemplate> findAllByCreatorIdAndType(ObjectId creatorId, TemplateType type);

    Long deleteById(ObjectId id);

    Optional<SavedTemplate> findByTypeAndName(TemplateType type, String name);
    
    List<SavedTemplate> findByTypeAndDatabaseId(TemplateType type, ObjectId databaseId);
    
    List<SavedTemplate> findByTypeAndTemplateId(TemplateType type, ObjectId templateId);
    
    Optional<SavedTemplate> findByIdAndTypeAndDatabaseId(ObjectId id, TemplateType type, ObjectId databaseId);
    
    Optional<SavedTemplate> findByNameAndTypeAndDatabaseId(String name, TemplateType type, ObjectId databaseId);
    
    Optional<SavedTemplate> findByNameAndTypeAndTemplateId(String name, TemplateType type, ObjectId templateId);

}
