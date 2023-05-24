package ac.software.semantic.repository;

import ac.software.semantic.model.Template;
import ac.software.semantic.model.constants.TemplateType;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TemplateRepository extends MongoRepository<Template, String> {
    Optional<Template> findById(ObjectId id);

    List<Template> findAllByCreatorIdAndType(ObjectId creatorId, TemplateType type);

    Long deleteById(ObjectId id);

    Optional<Template> findByTypeAndName(TemplateType type, String name);
    
    List<Template> findByTypeAndDatabaseId(TemplateType type, ObjectId databaseId);
    
    List<Template> findByTypeAndTemplateId(TemplateType type, ObjectId templateId);
    
    Optional<Template> findByIdAndTypeAndDatabaseId(ObjectId id, TemplateType type, ObjectId databaseId);
    
    Optional<Template> findByNameAndTypeAndDatabaseId(String name, TemplateType type, ObjectId databaseId);
    
    Optional<Template> findByNameAndTypeAndTemplateId(String name, TemplateType type, ObjectId templateId);

}
