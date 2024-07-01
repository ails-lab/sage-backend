package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomProjectRepository;

@Repository
public interface ProjectDocumentRepository extends DocumentRepository<ProjectDocument>, CustomProjectRepository {

   List<ProjectDocument> findByUserId(ObjectId userId);
   
   Optional<ProjectDocument> findByIdentifierAndDatabaseId(String identifier, ObjectId databaseId);
   
   Optional<ProjectDocument> findByUuid(String uuid);
   
   Optional<ProjectDocument> findByDatabaseDefaultAndDatabaseId(boolean databaseDefault, ObjectId databaseId);
   
}


