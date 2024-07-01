package ac.software.semantic.repository.core;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomPrototypeRepository;

@Repository
public interface PrototypeDocumentRepository extends DocumentRepository<PrototypeDocument>, CustomPrototypeRepository {

	List<PrototypeDocument> findByDatabaseId(ObjectId databaseId);
	
	List<PrototypeDocument> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
	
	Optional<PrototypeDocument> findByDatasetIdAndUuid(ObjectId datasetId, String uuid);
	
//	List<PrototypeDocument> findByDatabaseIdAndUserIdAndType(ObjectId databaseId, ObjectId userId, PrototypeType type);
//	Page<PrototypeDocument> findByDatabaseIdAndUserIdAndType(ObjectId databaseId, ObjectId userId, PrototypeType type, Pageable page);
//	
//	List<PrototypeDocument> findByDatasetIdAndUserIdAndType(ObjectId datasetId, ObjectId userId, PrototypeType type);
//	Page<PrototypeDocument> findByDatasetIdAndUserIdAndType(ObjectId datasetId, ObjectId userId, PrototypeType type, Pageable page);
}
