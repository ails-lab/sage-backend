package ac.software.semantic.repository;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import ac.software.semantic.model.base.SpecificationDocument;

public interface DocumentRepository<D extends SpecificationDocument> extends MongoRepository<D, String> {

	public Optional<D> findById(ObjectId id);
	
	public Optional<D> findByIdAndUserId(ObjectId id, ObjectId userId);

}
