package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.PrototypeLookupProperties;

@Repository
public interface CustomPrototypeRepository {

	public List<PrototypeDocument> find(ObjectId userId, List<Dataset> dataset, PrototypeLookupProperties lp, ObjectId databaseId);
	public Page<PrototypeDocument> find(ObjectId userId, List<Dataset> dataset, PrototypeLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
