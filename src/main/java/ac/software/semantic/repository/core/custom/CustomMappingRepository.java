package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.MappingLookupProperties;

@Repository
public interface CustomMappingRepository extends IdentifiableDocumentRepository<MappingDocument> {

	public List<MappingDocument> find(ObjectId userId, List<Dataset> dataset, MappingLookupProperties lp, ObjectId databaseId);
	public Page<MappingDocument> find(ObjectId userId, List<Dataset> dataset, MappingLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
