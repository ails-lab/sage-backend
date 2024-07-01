package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.IndexLookupProperties;
import ac.software.semantic.service.lookup.MappingLookupProperties;

@Repository
public interface CustomIndexRepository {

	public List<IndexDocument> find(ObjectId userId, List<Dataset> dataset, IndexLookupProperties lp, ObjectId databaseId);
	public Page<IndexDocument> find(ObjectId userId, List<Dataset> dataset, IndexLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
