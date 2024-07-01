package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.ComparatorDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.ComparatorLookupProperties;

@Repository
public interface CustomComparatorRepository extends IdentifiableDocumentRepository<ComparatorDocument>  {

	public List<ComparatorDocument> find(ObjectId userId, List<Dataset> dataset, ComparatorLookupProperties lp, ObjectId databaseId);
	public Page<ComparatorDocument> find(ObjectId userId, List<Dataset> dataset, ComparatorLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
