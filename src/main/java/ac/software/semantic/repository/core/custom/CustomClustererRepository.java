package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ClustererDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.AnnotatorLookupProperties;
import ac.software.semantic.service.lookup.ClustererLookupProperties;
import ac.software.semantic.service.lookup.IndexLookupProperties;
import ac.software.semantic.service.lookup.MappingLookupProperties;

@Repository
public interface CustomClustererRepository extends IdentifiableDocumentRepository<ClustererDocument> {

	public List<ClustererDocument> find(ObjectId userId, List<Dataset> dataset, ClustererLookupProperties lp, ObjectId databaseId);
	public Page<ClustererDocument> find(ObjectId userId, List<Dataset> dataset, ClustererLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
