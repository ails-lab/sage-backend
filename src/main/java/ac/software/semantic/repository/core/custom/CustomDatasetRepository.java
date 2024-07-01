package ac.software.semantic.repository.core.custom;

import java.util.Collection;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.DatasetLookupProperties;

@Repository
public interface CustomDatasetRepository extends IdentifiableDocumentRepository<Dataset> {

	public List<Dataset> find(ObjectId userId, List<ProjectDocument> project, DatasetLookupProperties lp, ObjectId databaseId, Collection<ObjectId> tripleStoreId);
	public Page<Dataset> find(ObjectId userId, List<ProjectDocument> project, DatasetLookupProperties lp, ObjectId databaseId, Collection<ObjectId> tripleStoreId, Pageable page);
	
}
