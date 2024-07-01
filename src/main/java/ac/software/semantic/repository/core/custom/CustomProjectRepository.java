package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.ProjectLookupProperties;

public interface CustomProjectRepository extends IdentifiableDocumentRepository<ProjectDocument> {

	public List<ProjectDocument> find(ObjectId userId, ProjectLookupProperties lp, ObjectId databaseId);
	public Page<ProjectDocument> find(ObjectId userId, ProjectLookupProperties lp, ObjectId databaseId, Pageable page);
}
