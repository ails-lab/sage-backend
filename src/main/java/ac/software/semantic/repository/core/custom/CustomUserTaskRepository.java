package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.UserTaskDocument;
import ac.software.semantic.service.lookup.UserTaskLookupProperties;

public interface CustomUserTaskRepository { //extends IdentifiableDocumentRepository<UserTaskDocument> {

	public List<UserTaskDocument> find(ObjectId userId, List<Dataset> dataset, UserTaskLookupProperties lp, ObjectId databaseId);
	public Page<UserTaskDocument> find(ObjectId userId, List<Dataset> dataset, UserTaskLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
