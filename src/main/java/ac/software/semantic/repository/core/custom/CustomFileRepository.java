package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.FileLookupProperties;
import ac.software.semantic.service.lookup.MappingLookupProperties;

@Repository
public interface CustomFileRepository  {

	public List<FileDocument> find(ObjectId userId, List<Dataset> dataset, FileLookupProperties lp, ObjectId databaseId);
	public Page<FileDocument> find(ObjectId userId, List<Dataset> dataset, FileLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
