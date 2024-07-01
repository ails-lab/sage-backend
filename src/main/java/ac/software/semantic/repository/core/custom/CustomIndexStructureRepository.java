package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.IndexStructureLookupProperties;

@Repository
public interface CustomIndexStructureRepository extends IdentifiableDocumentRepository<IndexStructure>  {

	public List<IndexStructure> find(ObjectId userId, List<Dataset> dataset, IndexStructureLookupProperties lp, ObjectId databaseId);
	public Page<IndexStructure> find(ObjectId userId, List<Dataset> dataset, IndexStructureLookupProperties lp, ObjectId databaseId, Pageable page);
	
}
