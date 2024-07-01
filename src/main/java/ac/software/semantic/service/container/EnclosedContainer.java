package ac.software.semantic.service.container;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.repository.DocumentRepository;


public interface EnclosedContainer<D extends SpecificationDocument, E extends EnclosingDocument> {

	public E getEnclosingObject();
	
	public DocumentRepository<E> getEnclosingDocumentRepository();
}
