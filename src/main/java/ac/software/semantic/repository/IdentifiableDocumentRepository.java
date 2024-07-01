package ac.software.semantic.repository;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;

public interface IdentifiableDocumentRepository<D extends SpecificationDocument>  {

	boolean existsSameIdentifier(D object, IdentifierType type);

}
