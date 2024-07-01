package ac.software.semantic.service;

import ac.software.semantic.model.IdentifiableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.repository.IdentifiableDocumentRepository;

public interface IdentifiableDocumentService<D extends IdentifiableDocument, F extends Response> extends ContainerService<D,F> {

	public default boolean identifierConfict(D object, IdentifierType type) {
		return ((IdentifiableDocumentRepository<D>)getRepository()).existsSameIdentifier(object, type);
	}

	default boolean isValidIdentifier(D doc, IdentifierType type) {
		if (type == IdentifierType.IDENTIFIER) {
			return doc.getIdentifier(type).matches("^[0-9A-Za-z\\-]+$");
		} else {
			return false;
		}
	}
	
	public default IdentifierType[] identifierTypes() {
		return new IdentifierType[] { IdentifierType.IDENTIFIER };
	}

}
