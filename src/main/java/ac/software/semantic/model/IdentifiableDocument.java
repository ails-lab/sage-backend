package ac.software.semantic.model;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;

public interface IdentifiableDocument extends SpecificationDocument {

	public String getIdentifier(IdentifierType type);

	public void setIdentifier(String identifier, IdentifierType type);
	
}
