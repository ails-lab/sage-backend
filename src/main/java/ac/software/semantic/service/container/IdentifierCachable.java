package ac.software.semantic.service.container;

import ac.software.semantic.model.base.SpecificationDocument;

public interface IdentifierCachable<D extends SpecificationDocument> {
	
	public void removeFromCache();

}
