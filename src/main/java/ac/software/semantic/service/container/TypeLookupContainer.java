package ac.software.semantic.service.container;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.request.UpdateRequest;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.lookup.LookupProperties;

public interface TypeLookupContainer<D extends SpecificationDocument, F extends Response, L extends LookupProperties> extends BaseContainer<D,F> {

	public L buildTypeLookupPropetries();
	
}
