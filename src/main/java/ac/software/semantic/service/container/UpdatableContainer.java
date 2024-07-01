package ac.software.semantic.service.container;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.request.UpdateRequest;
import ac.software.semantic.payload.response.Response;

public interface UpdatableContainer<D extends SpecificationDocument, F extends Response, T extends UpdateRequest> extends BaseContainer<D,F> {

	public D update(T request) throws Exception;
	
}
