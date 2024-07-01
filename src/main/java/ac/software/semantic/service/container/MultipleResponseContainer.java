package ac.software.semantic.service.container;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.payload.response.modifier.ResponseModifier;

public interface MultipleResponseContainer<D extends SpecificationDocument, F extends Response, R extends ResponseModifier> extends BaseContainer<D,F> {

	public F asResponse(R modifier);
	
	@Override
	public default F asResponse() {
		return asResponse(null);
	}
}
