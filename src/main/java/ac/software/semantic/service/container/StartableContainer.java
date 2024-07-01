package ac.software.semantic.service.container;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.base.StartableDocument;
import ac.software.semantic.model.constants.state.PagedAnnotationValidationState;
import ac.software.semantic.payload.response.Response;

public interface StartableContainer<D extends SpecificationDocument, F extends Response> extends BaseContainer<D,F> {

	public StartableDocument getLifecycleDocument();
	
	default boolean isStarted() {
		return getLifecycleDocument().getLifecycle() == PagedAnnotationValidationState.STARTED;
	}
}
