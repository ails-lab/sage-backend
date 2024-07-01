package ac.software.semantic.model.base;

import ac.software.semantic.model.constants.state.PagedAnnotationValidationState;
import ac.software.semantic.payload.response.ResponseTaskObject;

public interface StartableDocument {

	public PagedAnnotationValidationState getLifecycle();
	
	public ResponseTaskObject createResponseState();
}
