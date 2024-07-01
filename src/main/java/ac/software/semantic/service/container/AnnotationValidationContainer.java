package ac.software.semantic.service.container;

import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.payload.response.Response;

public interface AnnotationValidationContainer<V extends AnnotationValidation, F extends Response, I extends EnclosingDocument> extends EnclosedBaseContainer<V,F,I>, SideExecutableContainer<V,F,MappingExecuteState,I> {

	public default AnnotationValidation getAnnotationValidation() {
		return (AnnotationValidation)getObject();
	}
	
	public boolean hasValidations();
	

}
