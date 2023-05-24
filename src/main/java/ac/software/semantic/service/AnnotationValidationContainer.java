package ac.software.semantic.service;

import org.bson.types.ObjectId;

import ac.software.semantic.model.AnnotationValidation;

public interface AnnotationValidationContainer extends BaseContainer, ExecutableContainer {

	public AnnotationValidation getAnnotationValidation();

}
