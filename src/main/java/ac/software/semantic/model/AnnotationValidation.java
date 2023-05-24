package ac.software.semantic.model;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.MappingExecuteState;

public interface AnnotationValidation extends SpecificationDocument {

	public String getAsProperty();
	
	public MappingExecuteState getExecuteState(ObjectId fileSystemConfigurationId);
	
	public ObjectId getAnnotationEditGroupId();

}
