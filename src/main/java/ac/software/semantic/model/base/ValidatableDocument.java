package ac.software.semantic.model.base;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.ValidateState;

public interface ValidatableDocument<M extends ValidateState> {

	public List<M> getValidate();

	public void setValidate(List<M> execute);
	
	public M getValidateState(ObjectId fileSystemConfigurationId);
	
	public M checkValidateState(ObjectId fileSystemConfigurationId);
	
	public void deleteValidateState(ObjectId fileSystemConfigurationId);
}
