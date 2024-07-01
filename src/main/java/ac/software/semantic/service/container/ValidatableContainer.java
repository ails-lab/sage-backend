package ac.software.semantic.service.container;

import java.util.Date;
import java.util.List;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.model.ValidationResult;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.base.ValidatableDocument;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.ValidateState;
import ac.software.semantic.payload.response.Response;

public interface ValidatableContainer<D extends SpecificationDocument, F extends Response> extends BaseContainer<D,F> {

	public ValidatableDocument<ValidateState> getValidateDocument();

	public List<PrototypeDocument> getValidatorDocument() throws Exception;
	
	public FileSystemConfiguration getContainerFileSystemConfiguration();

	public TaskType getValidateTask();
	
	public ValidationResult validate() throws Exception;
	
	default ValidateState getValidateState() {
		return getValidateDocument().getValidateState(getContainerFileSystemConfiguration().getId());
	}
	
	default ValidateState checkValidateState() {
		return getValidateDocument().checkValidateState(getContainerFileSystemConfiguration().getId());
	}
	
	default void removeValidateState() {
		getValidateDocument().deleteValidateState(getContainerFileSystemConfiguration().getId());
	}
	
	default public void failValidation() throws Exception {			
		update(iec -> {			
			ValidateState ies = ((ValidatableContainer<D,F>)iec).getValidateDocument().checkValidateState(getContainerFileSystemConfiguration().getId());
	
			if (ies != null) {
				ies.fail(new Date(), "Unknown error.");
			}
		});
	}

}
