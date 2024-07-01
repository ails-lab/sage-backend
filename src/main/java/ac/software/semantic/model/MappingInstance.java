package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.base.ValidatableDocument;
import ac.software.semantic.model.constants.state.ValidatingState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.ValidateState;
import ac.software.semantic.service.exception.TaskConflictException;

public class MappingInstance extends MappingExecutePublishDocument<MappingExecuteState, MappingPublishState> implements ValidatableDocument<ValidateState> {

	@Id
	private ObjectId id;

	private String uuid;
	private String identifier;
	
	private List<ParameterBinding> binding;

	private List<MappingDataFile> dataFiles;

	private List<ValidateState> validate;
	
	private boolean active;

	public MappingInstance() {
		id = new ObjectId();

		binding = new ArrayList<>();
	}

	public MappingInstance(List<String> parameters) {
		binding = new ArrayList<>();
		for (String s : parameters) {
			binding.add(new ParameterBinding(s, ""));
		}

	}

	public ObjectId getId() {
		return id;
	}

	public boolean hasBinding() {
		return binding != null && binding.size() > 0;
	}

	public List<ParameterBinding> getBinding() {
		return binding;
	}

	public void setBinding(List<ParameterBinding> binding) {
		this.binding = binding;
	}

	public List<MappingDataFile> getDataFiles() {
		return dataFiles;
	}

	public void setDataFiles(List<MappingDataFile> dataFiles) {
		this.dataFiles = dataFiles;
	}
	
	public void addDataFile(MappingDataFile dataFile) throws TaskConflictException {
		if (this.dataFiles == null) {
			this.dataFiles = new ArrayList<>();
		}

		for (int i = 0; i < dataFiles.size();i++) {
			MappingDataFile mdf = dataFiles.get(i);
			if (mdf.getFilename().equals(dataFile.getFilename()) && mdf.getFileSystemConfigurationId().equals(dataFile.getFileSystemConfigurationId())) {
				throw new TaskConflictException("An attachment with the same name already exists.");
			}
		}

		this.dataFiles.add(dataFile);
	}
	
	
	public void removeDataFile(MappingDataFile dataFile) {
		if (dataFiles != null) {
			for (int i = 0; i < dataFiles.size(); i++) {
				MappingDataFile mdf = dataFiles.get(i);
				if (mdf.getFilename().equals(dataFile.getFilename()) && mdf.getFileSystemConfigurationId().equals(dataFile.getFileSystemConfigurationId())) {
					dataFiles.remove(i);
					break;
				}
			}
			
			if (dataFiles.size() == 0) {
				dataFiles = null;
			}
		}
	}
	
	public List<MappingDataFile> checkDataFiles(FileSystemConfiguration fileSystemConfiguration) {
		
		List<MappingDataFile> res = new ArrayList<>();
		if (dataFiles != null) {		
			for (MappingDataFile s : dataFiles) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfiguration.getId())) {
					res.add(s);
				}
			}
		}
		
//		if (res.size() > 0) {
			return res;
//		} else {
//			return null;
//		}
	}	
	
	@Override
	public List<ValidateState> getValidate() {
		return validate;
	}

	@Override
	public void setValidate(List<ValidateState> validate) {
		this.validate = validate;
	}

	@Override
	public ValidateState getValidateState(ObjectId fileSystemConfigurationId) {
		if (validate != null) {
			for (ValidateState s : validate) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					return s;
				}
			}
		} else {
			validate = new ArrayList<>();
		}
		
		ValidateState s = new ValidateState();
		s.setValidateState(ValidatingState.NOT_VALIDATED);
		s.setFileSystemConfigurationId(fileSystemConfigurationId);
		validate.add(s);
		
		return s;
	}

	@Override
	public ValidateState checkValidateState(ObjectId fileSystemConfigurationId) {
		if (validate != null) {		
			for (ValidateState s : validate) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}
	
	@Override
	public void deleteValidateState(ObjectId fileSystemConfigurationId) {
		if (validate != null) {
			for (int i = 0; i < validate.size(); i++) {
				if (validate.get(i).getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					validate.remove(i);
					break;
				}
			}
			
			if (validate.size() == 0) {
				validate = null;
			}
		}
	}

	@Override
	public String getUuid() {
		return uuid;
	}
	
	@Override
	public ObjectId getUserId() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
}
