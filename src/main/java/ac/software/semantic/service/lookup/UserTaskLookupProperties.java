package ac.software.semantic.service.lookup;

import org.bson.types.ObjectId;

public class UserTaskLookupProperties implements LookupProperties {
	
	private ObjectId fileSystemConfigurationId;
	
	public UserTaskLookupProperties() {
		
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}
	
}
