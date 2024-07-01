package ac.software.semantic.service.lookup;

import org.bson.types.ObjectId;

public class FileLookupProperties implements GroupLookupProperties {
	
	private ObjectId fileSystemConfigurationId;
	
	private Integer group;

	@Override
	public Integer getGroup() {
		return group;
	}

	@Override
	public void setGroup(Integer group) {
		this.group = group;
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}
	
}
