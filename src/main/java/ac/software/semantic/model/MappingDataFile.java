package ac.software.semantic.model;

import org.bson.types.ObjectId;

public class MappingDataFile {

	private ObjectId fileSystemConfigurationId;
	
	private String filename;

	public MappingDataFile(String filename, ObjectId fileSystemConfigurationId) {
		this.filename = filename;
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}
	
	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
}
