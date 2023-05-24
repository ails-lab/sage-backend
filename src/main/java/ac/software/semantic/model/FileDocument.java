package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.model.state.PublishState;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "FileDocuments")
public class FileDocument {
   
   @Id
   private ObjectId id;

   private ObjectId userId;
   private ObjectId datasetId;
   
   private String name;
//   private String fileName;
//   private List<String> contentFileNames;
   
   private String uuid;
   
   private List<FilePublishState> publish;
   
   private FileExecuteState execute;
   
   private Date updatedAt;
   
//   private String url;
   
//   private ObjectId fileSystemConfigurationId;

   
   public FileDocument() {   
       publish = new ArrayList<>();
   }

   public ObjectId getId() {
       return id;
   }
 
   public String getName() {
       return name;
   }
   
   public void setName(String name) {
       this.name = name;
   }

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}


	public List<FilePublishState> getPublish() {
		return publish;
	}

	public void setPublish(List<FilePublishState> publish) {
		this.publish = publish;
	}

	public FilePublishState getPublishState(ObjectId databaseConfigurationId) {
		for (FilePublishState s : publish) {
			if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
				return s;
			}
		}
		
		FilePublishState s = new FilePublishState();
		s.setPublishState(DatasetState.UNPUBLISHED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		publish.add(s);
		
		return s;
	}

	public FilePublishState checkPublishState(ObjectId databaseConfigurationId) {
		for (FilePublishState s : publish) {
			if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
				return s;
			}
		}
		
		return null;
	}

//	public String getFileName() {
//		return fileName;
//	}
//
//	public void setFileName(String fileName) {
//		this.fileName = fileName;
//	}
	
	public synchronized void removePublishState(PublishState ps) {
		if (publish != null) {
			publish.remove(ps);
		} 
		
	}

//	public ObjectId getFileSystemConfigurationId() {
//		return fileSystemConfigurationId;
//	}
//
//	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
//		this.fileSystemConfigurationId = fileSystemConfigurationId;
//	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

//	public List<String> getContentFileNames() {
//		return contentFileNames;
//	}
//
//	public void setContentFileNames(List<String> contentFileNames) {
//		this.contentFileNames = contentFileNames;
//	}

	public FileExecuteState getExecute() {
		return execute;
	}

	public void setExecute(FileExecuteState execute) {
		this.execute = execute;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}
	
	public ProcessStateContainer getCurrentPublishState(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) {
			PublishState ps = checkPublishState(vc.getId());
			if (ps != null) {
				return new ProcessStateContainer(ps, vc);
			}
		}
		
		return null;
	}
//
//	public String getUrl() {
//		return url;
//	}
//
//	public void setUrl(String url) {
//		this.url = url;
//	}	
}