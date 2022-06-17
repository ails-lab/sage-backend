package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


@Document(collection = "FileDocuments")
public class FileDocument {
   
   @Id
   private ObjectId id;

   private ObjectId userId;
   private ObjectId datasetId;
   
   private String name;
   private String fileName;
   
   private String uuid;
   
   private List<PublishState> publish;
   
   public FileDocument() {   }
   
   public FileDocument(ObjectId userId, String name, String uuid, ObjectId datasetId, String fileName) {
       this.userId = userId;
       this.name = name;
       this.uuid = uuid;
       this.datasetId = datasetId;
       this.fileName = fileName;
       
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
   
   public ObjectId getUserId() {
       return userId;
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


	public List<PublishState> getPublish() {
		return publish;
	}

	public void setPublish(List<PublishState> publish) {
		this.publish = publish;
	}

	public PublishState getPublishState(ObjectId databaseConfigurationId) {
		for (PublishState s : publish) {
			if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
				return s;
			}
		}
		
		PublishState s = new PublishState();
		s.setPublishState(DatasetState.UNPUBLISHED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		publish.add(s);
		
		return s;
	}

	public PublishState checkPublishState(ObjectId databaseConfigurationId) {
		for (PublishState s : publish) {
			if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
				return s;
			}
		}
		
		return null;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public synchronized void removePublishState(PublishState ps) {
		if (publish != null) {
			publish.remove(ps);
		} 
		
	}
}