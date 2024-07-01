package ac.software.semantic.model;

import java.io.File;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;

@Document(collection = "FileSystemConfigurations")
public class FileSystemConfiguration implements ConfigurationObject {
   @Id
   private ObjectId id;
   
   private ObjectId databaseId;
   
   private String name;
   
	@Transient
	protected int order;

   private String dataFolder;
   
   private String ip;
   
   public FileSystemConfiguration() { }

   	public ObjectId getId() {
   		return id;
   	}
	   
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public String getDataFolder() {
		return dataFolder; 
	}

	public String getUserDataFolder(UserPrincipal currentUser) {
		if (dataFolder.endsWith("/") || dataFolder.endsWith("\\")) {
			return dataFolder + currentUser.getUsername() + "/";
		} else {
			return dataFolder + File.separatorChar + currentUser.getUsername() + "/"; 
		}
	}
	
	public String getPublicFolder() {
		return dataFolder + File.separatorChar + "public" + File.separatorChar; 
	}

	public void setDataFolder(String folder) {
		this.dataFolder = folder;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}


}
