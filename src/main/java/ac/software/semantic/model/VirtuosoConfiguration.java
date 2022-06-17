package ac.software.semantic.model;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import ac.software.semantic.config.SFTPAdaptor.SftpDeleteGateway;
import ac.software.semantic.config.SFTPAdaptor.SftpUploadGateway;
import ac.software.semantic.controller.SseApplicationEvent;

@Document(collection = "VirtuosoConfigurations")
public class VirtuosoConfiguration {
	
	Logger logger = LoggerFactory.getLogger(VirtuosoConfiguration.class);

	@Id
	private ObjectId id;
   
	private ObjectId databaseId;
   
	private String name;
   
	private String sparqlEndpoint;
   
	private String fileServer;
	
	@JsonIgnore
	private String sftpUsername;
	@JsonIgnore
	private String sftpPassword;
   
	private String uploadFolder;
   
	private String isqlLocation;
	
	@JsonIgnore
	private String isqlUsername;
	@JsonIgnore
	private String isqlPassword;
	
	private String isqlFolder;
   
	@JsonIgnore
	@Value("${mapping.temp.folder}")
	private String tempFolder;

	@JsonIgnore
	private Connection conn;
	
	@JsonIgnore
	@Autowired
	private ApplicationEventPublisher applicationEventPublisher;
	
	@JsonIgnore
	private ObjectMapper mapper;
	
	public VirtuosoConfiguration() { 
		mapper = new ObjectMapper();
		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
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

	public String getSparqlEndpoint() {
		return sparqlEndpoint;
	}

	public void setSparqlEndpoint(String sparqlEndpoint) {
		this.sparqlEndpoint = sparqlEndpoint;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public String getFileServer() {
		return fileServer;
	}

	public void setFileServer(String fileServer) {
		this.fileServer = fileServer;
	}

	public String getUploadFolder() {
		return uploadFolder;
	}

	public void setUploadFolder(String uploadFolder) {
		this.uploadFolder = uploadFolder;
	}

	public String getIsqlLocation() {
		return isqlLocation;
	}

	public void setIsqlLocation(String isqlLocation) {
		this.isqlLocation = isqlLocation;
	}

	public String getIsqlUsername() {
		return isqlUsername;
	}

	public void setIsqlUsername(String isqlUsername) {
		this.isqlUsername = isqlUsername;
	}

	public String getIsqlPassword() {
		return isqlPassword;
	}

	public void setIsqlPassword(String isqlPassword) {
		this.isqlPassword = isqlPassword;
	}

	public String getIsqlFolder() {
		return isqlFolder;
	}
	
    public String getNrmalizedIsqlFolder() {
    	if (isqlFolder.endsWith("/")) {
    		return isqlFolder.substring(0, isqlFolder.length() - 1);
    	} else {
    		return isqlFolder;
    	}
    }

	public void setIsqlFolder(String isqlFolder) {
		this.isqlFolder = isqlFolder;
	}

	public String getSftpUsername() {
		return sftpUsername;
	}

	public void setSftpUsername(String sftpUsername) {
		this.sftpUsername = sftpUsername;
	}

	public String getSftpPassword() {
		return sftpPassword;
	}

	public void setSftpPassword(String sftpPassword) {
		this.sftpPassword = sftpPassword;
	}

	public boolean equals(Object obj) {
		if (obj instanceof VirtuosoConfiguration) {
			return ((VirtuosoConfiguration) obj).getName().equals(name);
		} else {
			return false;
		}
	}
	
	public int hashCode() {
		return name.hashCode();
	}

    public void connect() throws SQLException {
    	try {
    		logger.info("Connecting to Virtuoso isql " + getIsqlLocation());
			conn = DriverManager.getConnection(getIsqlLocation(), getIsqlUsername(), getIsqlPassword());
		}
    	catch (Exception e) {
			conn = null;
			e.printStackTrace();
		}
    }
    
    public Statement createStatement() throws SQLException {
    	return conn.createStatement();
    }

    public void executeStatement(String params) throws SQLException {
		try (Statement statement = conn.createStatement()){
			statement.execute(params);
		}
		catch (Exception e) {
//			logger.info("Attept to reconnect: "+count+"/"+maxTries);
			connect();
			if (conn != null) {
				try (Statement statement = conn.createStatement()){
					statement.execute(params);
				}
			}
		}
	}

    public void executeUpdateStatement(String params) throws SQLException {
    	try (Statement statement = conn.createStatement()){
			statement.executeUpdate(params);
		}
		catch (Exception e) {
//			logger.info("Attept to reconnect: "+count+"/"+maxTries);
			connect();
			if (conn != null) {
				try (Statement statement = conn.createStatement()){
					statement.executeUpdate(params);
				}
			}
		}
	}
	
    public void prepare(SftpUploadGateway sftpUploadGateway, String folder, String file, Set<String> uploaded) {
    	
//    	System.out.println("prepare " + virtuosoConfiguration.getFileServer());
    	
    	if (getFileServer() != null) {
//    		logger.info("Uploading file " + file + " to " + vc.getFileServer());
    		sftpUploadGateway.upload(new File(folder + "/" + file), this);
    		uploaded.add(file);
    		logger.info("File " + file  + " uploaded.");
    	}
    }
    
    public String lddir(String folder, String file, String graph) {
    	
    	if (getFileServer() != null) {
    		return "ld_dir('" + getNrmalizedIsqlFolder() + "', '" + file + "', '" + graph + "')";
    	} else {
    		return "ld_dir('" + folder + "', '" + file + "', '" + graph + "')";
    	}
    }
    
    public String delete(String folder, String file) {
    	
    	if (getFileServer() != null) {
    		return "DELETE FROM DB.DBA.load_list WHERE ll_file ='" + getNrmalizedIsqlFolder() + "/"  + file + "'";
    	} else {
    		return "DELETE FROM DB.DBA.load_list WHERE ll_file ='" + folder + "/"  + file + "'";
    	}
    }
    
    public void deleteFile(SftpDeleteGateway sftpDeleteGateway, String f) {
    	sftpDeleteGateway.deleteFile(f, this);
    }
    
//    private void emitMessage() {
//    	NotificationObject no = new NotificationObject("publish", "INFO", id);
//		
//		try {
//			SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
//		
//			applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
//		} catch (JsonProcessingException e) {
//			e.printStackTrace();
//		}
//    }

}
