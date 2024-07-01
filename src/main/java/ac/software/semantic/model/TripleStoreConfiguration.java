package ac.software.semantic.model;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.config.SFTPAdaptor.SftpDeleteGateway;
import ac.software.semantic.config.SFTPAdaptor.SftpUploadGateway;
import ac.software.semantic.model.constants.type.TripleStoreType;

@Document(collection = "VirtuosoConfigurations")
public abstract class TripleStoreConfiguration implements ConfigurationObject {
	
	Logger logger = LoggerFactory.getLogger(TripleStoreConfiguration.class);

	@Id
	protected ObjectId id;
   
	protected ObjectId databaseId;
	protected String name;
	protected String sparqlEndpoint;
	protected String fileServer;
	
	@Transient
	protected int order;
	
	protected String uploadFolder;
	
	@Transient
	@JsonIgnore
	protected String sftpUsername;
	
	@Transient
	@JsonIgnore
	protected String sftpPassword;
	
	@Transient
	@JsonIgnore	
	protected boolean localImport; 
	
	@Transient
	@JsonIgnore
	protected Map<String, String> directGraphMap;

	@Transient
	@JsonIgnore
	protected Map<String, String>  inverseGraphMap;

//	@Transient
//	@JsonIgnore
//	@Value("${mapping.temp.folder}")
//	protected String tempFolder;

//	@Transient
//	@JsonIgnore
//	@Autowired
//	protected ApplicationEventPublisher applicationEventPublisher;
	
//	@Transient
//	@JsonIgnore
//	protected ObjectMapper mapper;
	
	public TripleStoreConfiguration() { 
//		mapper = new ObjectMapper();
//		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//	    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
		
		directGraphMap = new HashMap<>();
		inverseGraphMap = new HashMap<>();
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
		if (obj instanceof TripleStoreConfiguration) {
			return ((TripleStoreConfiguration) obj).getName().equals(name);
		} else {
			return false;
		}
	}
	
	public int hashCode() {
		return name.hashCode();
	}

    public abstract void connect() throws Exception;

    public abstract void executePrepareLoadStatement(String folder, String file, String graph, String fileSystemDataFolder) throws Exception;

    public abstract void executePrepareDeleteStatement(String folder, String file, String fileSystemDataFolder) throws Exception;

    public abstract void executeClearGraphStatement(String graph) throws Exception;
    
    public abstract void executeSparqlUpdateStatement(String params) throws Exception;
    
    public abstract void executeCheckpointStatement() throws Exception;
    
    public abstract void executeLoadStatement() throws Exception;
    
    public void prepare(SftpUploadGateway sftpUploadGateway, String folder, String file, Set<String> uploaded) {
    	
//    	System.out.println("prepare " + virtuosoConfiguration.getFileServer());
    	
    	if (!localImport && getFileServer() != null) {
//    		logger.info("Uploading file " + file + " to " + getFileServer());
    		sftpUploadGateway.upload(new File(folder, file), this);
    		uploaded.add(file);
    		logger.info("File " + new File(folder, file)  + " uploaded to " + getFileServer());
    	}
    }
    
    public void deleteFile(SftpDeleteGateway sftpDeleteGateway, String f) {
    	sftpDeleteGateway.deleteFile(f, this);
    }
    
    public void deleteFiles(SftpDeleteGateway sftpDeleteGateway, Set<String> files) {
    	for (String f : files) {
    		sftpDeleteGateway.deleteFile(f, this);
    	}
    }

    public void deleteFiles(SftpDeleteGateway sftpDeleteGateway, Collection<String> files) {
    	for (String f : files) {
    		sftpDeleteGateway.deleteFile(f, this);
    	}
    }
    
	public boolean isLocalImport() {
		return localImport;
	}

	public void setLocalImport(boolean localImport) {
		this.localImport = localImport;
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
	
	public abstract TripleStoreType getType();

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	
	public void addGraph(String uri, String identifier) {
		directGraphMap.put(uri, identifier);
		inverseGraphMap.put(identifier, uri);
	}
	
	public String getGraphUri(String identifier) {
		return inverseGraphMap.get(identifier);
	}
	
	public String getGraphIdentifier(String uri) {
		return directGraphMap.get(uri);
	}

}

