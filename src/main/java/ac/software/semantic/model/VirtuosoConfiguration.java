package ac.software.semantic.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.constants.TripleStoreType;

@Document(collection = "VirtuosoConfigurations")
//@TypeAlias("virtuoso")
public class VirtuosoConfiguration extends TripleStoreConfiguration {

	private String isqlLocation;
	
	private String isqlFolder; // for sftp upload
	private String isqlLocalDataFolder; // data folder where produced files are saved
	
	@Transient
	@JsonIgnore
	private String isqlUsername;
	
	@Transient
	@JsonIgnore
	private String isqlPassword;
	
	@Transient
	@JsonIgnore
	private Connection conn;
	
	public VirtuosoConfiguration() {
		super();
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
	
	public void setIsqlFolder(String isqlFolder) {
		this.isqlFolder = isqlFolder;
	}

	@Override
    public void connect() throws Exception {
    	try {
    		logger.info("Connecting to Virtuoso isql " + getIsqlLocation());
			conn = DriverManager.getConnection(getIsqlLocation(), getIsqlUsername(), getIsqlPassword());
		}
    	catch (Exception e) {
			conn = null;
			e.printStackTrace();
		}
    }

    private String getNormalizedIsqlFolder(String folder, String fileSystemDataFolder) throws Exception {
    	if (localImport) { // use data directly from sage output
    		String importFolder = null;
    		
        	if (!fileSystemDataFolder.endsWith("/")) {
        		fileSystemDataFolder = fileSystemDataFolder + "/";
        	}
    		
    		if (folder.startsWith(fileSystemDataFolder)) {
    			importFolder = folder.substring(fileSystemDataFolder.length());
    		} else {
    			throw new Exception("Incorrect folder " + folder); // shouldn't happer
    		}
    		
        	if (isqlLocalDataFolder.endsWith("/")) {
        		importFolder = isqlLocalDataFolder + importFolder;
        	} else {
        		importFolder = isqlLocalDataFolder + "/" + importFolder;
        	}
        	
	    	if (importFolder.endsWith("/")) {
	    		return importFolder.substring(0, importFolder.length() - 1);
	    	} else {
	    		return importFolder;
	    	}
    		
    	} else { // use uploaded data 
	    	if (isqlFolder.endsWith("/")) {
	    		return isqlFolder.substring(0, isqlFolder.length() - 1);
	    	} else {
	    		return isqlFolder;
	    	}
    	}
    }
    
//    private Statement createStatement() throws SQLException {
//    	return conn.createStatement();
//    }
    
    private void executeIsqlStatement(String stmt) throws Exception {
    	if (conn == null) {
    		connect();
    	}
    	
//    	System.out.println("Executing " + stmt);
    	
		try (Statement statement = conn.createStatement()){
			statement.execute(stmt);
		}
		catch (Exception e) {
	//		logger.info("Attempt to reconnect: "+count+"/"+maxTries);
			connect();
			if (conn != null) {
				try (Statement statement = conn.createStatement()){
					statement.execute(stmt);
				}
			}
		}
    }
    
	private void executeIsqlUpdateStatement(String stmt) throws Exception {
    	if (conn == null) {
    		connect();
    	}
		
		try (Statement statement = conn.createStatement()){
			statement.executeUpdate(stmt);
		}
		catch (Exception e) {
	//		logger.info("Attept to reconnect: "+count+"/"+maxTries);
			connect();
			if (conn != null) {
				try (Statement statement = conn.createStatement()){
					statement.executeUpdate(stmt);
				}
			}
		}
	}

	@Override
    public void executeSparqlUpdateStatement(String stmt) throws Exception {
    	executeIsqlStatement("sparql " + stmt);
	}
    
	@Override
    public void executeClearGraphStatement(String graph) throws Exception {
    	executeIsqlStatement("sparql define sql:log-enable 3 clear graph <" + graph + ">");
	}

	@Override
    public void executeCheckpointStatement() throws Exception {
    	executeIsqlStatement("checkpoint");
	}

	@Override
    public void executeLoadStatement() throws Exception {
    	executeIsqlStatement("rdf_loader_run()");
	}

	@Override
    public void executePrepareLoadStatement(String folder, String file, String graph, String fileSystemDataFolder) throws Exception {
    	executeIsqlStatement(lddir(folder, file, graph, fileSystemDataFolder));
    }

    
    private String lddir(String folder, String file, String graph, String fileSystemDataFolder) throws Exception {
    	if (localImport || getFileServer() != null) {
    		return "ld_dir('" + getNormalizedIsqlFolder(folder, fileSystemDataFolder) + "', '" + file + "', '" + graph + "')";
    	} else {
    		return "ld_dir('" + folder + "', '" + file + "', '" + graph + "')";
    	}
    }
    
    @Override
    public void executePrepareDeleteStatement(String folder, String file, String fileSystemDataFolder) throws Exception {
    	executeIsqlUpdateStatement(prepareDelete(folder, file, fileSystemDataFolder));
    }
    
    private String prepareDelete(String folder, String file, String fileSystemDataFolder) throws Exception {
    	if (getFileServer() != null) {
    		return "DELETE FROM DB.DBA.load_list WHERE ll_file ='" + getNormalizedIsqlFolder(folder, fileSystemDataFolder) + "/"  + file + "'";
    	} else {
    		return "DELETE FROM DB.DBA.load_list WHERE ll_file ='" + folder + "/"  + file + "'";
    	}
    }

	public String getIsqlLocalDataFolder() {
		return isqlLocalDataFolder;
	}

	public void setIsqlLocalDataFolder(String isqlLocalDataFolder) {
		this.isqlLocalDataFolder = isqlLocalDataFolder;
	}
	
	@Override
	public TripleStoreType getType() {
		return TripleStoreType.OPENLINK_VIRTUOSO;
	}
}
