package ac.software.semantic.model;


import java.net.URLEncoder;

import org.apache.jena.query.Syntax;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.constants.TripleStoreType;

@Document(collection = "VirtuosoConfigurations")
//@TypeAlias("blazegraph")
public class BlazegraphConfiguration extends TripleStoreConfiguration {
	
	@Transient
	@JsonIgnore
	private String uriPre;
	
	public BlazegraphConfiguration() {
		super();
		
		String os = System.getProperty("os.name");
		if (os.startsWith("Windows")) {
			uriPre = "/";
		} else {
			uriPre = "";
		}
	}

	@Override
    public void connect() throws Exception { }

    private String getNormalizedUploadFolder() {
    	if (uploadFolder.endsWith("/")) {
    		return uploadFolder.substring(0, uploadFolder.length() - 1);
    	} else {
    		return uploadFolder;
    	}
    }
    
    
	@Override
    public void executePrepareLoadStatement(String folder, String file, String graph, String fileSystemDataFolder) throws Exception {
		String sparql;
	
		if (localImport) {
			sparql = "LOAD <file://" + uriPre + folder + "/" + URLEncoder.encode(file) + "> INTO GRAPH <" + graph + ">" ;
		} else if (getFileServer() != null) {
    		sparql = "LOAD <file://" + uriPre + getNormalizedUploadFolder() + "/" + URLEncoder.encode(file) + "> INTO GRAPH <" + graph + ">" ;
    	} else {
    		sparql = "LOAD <file://" + uriPre + folder + "/" + URLEncoder.encode(file) + "> INTO GRAPH <" + graph + ">" ;
    	}
    	
		UpdateProcessor up = UpdateExecutionFactory.createRemote(UpdateFactory.create(sparql), getSparqlEndpoint());
		up.execute();
				
	}

	@Override
    public void executePrepareDeleteStatement(String folder, String file, String fileSystemDataFolder) throws Exception { }

	@Override
    public void executeClearGraphStatement(String graph) throws Exception { 
		String sparql = "CLEAR GRAPH <" + graph + ">" ;
		
		UpdateProcessor up = UpdateExecutionFactory.createRemote(UpdateFactory.create(sparql), getSparqlEndpoint());
		up.execute();
						
	}
    
	@Override
    public void executeSparqlUpdateStatement(String stmt) throws Exception {
		UpdateProcessor up = UpdateExecutionFactory.createRemote(UpdateFactory.create(stmt, Syntax.syntaxSPARQL_11), getSparqlEndpoint());
		up.execute();
	}
    
	@Override
    public void executeCheckpointStatement() throws Exception { }
    
	@Override
    public void executeLoadStatement() throws Exception { }

	@Override
	public TripleStoreType getType() {
		return TripleStoreType.BLAZEGRAPH;
	}

}
