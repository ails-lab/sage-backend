package ac.software.semantic.service;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.config.AppConfiguration;
import ac.software.semantic.config.SFTPAdaptor.SftpDeleteGateway;
import ac.software.semantic.config.SFTPAdaptor.SftpUploadGateway;
import ac.software.semantic.config.VocabulariesBean;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ExecuteState;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.MappingType;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.ResourceOptionType;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.model.VocabularizerDocument;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.monitor.FileSystemOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SACCVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SOAVocabulary;


@Component
public class VirtuosoJDBC {

	Logger logger = LoggerFactory.getLogger(VirtuosoJDBC.class);
	
    @Value("${mapping.execution.folder}")
    private String mappingsFolder;
    
    @Value("${annotation.execution.folder}")
    private String annotationsFolder;

    @Value("${vocabularizer.execution.folder}")
    private String vocabularizerFolder;

    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;    

    @Value("${annotation.manual.folder}")
    private String manualFolder;

    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

    @Value("${backend.server}")
    private String server;
    
	@Autowired
	DatasetRepository datasetRepository;
	
	@Autowired
	MappingRepository mappingRepository;
	
	@Autowired
	Virtuoso ivirtuoso;
	
	@Autowired
	SchemaService schemaService;

	
	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Autowired
	private Environment env;
	
//    private Map<String,VirtuosoHandler> virtuosoHandlers;
    
    private FileSystemConfiguration fileSystemConfiguration;
    
    @Value("${mapping.temp.folder}")
    private String tempFolder;

    @Autowired
    private SftpUploadGateway sftpUploadGateway;
    
    @Autowired
    private SftpDeleteGateway sftpDeleteGateway;

	@Autowired
	@Qualifier("date-format")
	private SimpleDateFormat dateFormat;
  
	@Autowired
	ApplicationContext context;

	@Autowired
	@Qualifier("virtuoso-configuration") 
	Map<String,VirtuosoConfiguration> virtuosoConfigurations;
    
    public VirtuosoJDBC(@Qualifier("virtuoso-configuration") Map<String,VirtuosoConfiguration> vcs,
    		            @Qualifier("filesystem-configuration") FileSystemConfiguration fsc,
    		            @Value("${virtuoso.isql.username}") String isqlUsername,
    		            @Value("${virtuoso.isql.password}") String isqlPassword) throws SQLException {
    	
    	for (VirtuosoConfiguration vc : vcs.values()) {
    		vc.connect();
    	}
    	
    	this.fileSystemConfiguration = fsc;
    }

    
    private String getMappingsFolder(UserPrincipal currentUser) {
    	if (mappingsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder.substring(0, mappingsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder;
    	}
    }

    private String getUploadsFolder(UserPrincipal currentUser) {
    	if (uploadsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder.substring(0, uploadsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder;
    	}    	
    }
    
    private String getAnnotationsFolder(UserPrincipal currentUser) {
    	if (annotationsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder.substring(0, annotationsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder;
    	}    	
    }
    
    private String getFolder(UserPrincipal currentUser, String rootFolder, String subFolder) {
    	if (rootFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + rootFolder + subFolder;
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + rootFolder + "/" + subFolder;
    	}    	
    }
    
    private String getVocabularizerFolder(UserPrincipal currentUser) {
    	if (vocabularizerFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizerFolder.substring(0, vocabularizerFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizerFolder;
    	}    	
    }
    
    private String getManualFolder(UserPrincipal currentUser) {
    	if (manualFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + manualFolder.substring(0, manualFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + manualFolder;
    	}    	
    }
    
    //cannot published two datasets at the same time : problem for linking annotations to annotations sets  
    public synchronized boolean publish(UserPrincipal currentUser, String virtuoso, Dataset dataset, List<MappingDocument> mappings, List<FileDocument> files) throws Exception {
    	
    	VirtuosoConfiguration vc = virtuosoConfigurations.get(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
    	
    	String mf = getMappingsFolder(currentUser);
    	
    	boolean header = false;
    	
		for (MappingDocument map : mappings) {
			boolean hi = !map.getParameters().isEmpty();

			for (MappingInstance mi : map.getInstances()) {
    			ExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());

				for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
					String fileName = map.getUuid() + (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig";

					vc.executeUpdateStatement(vc.delete(mf + "/" + dataset.getUuid(), fileName));
					//for compatibility
					vc.executeUpdateStatement(vc.delete(mf, fileName));


					String targetGraph = null;
	    			if (dataset.getType().equals("annotation-dataset")) {
	    				targetGraph = dataset.getAsProperty();
					} else {
		    			if (map.getType() == MappingType.HEADER) {
		    				targetGraph = SEMAVocabulary.contentGraph.toString();
			    			header = true;
		    			} else if (map.getType() == MappingType.CONTENT) {
		    				targetGraph = SEMAVocabulary.getDataset(dataset.getUuid()).toString();
				    	}
					}

	    			if (targetGraph != null) {
	    				if (new File(mf + "/" + dataset.getUuid() + "/" + fileName).exists()) {
	    					vc.prepare(sftpUploadGateway, mf + "/" + dataset.getUuid(), fileName, uploadedFiles);
	    					vc.executeStatement(vc.lddir(mf + "/" + dataset.getUuid(), fileName, targetGraph));
	    				} else {
	    					// for compatibility
	    					vc.prepare(sftpUploadGateway, mf, fileName, uploadedFiles);
       						vc.executeStatement(vc.lddir(mf, fileName, targetGraph));
	    				}

	    			}
    			}
	    	}
		}
		
		String uf = getUploadsFolder(currentUser) + "/";
		
		for (FileDocument file : files) {
    		File folder = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder, file.getId().toString());
    		
    		for (File f : folder.listFiles()) {
//        			stmt.executeUpdate(delete(uf + file.getId().toString(), f.getName()));
    			vc.executeUpdateStatement(vc.delete(uf + file.getId().toString(), f.getName()));
    		}
    		
    		String targetGraph = null;
			if (dataset.getType().equals("annotation-dataset")) {
				targetGraph = dataset.getAsProperty();
			} else {
				targetGraph = SEMAVocabulary.getDataset(dataset.getUuid()).toString();
			}
    		
    		for (File f : folder.listFiles()) {
				vc.prepare(sftpUploadGateway, uf + file.getId().toString(), f.getName(), uploadedFiles);
//    				stmt.execute(lddir(uf + file.getId().toString(), f.getName(), targetGraph));
				vc.executeStatement(vc.lddir(uf + file.getId().toString(), f.getName(), targetGraph));
    		}
		}
		
    	vc.executeStatement( "rdf_loader_run()");
	    
	    for (String f : uploadedFiles) {
			vc.deleteFile(sftpDeleteGateway, f);
	    }
	    
	    if (dataset.getType().equals("annotation-dataset")) {
    		String insert = 
    			"INSERT { GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
    					"<" + SEMAVocabulary.getAnnotationSet(dataset.getUuid()).toString() + "> <" + DCTVocabulary.hasPart.toString() + "> ?p } }" +
    					"WHERE { GRAPH <" + dataset.getAsProperty() + "> { " +
    					       " ?p a <" + OAVocabulary.Annotation + "> . } " +
    					"FILTER NOT EXISTS { GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
    						   " ?g <" + DCTVocabulary.hasPart.toString() + "> ?p } } } ";
    					
//	    		stmt.execute("sparql " + insert);
    		vc.executeStatement( "sparql " + insert);
	    } 
	    
	    publishPostprocess(currentUser, virtuoso, dataset, header);
	    
    	vc.executeStatement( "checkpoint");
	    	
		return true;

    }

    public boolean unpublishMapping(UserPrincipal currentUser, String virtuoso, Dataset dataset, MappingDocument mapping, MappingInstance mi) throws Exception {
    	
    	VirtuosoConfiguration vc = virtuosoConfigurations.get(virtuoso);
    	
    	String mf = getMappingsFolder(currentUser) + "/" + dataset.getUuid() + "/";
    	
		boolean hi = !mapping.getParameters().isEmpty();

		ExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());

		for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
			String fileName = mapping.getUuid() + (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig";

			File f = new File(mf + fileName);
				
			boolean ok = false;
			if (!f.exists()) {
				// for compatibility
				f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + getMappingsFolder(currentUser) + "/" + fileName);
			}
				
			if (!f.exists()) {
				logger.warn("Failed to unpublish " + f.getName());
				return false;
			}
			
//			org.apache.jena.query.Dataset ds = RDFDataMgr.loadDataset(f.getAbsolutePath());
//			Model model = ds.getDefaultModel();
//			for (Map.Entry<String, String> entry : model.getNsPrefixMap().entrySet()) {
//				model.removeNsPrefix(entry.getKey());
//			}
//				
//			ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			RDFDataMgr.write(baos, model, Lang.TTL);
//
//			String data = new String(baos.toByteArray());
//			String sparql = 
//			   "DELETE DATA FROM <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> {\n " +
//			      new String(baos.toByteArray()) + " }";

			
//			System.out.println(sparql);
			
			
			
			try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			    String line;
		    	StringBuffer entry = new StringBuffer("sparql DELETE DATA FROM <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> {\n ");
		    	int count = 0;
			    while ((line = br.readLine()) != null) {
			    	
			    	if (line.length() == 0) {
					    if (count > 0) {
				    		entry.append("}");
				    		
				    		System.out.println(entry);

				    		vc.executeStatement(entry.toString());
					    }
			    		
			    		entry = new StringBuffer("sparql DELETE DATA FROM <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> {\n ");
			    	} else {
			    		count++;
			    		entry.append(line);
			    	}
			    }
			    
			    if (count > 0) {
			    	entry.append("}");
		    		
		    		System.out.println(entry);
			    	vc.executeStatement(entry.toString());
			    }
			}
			
			
//			vc.executeUpdateStatement(sparql);
		}
	    
//    	vc.executeStatement( "checkpoint");
	    	
		return true;

    }

	@Autowired
	ResourceLoader resourceLoader;
    
	private boolean executeSaturate(UserPrincipal currentUser, String virtuoso, Dataset dataset) throws Exception {

		VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
		
		String mf = getMappingsFolder(currentUser); 
		
		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(mf + File.separatorChar, dataset.getUuid() + "-owl-sameAs-saturate", shardSize)) {

			Map<String, Object> params = new HashMap<>();

			params.put("iigraph", SEMAVocabulary.getDataset(dataset.getUuid()).toString());
			params.put("iirdfsource", vc.getSparqlEndpoint());

			String d2rml = env.getProperty("saturator.owl-sameAs.d2rml");
			String str;
			try (InputStream inputStream = resourceLoader.getResource("classpath:" + d2rml).getInputStream()) {
				str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			}


			Executor exec = new Executor(outhandler, safeExecute);

//			try (ExecuteMonitor em = new ExecuteMonitor("saturator", id, null, applicationEventPublisher)) {
//				exec.setMonitor(em);

				D2RMLModel rmlMapping = D2RMLModel.readFromString(str);

//				SSEController.send("annotator", applicationEventPublisher, this, new ExecuteNotificationObject(id, null,
//						ExecutionInfo.createStructure(rmlMapping), executeStart));

				logger.info("Saturator started -- dataset : " + dataset.getUuid());

				exec.execute(rmlMapping, params);

//				SSEController.send("annotator", applicationEventPublisher, this, new NotificationObject("execute",
//						MappingState.EXECUTED.toString(), id, null, executeStart, executeFinish, subjects.size()));

				logger.info("Saturator executed -- dataset : " + dataset.getUuid() + ", shards: " + outhandler.getShards());
				
				Set<String> uploadedFiles = new HashSet<>();
		    	
				for (int i = 0; i < Math.max(1, outhandler.getShards()); i++) {
					String fileName = dataset.getUuid() + "-owl-sameAs-saturate" + (i == 0 ? "" : "_#" + i) + ".trig";
	    				
		   			vc.executeUpdateStatement(vc.delete(mf, fileName));
		    				
					String targetGraph = SEMAVocabulary.getDataset(dataset.getUuid()).toString();
					vc.prepare(sftpUploadGateway, mf, fileName, uploadedFiles);
 					vc.executeStatement(vc.lddir(mf, fileName, targetGraph));
				}
					
		    	vc.executeStatement( "rdf_loader_run()");
			    
			    for (String f : uploadedFiles) {
					vc.deleteFile(sftpDeleteGateway, f);
			    }
				
			    logger.info("Saturator completed -- dataset : " + dataset.getUuid());
			    
				return true;

			} catch (Exception ex) {
				ex.printStackTrace();
				
				logger.info("Saturator failed -- id: " + dataset.getUuid());
				
//				exec.getMonitor().currentConfigurationFailed();

				throw ex;
			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//
////			SSEController.send("annotator", applicationEventPublisher, this,
////					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), id, null, null, null));
//
//			return false;
//		}

	}
	
    
    private void publishPostprocess(UserPrincipal currentUser, String virtuoso, Dataset dataset, boolean header) throws Exception {
    	
    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
		SSLContext sslContext = new SSLContextBuilder()
			      .loadTrustMaterial(null, new TrustStrategy() {
					@Override
					public boolean isTrusted(X509Certificate[] certificate, String authType)
							throws CertificateException {
						return true;
					}
				}).build();
			 
		HttpClient client = HttpClients.custom()
			      .setSSLContext(sslContext)
			      .setSSLHostnameVerifier(new NoopHostnameVerifier())
			      .build();
		
	    boolean isSKOS = false;
	   	String endpoint = null;

		if (dataset.getType().equals("annotation-dataset")) {
			String insert = 
			   "sparql insert { graph <" + SEMAVocabulary.annotationGraph + "> { " +
                    "<" + SEMAVocabulary.getAnnotationSet(dataset.getUuid()) + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . } }";
	   		vc.executeStatement( insert);
		} else if (dataset.getType().equals("alignment-dataset")) {
			String insert = 
				"sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " +
			                   "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . ";
					
 		    for (String s : dataset.getTypeUri()) {
			   insert  += "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> a <" + s + "> . ";
		    }
 		   
 		    boolean bidirectional =  false;
 		    ResourceOption bro =  dataset.getOptionsByType(ResourceOptionType.BIDIRECTIONAL);
 		    if (bro != null && bro.getValue().toString().equalsIgnoreCase("true")) {
 		    	bidirectional = true;
 		    }
 		    
 		    String source = dataset.getLinkByType(ResourceOptionType.SOURCE).getValue().toString();
 		    String target = dataset.getLinkByType(ResourceOptionType.TARGET).getValue().toString();
 		    
 		    insert  += "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + SEMAVocabulary.isAlignmentOf.toString() + "> [ <" + SEMAVocabulary.source + "> <" + source + "> ; <" + SEMAVocabulary.target + "> <" + target + "> ] . ";
 		    
 		    insert += " } }";
 		    
	   		vc.executeStatement(insert);
 		   	
 		   	if (bidirectional) {
 		    	executeSaturate(currentUser, virtuoso, dataset);

 				String sinsert = "sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { ";
 		    	sinsert  += "<" + SEMAVocabulary.getDataset(dataset.getUuid())  + "> <" + SEMAVocabulary.isAlignmentOf + "> [ <" + SEMAVocabulary.source + "> <" + target + "> ; <" + SEMAVocabulary.target + "> <" + source + "> ] . ";
 		    	sinsert += " } }";
 		    	
 		   		vc.executeStatement(sinsert);
 		   	}
 		    
//	 		    if (bidirectional) {
//	 			  logger.info("Saturating bidirectional alignment dataset." );
//	 		   
//	 			  String bidir = "sparql insert { graph <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> { ?p <" + OWLVocabulary.sameAs + "> ?q } } WHERE { graph <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> { ?q <" + OWLVocabulary.sameAs + "> ?p } }";
//
//	 			  stmt.execute(bidir);
//	 		    }
 		    
		} else if (header) {
    	   	
			String insert = 
			   "sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " +
	                   "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . ";
			
//			insert  += "<" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> <" + DCVocabulary.issued + "> \"" + dateFormat.format(new Date()) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
			
 		    for (String s : dataset.getTypeUri()) {
			   insert  += "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> a <" + s + "> . ";
		    }
 		    
 		    for (ResourceOption ro : dataset.getLinks()) {
 		    	insert  += "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + ResourceOptionType.getProperty(ro.getType()) + "> <" + ro.getValue() + "> . ";
 		    }

 		    String remote = "SELECT ?endpoint FROM <" + SEMAVocabulary.contentGraph + "> " +  
 		            " { <" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/endpoint" : SEMAVocabulary.endpoint ) + "> ?endpoint }";
	    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(remote, Syntax.syntaxARQ))) {
	   		
 		    	ResultSet rs = qe.execSelect();
 		    	while (rs.hasNext()) {
 		    	   endpoint = rs.next().get("endpoint").toString();
 				}
 		    	
 		    	if (endpoint != null) {
 				   insert += "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> a <" + SEMAVocabulary.RemoteDataset + "> . ";
 		    	}
	    	}
	    	
		    if (dataset.getTypeUri().contains(SEMAVocabulary.DataCatalog.toString())) {
		    	for (ObjectId memberId : dataset.getDatasets()) {
		    		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(memberId, new ObjectId(currentUser.getId()));
		    		
		    		if (doc.isPresent()) {
			    		insert  +=	"<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + DCTVocabulary.hasPart + "> <" + SEMAVocabulary.getDataset(doc.get().getUuid())  + "> . ";
		    		}
		    	}	
		    }
		    
// 		    	System.out.println("ENDPOINT" + endpoint);
	    	
 		    // check if thesaurus is SKOS
 		    if (dataset.getTypeUri().contains(SEMAVocabulary.ThesaurusCollection.toString())) {
 		    	String sparql =
 		            "ASK " + (endpoint == null ? "FROM <" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> " : "") +  
 		            " { ?p a <" + SKOSVocabulary.Concept + "> }";
 		    	
//	 		    	System.out.println(sparql);
 		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ), client)) {
	
	 		    	if (qe.execAsk()) {
	 				   insert += "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> a <" + SEMAVocabulary.SKOSThesaurus + "> . ";
	 				   isSKOS = true;
	 				}
 		    	}
 		    	
 		    	sparql =
 		    			"ASK " + (endpoint == null ? "FROM <" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> " : "") +  
	 		            " { ?p a <http://www.w3.org/2002/07/owl#Ontology> }";
	 		    	
//	 		    	System.out.println(sparql);
	 		    try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ), client)) {
		
		 		    if (qe.execAsk()) {
		 			   insert += "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> a <" + SEMAVocabulary.OWLOntology + "> . ";
		 			}
	 		    }			 		    	
 		    }	
 		    
            insert += " } } ";
            
   	   		vc.executeStatement( insert);
		}
		
		if (!dataset.getType().equals("annotation-dataset")) {
			String delete = 
					   "sparql delete where { graph <" + SEMAVocabulary.contentGraph + "> { " +
			                   "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + DCTVocabulary.issued + "> ?date } } ";
		   	
	   		vc.executeStatement( delete);
		   	
			String insert = 
			   "sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " +
	                "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + DCTVocabulary.issued + "> \"" + dateFormat.format(new Date()) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . } } ";
			
	   		vc.executeStatement(insert);
		}
		
		// compute schema 
		if (dataset.getTypeUri().contains(SEMAVocabulary.VocabularyCollection.toString()) || 
				dataset.getTypeUri().contains(SEMAVocabulary.DataCollection.toString())) {
			
			logger.info("Computing schema for " + SEMAVocabulary.getDataset(dataset.getUuid()));
			
			try {
				Model schema = schemaService.buildSchema(SEMAVocabulary.getDataset(dataset.getUuid()).toString(), true);
				schema.clearNsPrefixMap();
				
				Writer sw = new StringWriter();
	
				RDFDataMgr.write(sw, schema, RDFFormat.TTL) ;
				
				String insert = 
						"sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " + sw.toString() + " } }" ;
				
				vc.executeStatement(insert);
			} catch (Exception ex) {
				logger.error("Failed to compute schema for " + SEMAVocabulary.getDataset(dataset.getUuid()) +": " + ex.getMessage());
			}
			
			String sparql = legacyUris ?
		    		"SELECT ?identifier WHERE { " +
		    		  "GRAPH <" + SEMAVocabulary.contentGraph + "> {" +
		    		    "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <http://purl.org/dc/elements/1.1/identifier> ?identifier } } " : 
		    		"SELECT ?identifier WHERE { " +
		    		  "GRAPH <" + SEMAVocabulary.contentGraph + "> {" +
		    		  "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + DCTVocabulary.identifier + "> ?identifier } } " ;
			
			
			//add sparql endpoint 
	    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
	    		ResultSet rs = qe.execSelect();
	    		while (rs.hasNext()) {
	    			RDFNode id = rs.next().get("identifier");
	    			if (id.isLiteral()) {
	    				String insert = 
	    						"sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " + 
	    				        "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + ">  <" + VOIDVocabulary.sparqlEndpoint + "> <" + server + "/api/content/" + id + "/sparql> } }" ;
	    				
	    				vc.executeStatement(insert);
	    				break;
	    			}
	    		}
	    	}				
		}
		
	    // add languages for Vocabularies
	    if (dataset.getTypeUri().contains(SEMAVocabulary.VocabularyCollection.toString())) {
	    	if (endpoint == null) {
	    		// avoid join (join does not always work) : first get label property
	    		// if this fails also, only solution is to iterate over records
	    		String languages = legacyUris ?
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + SEMAVocabulary.contentGraph + "> {" +
			    		    "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?n . " +
			    		    "?n <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " + 
			    		    "?n <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProperty } } " : 
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + SEMAVocabulary.contentGraph + "> {" +
			    		    "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + SEMAVocabulary.dataProperty + "> ?n . " +
			    		    "?n <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . " + 
			    		    "?n <" + SEMAVocabulary.uri + "> ?labelProperty } } "; 
		    	
	    		String label = "";
		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(languages, Syntax.syntaxARQ))) {
		    		ResultSet rs = qe.execSelect();
		    		while (rs.hasNext()) {
		    			String lang = rs.next().get("labelProperty").toString();
		    			if (label.length()> 0) {
		    				label += "|";
		    			}
		    			label += "<" + lang + ">";
		    		}
		    	}	
		    	
//		    	System.out.println(QueryFactory.create(languages, Syntax.syntaxARQ));
		    	
		    	if (label.length() != 0) {
			    	languages = "SELECT DISTINCT ?lang FROM <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> WHERE { ?p " + label + " ?r . BIND(LANG(?r) AS ?lang) } ";
			    	
//			    	System.out.println(languages);
			    	
			    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(languages, Syntax.syntaxARQ))) {
			    		ResultSet rs = qe.execSelect();
			    		while (rs.hasNext()) {
			    			String lang = rs.next().get("lang").toString();
			    			if (lang.length() > 0) {
			    				String insert = "sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " +
			    						"<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + (legacyUris ? "http://purl.org/dc/elements/1.1/language" : DCTVocabulary.language) + ">  \"" + lang + "\" . } }";

				//		   		stmt.execute(insert);
								vc.executeStatement( insert);
			    			}
			    		}
			    	} catch (Exception ex) {
			    		ex.printStackTrace();
			    	}
		    	}
		    } else {
		    	String languages = legacyUris ?  
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + SEMAVocabulary.contentGraph + "> {" +
			    		    "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?n . " +
			    		    "?n <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " + 
			    		    "?n <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProperty } } " : 
	    		"SELECT DISTINCT ?labelProperty WHERE { " +
	    		  "GRAPH <" + SEMAVocabulary.contentGraph + "> {" +
	    		    "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + SEMAVocabulary.dataProperty + "> ?n . " +
	    		    "?n <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . " + 
	    		    "?n <" + SEMAVocabulary.uri + "> ?labelProperty } } "; 
			    	
		//	    	System.out.println(languages);

			    	String label = "";
			    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(languages, Syntax.syntaxARQ))) {
			    		ResultSet rs = qe.execSelect();
			    		while (rs.hasNext()) {
			    			String lang = rs.next().get("labelProperty").toString();
			    			if (label.length()> 0) {
			    				label += "|";
			    			}
			    			label += "<" + lang + ">";
			    		}
			    	}
			    	
			    	if (label.length() != 0) {
				    	languages = 
					    		"SELECT DISTINCT ?lang WHERE { ?p " + label + " ?r . BIND(LANG(?r) AS ?lang) } ";
					    	
	//				    	System.out.println(languages);
	
				    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(languages, Syntax.syntaxARQ), client)) {
				    		ResultSet rs = qe.execSelect();
				    		while (rs.hasNext()) {
				    			String lang = rs.next().get("lang").toString();
				    			if (lang.length() > 0) {
									String insert = "sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " +
								                    "<" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <" + (legacyUris ? "http://purl.org/dc/elements/1.1/language" : DCTVocabulary.language) + ">  \"" + lang + "\" . } }";
								    		
									vc.executeStatement(insert);
				    			}
				    		}
				    	} catch (Exception ex) {
				    		ex.printStackTrace();
				    	}
			    	}
		    	}
	    
	    
		    if (isSKOS) {
		    	
		    	String schemes = 
		    		"SELECT DISTINCT ?scheme WHERE { " +
		    		   (endpoint == null ? "GRAPH <" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> ": "") + "{ " + 
		    		   " ?p <" + SKOSVocabulary.inScheme + "> ?scheme } }";
		    	
//		    	System.out.println(schemes);
	   		
		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(schemes, Syntax.syntaxARQ), client)) {
		    		ResultSet rs = qe.execSelect();
		    		while (rs.hasNext()) {
		    			String scheme = rs.next().get("scheme").toString();
		    			
						String insert = "sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " +
//						                    "<" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> <http://sw.islab.ntua.gr/semaspace/model/scheme>  <" + scheme + "> . } }";
                                        "<" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + "> <" + SEMAVocabulary.scheme + "> <" + scheme + "> . } }";
					    		
						vc.executeStatement(insert);
					}
		    	} catch (Exception ex) {
		    		ex.printStackTrace();
		    	}
		    }		    
	    }
	    
	    // update prefixes maps
	    
	    if (dataset.getType().equals("vocabulary-dataset")) {
	    	((VocabulariesBean)context.getBean("vocabularies")).setMap(AppConfiguration.createVocabulariesMap(vc, legacyUris));
	    }
	    
    	((VocabulariesBean)context.getBean("all-datasets")).setMap(AppConfiguration.createDatasetsMap(vc, legacyUris));

    }
    
    
    public boolean unpublish(UserPrincipal currentUser, String virtuoso, Dataset dataset, List<MappingDocument> mappings, List<FileDocument> files, boolean metadata, boolean content) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
    	String mf = getMappingsFolder(currentUser);
    	
		for (MappingDocument map : mappings) {
			boolean hi = !map.getParameters().isEmpty();
			
			for (MappingInstance mi : map.getInstances()) {
				ExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());
				
				if (es != null) { // should we have this ?
   					for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
   						vc.executeUpdateStatement(vc.delete(mf, map.getUuid() + (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig"));
   					}
				}
			}
		}
		
		String uf = getUploadsFolder(currentUser) + "/";
		
		for (FileDocument file : files) {
    		File folder = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder, file.getId().toString());
        		
    		for (File f : folder.listFiles()) {
    			vc.executeUpdateStatement(vc.delete(uf + file.getId().toString(), f.getName()));
    		}
		}
    	
    	if (content && dataset.getType().equals("annotation-dataset")) {
    		String sparql =
    			"DELETE { GRAPH <" + dataset.getAsProperty() + "> {" +
	            " ?annId ?p1 ?o1 ." + 
	            " ?o1 ?p2 ?o2 . } } " +            		
	    		"WHERE { " + 
	            "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
	            "    <" + SEMAVocabulary.getAnnotationSet(dataset.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
	            "  GRAPH <" + dataset.getAsProperty() + "> {" +
	            "    ?annId ?p1 ?o1 . " +
	            "    OPTIONAL { ?o1 ?p2 ?o2 } } }  ";
	    		
	    	vc.executeStatement( "sparql " + sparql);
	    		
	    		sparql =
	            "DELETE WHERE { GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " + 
	    		    "<" + SEMAVocabulary.getAnnotationSet(dataset.getUuid()) + "> ?p ?q . } }";
	    		
			vc.executeStatement( "sparql " + sparql);


			vc.executeStatement( "checkpoint" );

    	} else {
    		if (metadata) {
    			ivirtuoso.nestedDelete(vc, SEMAVocabulary.contentGraph.toString(), SEMAVocabulary.getDataset(dataset.getUuid()).toString());
    		}
	    	//TODO: unpublish annotations
	    	
	    	if (content) {
		    	logger.info("Clearing graph <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + ">");
	    		vc.executeStatement( "sparql clear graph <" + SEMAVocabulary.getDataset(dataset.getUuid()).toString() + ">");
	    		vc.executeStatement("checkpoint");
	    	}
	
    	}
		return true;
    	
    }    
    
    
//    public boolean unpublishMetadata(UserPrincipal currentUser, Dataset dataset, List<MappingDocument> mappings) throws Exception {
//
//    	String mf = getMappingsFolder(currentUser);
//    	
//		for (MappingDocument map : mappings) {
//			if (map.getType() == MappingType.HEADER) {
//	    		try (Statement stmt = conn.createStatement()) {
//	    			boolean hi = !map.getParameters().isEmpty();
//	    			
//	    			for (MappingInstance mi : map.getInstances()) {
//	    				ExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());
//	    				
//	   					for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
//	   						stmt.executeUpdate(delete(mf, map.getUuid() + (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig"));
//	   					}
//	
//	    			}
//		    	}
//			}
//		}
//		
//    	virtuoso.nestedDelete(SEMAVocabulary.contentGraph.toString(), SEMAVocabulary.getDataset(dataset.getUuid()).toString());
//	    	
//		return true;
//    	
//    }   
    
    public boolean publish(UserPrincipal currentUser, String virtuoso, List<AnnotatorDocument> docs) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
    	
//    	System.out.println("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
    	
    	String af = getAnnotationsFolder(currentUser);

    	logger.info("Preparing publication");

		for (AnnotatorDocument adoc : docs) {
			ExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());
    		
			logger.info("Publication execute " + vc.delete(af, adoc.getUuid() + "_catalog.trig"));
			
			vc.executeUpdateStatement(vc.delete(af, adoc.getUuid() + "_catalog.trig"));
    		vc.prepare(sftpUploadGateway, af, adoc.getUuid() + "_catalog.trig", uploadedFiles);
    		
    		logger.info("Publication execute " + vc.lddir(af, adoc.getUuid() + "_catalog.trig", SEMAVocabulary.annotationGraph.toString()));
    		
    		vc.executeStatement(vc.lddir(af, adoc.getUuid() + "_catalog.trig", SEMAVocabulary.annotationGraph.toString()));

    		logger.info("Publication shards " + Math.max(1, es.getExecuteShards()));
    		
			for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
				String fileName = adoc.getUuid() + (i == 0 ? "" : "_#" + i) + ".trig";

				vc.executeUpdateStatement(vc.delete(af, fileName));
				vc.prepare(sftpUploadGateway, af, fileName, uploadedFiles);
	    		vc.executeStatement(vc.lddir(af, fileName, adoc.getAsProperty()));
    		}
		}
    	
		logger.info("Executing publication");
		
   		vc.executeStatement( "rdf_loader_run()");
    	
    	logger.info("Deleting publication uploaded files");
    	
	    for (String f : uploadedFiles) {
			vc.deleteFile(sftpDeleteGateway, f);
	    }
	    
	    logger.info("Updating publication annotationset graph");
	    
		for (AnnotatorDocument adoc : docs) {
			String insert = 
			   "sparql insert { graph <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
                    "<" + SEMAVocabulary.getAnnotationSet(adoc.getUuid()).toString() + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . } }";

    		vc.executeStatement( insert);
		}
    	
   		vc.executeStatement( "checkpoint");

		return true;

    }    
    
//    public boolean publishAsAnnotations(List<MappingDocument> docs) throws Exception {
//
////    	System.out.println("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
//    	
//		try (Statement stmt = conn.createStatement()) {
//			for (MappingDocument adoc : docs) {
//	    		stmt.executeUpdate("DELETE FROM DB.DBA.load_list WHERE ll_file ='" + mappingsFolder  + adoc.getUuid() + ".trig'");
//    			stmt.execute("ld_dir('" + mappingsFolder.substring(0, mappFolder.length() - 1) + "', '" + adoc.getUuid() + "_catalog.trig', '" + SEMAVocabulary.annotationGraph.toString() + "')");
//				stmt.execute("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
//			}
//    	}
//    	
//    	try (Statement stmt = conn.createStatement()) {
//    		stmt.execute("rdf_loader_run()");
//    	}
//    	
//    	try (Statement stmt = conn.createStatement()) {
//    		for (AnnotatorDocument adoc : docs) {
//				String insert = 
//				   "sparql insert { graph <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
//	                    "<" + SEMAVocabulary.getAnnotationSet(adoc.getUuid()).toString() + "> a <http://www.w3.org/ns/dcat#Dataset> . } }";
//	
//	    		stmt.execute(insert);
//    		}
//    	}    	
//    	
//    	try (Statement stmt = conn.createStatement()) {
//    		stmt.execute("checkpoint");
//    	}
//
//		return true;
//
//    }        
    
 	

    public boolean unpublish(String virtuoso, List<AnnotatorDocument> docs) throws Exception {
    	
    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
//    	virtuoso.nestedDelete(SEMAVocabulary.contentGraph.toString(), SEMAVocabulary.getDataset(dataset.getUuid()).toString());
    	
		for (AnnotatorDocument doc : docs) {

			logger.info("Annotation unpublishing " + SEMAVocabulary.getAnnotationSet(doc.getUuid()));

			int iter = 0;
			while (true) {
    			String select = "SELECT (count(?annId) AS ?count) WHERE { " +
    		            "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
    		            "    <" + SEMAVocabulary.getAnnotationSet(doc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
    		            "  GRAPH <" + doc.getAsProperty() + "> {" +
    				    "    ?annId a <" + OAVocabulary.Annotation + "> } }"; 
 		    	
    			logger.info("Counting annotations to unpublish " + SEMAVocabulary.getAnnotationSet(doc.getUuid()));
//	    			System.out.println(QueryFactory.create(select, Syntax.syntaxSPARQL_11));
    			
    			int count = 0;
    			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(select, Syntax.syntaxSPARQL_11))) {
 		   			ResultSet rs = qe.execSelect();
 		   			if (rs.hasNext()) {
 		   				QuerySolution qs = rs.next();
 		   				count = qs.get("count").asLiteral().getInt();
 		   			}
 		    	}
    			
    			logger.info("Annotations to unpublish " + SEMAVocabulary.getAnnotationSet(doc.getUuid()) + ": " + count);
    			
    			if (count == 0) {
    				break;
    			}
    			
	    		String sparql =
	            "DELETE { GRAPH <" + doc.getAsProperty() + "> {" +
	            " ?annId ?p1 ?o1 ." + 
	            " ?o1 ?p2 ?o2 . } } " +            		
	    		"WHERE { " + 
	            "  GRAPH <" + doc.getAsProperty() + "> {" +
	            "    ?annId ?p1 ?o1 . " +
	            "    OPTIONAL { ?o1 ?p2 ?o2 } } " + 
	            "  { SELECT ?annId WHERE { " +
	            "      GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
	            "        <" + SEMAVocabulary.getAnnotationSet(doc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
	            "   } LIMIT 100000 OFFSET " + iter*100000 + " " +
	            " } } ";
	    		
	    		logger.info("Unpublishing " + SEMAVocabulary.getAnnotationSet(doc.getUuid()) + " batch #" + iter);
	    		
	    		vc.executeStatement("sparql " + sparql);
	    		
	    		logger.info("Unpublishing " + SEMAVocabulary.getAnnotationSet(doc.getUuid()) + " batch #" + iter + " completed.");

	    		iter++;
			}
			
			logger.info("Unpublishing annotationset for " + SEMAVocabulary.getAnnotationSet(doc.getUuid()));
			
    		String sparql =
            "DELETE WHERE { GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
            " <" + SEMAVocabulary.getAnnotationSet(doc.getUuid()) + "> ?p ?q . } }";
    		
    		vc.executeStatement("sparql " + sparql);
    		
    		logger.info("Unpublication of " + SEMAVocabulary.getAnnotationSet(doc.getUuid()) + " completed.");
		}
		
		vc.executeStatement("checkpoint");

		return true;

    }    
    
    public boolean publish(UserPrincipal currentUser, String virtuoso, PagedAnnotationValidation pav, List<AnnotationEdit> deletes) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
//    	System.out.println("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
    	
    	//DELETE first
	
		String onPropertyString = AnnotationEditGroup.onPropertyListAsString(pav.getOnProperty());
		String annfilter = AnnotationEditGroup.annotatorFilter("v", pav.getAnnotatorDocumentUuid());
		
		for (AnnotationEdit edit :  deletes) {
			String sparql = 
				"DELETE { " + 
			    "  GRAPH <" + pav.getAsProperty() + "> {" +
		        "    ?annId ?p1 ?o1 ." + 
		        "    ?o1 ?p2 ?o2 . } " +
		        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +
		        "} " +            		
		    	"WHERE { " + 
		        "  GRAPH <" + pav.getAsProperty() + "> { " + 
			    "   ?v a <" + OAVocabulary.Annotation + "> ?annId . " + 
		        "   ?annId <" + OAVocabulary.hasBody + "> <" + edit.getAnnotationValue() + "> . " +
		        "   ?annId <" + OAVocabulary.hasTarget + "> ?target . " +
			    annfilter +
		        "   ?target  <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
		        "            <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " ; " +
		        "            <" + OAVocabulary.hasSource + "> ?s  . " +
	            "   ?annId ?p1 ?o1 . " +
	            "   OPTIONAL { ?o1 ?p2 ?o2 } } . " +	    			        
		        " GRAPH <" + SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString() + "> { " +
		        "  ?s " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
                " GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +		    			        
		        "}";
			
//				System.out.println(sparql);
		    	
			vc.executeStatement("sparql " + sparql);
		}
    	
    	String mf = getFolder(currentUser, annotationsFolder, pav.getDatasetUuid());
    	
    	//INSERT then
		vc.executeUpdateStatement(vc.delete(mf, pav.getUuid() + "_add_catalog.trig"));
		vc.executeUpdateStatement(vc.delete(mf, pav.getUuid() + "_add.trig"));

		vc.prepare(sftpUploadGateway, mf, pav.getId() + "_add_catalog.trig", uploadedFiles);
		vc.executeStatement(vc.lddir(mf, pav.getUuid() + "_add_catalog.trig", SEMAVocabulary.annotationGraph.toString()));
		vc.prepare(sftpUploadGateway, mf, pav.getId() + "_add.trig", uploadedFiles);
		vc.executeStatement(vc.lddir(mf, pav.getUuid() + "_add.trig", pav.getAsProperty()));

		vc.executeStatement("rdf_loader_run()");
    	
	    for (String f : uploadedFiles) {
			vc.deleteFile(sftpDeleteGateway, f);
	    }
    	
		vc.executeStatement("checkpoint");

		return true;

    }    
    
    public boolean unpublish(UserPrincipal currentUser, String virtuoso, PagedAnnotationValidation pav, List<AnnotationEdit> adds) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
//    	System.out.println("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
    	
    	String mf = getFolder(currentUser, annotationsFolder, pav.getDatasetUuid());
    	
    	// INSERT DELETES
		vc.executeUpdateStatement(vc.delete(mf, pav.getUuid() + "_delete_catalog.trig"));
		vc.executeUpdateStatement(vc.delete(mf, pav.getUuid() + "delete.trig"));

		vc.prepare(sftpUploadGateway, mf, pav.getUuid() + "_delete_catalog.trig", uploadedFiles);
		vc.executeStatement(vc.lddir(mf, pav.getUuid() + "_delete_catalog.trig", SEMAVocabulary.annotationGraph.toString()));
		vc.prepare(sftpUploadGateway, mf, pav.getId() + "_delete.trig", uploadedFiles);
		vc.executeStatement(vc.lddir(mf, pav.getUuid() + "_delete.trig", pav.getAsProperty()));
		
		vc.executeStatement("rdf_loader_run()");
	    
    	for (String f : uploadedFiles) {
			vc.deleteFile(sftpDeleteGateway, f);
	    }
    	
    	//DELETE ADDS
		String onPropertyString = AnnotationEditGroup.onPropertyListAsString(pav.getOnProperty());
		String annfilter = AnnotationEditGroup.annotatorFilter("v", pav.getAnnotatorDocumentUuid());

		for (AnnotationEdit edit :  adds) {
			String sparql =
				"DELETE { " + 
			    "  GRAPH <" + pav.getAsProperty() + "> {" +
		        "    ?annId ?p1 ?o1 ." + 
		        "    ?o1 ?p2 ?o2 . } " +
		        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +
		        "} " +            		
		    	"WHERE { " + 
		        "  GRAPH <" + pav.getAsProperty() + "> { " + 
			    "   ?v a <" + OAVocabulary.Annotation + "> ?annId . " + 
		        "   ?annId <" + OAVocabulary.hasBody + "> <" + edit.getAnnotationValue() + "> . " +
		        "   ?annId <" + OAVocabulary.hasTarget + "> ?target . " +
			    annfilter +
		        "   ?target  <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
		        "            <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " ; " +
		        "            <" + OAVocabulary.hasSource + "> ?s  . " +
	            "   ?annId ?p1 ?o1 . " +
	            "   OPTIONAL { ?o1 ?p2 ?o2 } } . " +
		        " GRAPH <" + SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString() + "> { " +
		        "  ?s " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
                " GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +		    			        
		    	"} ";
			
//				System.out.println(sparql);
		    	
			vc.executeStatement("sparql " + sparql);
		}
    	
   		vc.executeStatement("checkpoint");

		return true;

    }        
    
    
    public boolean publish(UserPrincipal currentUser, String virtuoso, FilterAnnotationValidation fav) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
    	String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + manualFolder + fav.getDatasetUuid();

    	logger.info("Preparing filter annotation validation publication");

		
		ExecuteState es = fav.getExecuteState(fileSystemConfiguration.getId());
    		
//    		logger.info("Publication shards " + Math.max(1, es.getExecuteShards()));
    		
		for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
			String fileName = fav.getUuid() + (i == 0 ? "" : "_#" + i) + ".trig";

//				stmt.executeUpdate(delete(datasetFolder, fileName));
			vc.executeUpdateStatement(vc.delete(datasetFolder, fileName));
			vc.prepare(sftpUploadGateway, datasetFolder, fileName, uploadedFiles);
//	    		stmt.execute(lddir(datasetFolder, fileName, fav.getAsProperty()));
    		vc.executeStatement(vc.lddir(datasetFolder, fileName, fav.getAsProperty()));
		}
    	
		logger.info("Executing filter annotation validation publication");
		
   		vc.executeStatement( "rdf_loader_run()");
    	
    	logger.info("Deleting filter annotation validation uploaded files");
    	
	    for (String f : uploadedFiles) {
			vc.deleteFile(sftpDeleteGateway, f);
	    }
	    
	    logger.info("Updating filter annotation validation annotationset graph");
	    
		String insert = 
    			"INSERT { GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
//	    					"  ?annotationset <" + DCVocabulary.hasPart + "> ?annotation } }" +
    					"  <" + SEMAVocabulary.getAnnotationSet(fav.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation } }" +
    					"WHERE { " +
    					"  GRAPH <" + fav.getAsProperty() + "> { " +
    					"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
    					"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
    					"    ?validation <" + ASVocabulary.generator + "> <" + SEMAVocabulary.getAnnotationValidator(fav.getUuid()) + "> ." +
    					"    ?validation <" + SOAVocabulary.action + "> <" + SOAVocabulary.Add + "> . }" +
    					"} ";
    					
		vc.executeStatement("sparql " + insert);
    	
		vc.executeStatement("checkpoint");
    	
    	logger.info("Filter annotation validation publication completed");

		return true;

    }    
    
    public boolean unpublish(UserPrincipal currentUser, String virtuoso, AnnotationValidation fav) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
		logger.info("Executing annotation validation unpublication");
		
		String sparql =
				"DELETE { " + 
			    "  GRAPH <" + fav.getAsProperty() + "> {" +
			    "    ?annotation ?p1 ?o1 . " +
			    "    ?o1 ?p2 ?p2 . } " +
		        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
                "    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " +
		        "} " +            		
		    	"WHERE { " + 
		        "  GRAPH <" + fav.getAsProperty() + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
				"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
				"    ?validation <" + ASVocabulary.generator + "> <" + SEMAVocabulary.getAnnotationValidator(fav.getUuid()) + "> ." +
				"    ?validation <" + SOAVocabulary.action + "> <" + SOAVocabulary.Add + "> . " +
				"    ?annotation ?p1 ?o1 . " +
				"    OPTIONAL { ?o1 ?p2 ?o2 } } " +
				"  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
				"    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation } " +
		    	"} ";
		
		vc.executeStatement("sparql " + sparql);

		// delete validations in original annotations
		sparql =
				"DELETE { " + 
			    "  GRAPH <" + fav.getAsProperty() + "> {" +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation ?p ?o } " +
		        "} " +            		
		    	"WHERE { " + 
		        "  GRAPH <" + fav.getAsProperty() + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation <" + ASVocabulary.generator + "> <" + SEMAVocabulary.getAnnotationValidator(fav.getUuid()) + "> . " +
			    "    ?validation ?p ?o } " + 
		    	"} ";
		
		vc.executeStatement("sparql " + sparql);

		vc.executeStatement("checkpoint");

    	logger.info("Annotation validation unpublication completed");
    	
		return true;

    }  

    public boolean publish(UserPrincipal currentUser, String virtuoso, PagedAnnotationValidation pav) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
    	String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + manualFolder + pav.getDatasetUuid();

    	logger.info("Preparing paged annotation validation publication");

		ExecuteState es = pav.getExecuteState(fileSystemConfiguration.getId());
    		
// 		logger.info("Publication shards " + Math.max(1, es.getExecuteShards()));
    		
		for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
			String fileName = pav.getUuid() + (i == 0 ? "" : "_#" + i) + ".trig";

			vc.executeUpdateStatement(vc.delete(datasetFolder, fileName));
			vc.prepare(sftpUploadGateway, datasetFolder, fileName, uploadedFiles);
    		vc.executeStatement(vc.lddir(datasetFolder, fileName, pav.getAsProperty()));
		}
    	
		logger.info("Executing paged annotation validation publication");
		
   		vc.executeStatement("rdf_loader_run()");
    	
    	logger.info("Deleting paged annotation validation uploaded files");
    	
	    for (String f : uploadedFiles) {
			vc.deleteFile(sftpDeleteGateway, f);
	    }
	    
	    logger.info("Updating paged annotation validation annotationset graph");
	    
		String insert = 
    			"INSERT { GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
    					"  <" + SEMAVocabulary.getAnnotationSet(pav.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation } }" +
    					"WHERE { " +
    					"  GRAPH <" + pav.getAsProperty() + "> { " +
    					"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
    					"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
    					"    ?validation <" + ASVocabulary.generator + "> <" + SEMAVocabulary.getAnnotationValidator(pav.getUuid()) + "> ." +
    					"    ?validation <" + SOAVocabulary.action + "> <" + SOAVocabulary.Add + "> . } " +
    					"} ";
    					
		vc.executeStatement("sparql " + insert);
		vc.executeStatement("checkpoint");
    	
    	logger.info("Paged annotation validation publication completed");

		return true;

    }    
    
    public boolean publish(UserPrincipal currentUser, String virtuoso, VocabularizerDocument doc) throws Exception {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
    	
    	String vf = getVocabularizerFolder(currentUser);
    	
		vc.executeUpdateStatement(vc.delete(vf, doc.getUuid() + "_catalog.trig"));
		vc.executeUpdateStatement(vc.delete(vf, doc.getUuid() + ".trig"));

		vc.prepare(sftpUploadGateway, vf, doc.getUuid() + "_catalog.trig", uploadedFiles);
		vc.executeStatement(vc.lddir(vf, doc.getUuid() + "_catalog.trig", SEMAVocabulary.contentGraph.toString()));
		vc.prepare(sftpUploadGateway, vf, doc.getUuid() + ".trig", uploadedFiles);
		vc.executeStatement(vc.lddir(vf, doc.getUuid() + ".trig", SEMAVocabulary.getDataset(doc.getUuid()).toString()));
    	
		vc.executeStatement("rdf_loader_run()");
    	
    	for (String f : uploadedFiles) {
			vc.deleteFile(sftpDeleteGateway, f);
	    }
    	
		String insert = 
		   "sparql insert { graph <" + SEMAVocabulary.contentGraph.toString() + "> { " +
                   "<" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . " +
                   "<" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + "> a <" + SEMAVocabulary.Autogenerated + "> . " +
		           "<" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + "> a <" + SEMAVocabulary.VocabularyCollection + "> . " +
		           "<" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + "> a <" + SEMAVocabulary.ThesaurusCollection + "> . " + 
		           "<" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + "> a <" + SEMAVocabulary.SKOSThesaurus + "> . } }";

		vc.executeStatement(insert);
		vc.executeStatement("checkpoint");
    	
		return true;

    }        
    
    public boolean unpublish(String virtuoso, VocabularizerDocument doc) throws Exception {
    	
    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);

    	ivirtuoso.nestedDelete(vc, SEMAVocabulary.contentGraph.toString(), SEMAVocabulary.getDataset(doc.getUuid()).toString());
    	//TODO: unpublish annotations
    	
//    	System.out.println("sparql clear graph <" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + ">");
		vc.executeStatement("sparql clear graph <" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + ">");
		vc.executeStatement("checkpoint");

		return true;

    }    
    
    public void resetAccessGraph(String virtuoso) throws SQLException {
    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
		vc.executeStatement("sparql clear graph <" + SEMAVocabulary.accessGraph.toString() + ">");
		vc.executeStatement("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.defaultGroup.toString() + "> a <" + SACCVocabulary.Group.toString() + "> } } ");
		vc.executeStatement("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.defaultGroup.toString() + "> a <" + SACCVocabulary.PublicGroup.toString() + "> } }");
		vc.executeStatement("checkpoint");
    }
    
    public void addUserToAccessGraph(String virtuoso, String uuid) throws SQLException {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);
    	
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getUser(uuid).toString() + "> a <" + SACCVocabulary.User.toString() + "> } } ");
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getGroup(uuid).toString() + "> a <" + SACCVocabulary.Group.toString() + "> } } ");    		
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.defaultGroup.toString() + "> <" + SACCVocabulary.member.toString() + "> <" + SEMAVocabulary.getUser(uuid).toString() + "> } } ");
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getGroup(uuid).toString() + "> <" + SACCVocabulary.member.toString() + ">  <" + SEMAVocabulary.getUser(uuid).toString() + "> } } ");    		

		vc.executeStatement("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getUser(uuid).toString() + "> a <" + SACCVocabulary.User.toString() + "> } } ");
		vc.executeStatement("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getGroup(uuid).toString() + "> a <" + SACCVocabulary.Group.toString() + "> } } ");
		vc.executeStatement("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.defaultGroup.toString() + "> <" + SACCVocabulary.member.toString() + "> <" + SEMAVocabulary.getUser(uuid).toString() + "> } } ");
		vc.executeStatement("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getGroup(uuid).toString() + "> <" + SACCVocabulary.member.toString() + ">  <" + SEMAVocabulary.getUser(uuid).toString() + "> } } ");
		vc.executeStatement("checkpoint");
    }
    
    public void addDatasetToAccessGraph(UserPrincipal currentUser, String virtuoso, String datasetUuid, boolean isPublic) throws SQLException {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);

    	removeDatasetFromAccessGraph(virtuoso, datasetUuid);
    	
		vc.executeStatement( "sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getGroup(currentUser.getUuid()).toString() + "> <" + SACCVocabulary.dataset.toString() + "> <" + SEMAVocabulary.getDataset(datasetUuid).toString() + "> } } ");
   		if (isPublic) {
			vc.executeStatement( "sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.defaultGroup.toString() + "> <" + SACCVocabulary.dataset.toString() + "> <" + SEMAVocabulary.getDataset(datasetUuid).toString() + "> } } ");
    	}
   		
		vc.executeStatement( "checkpoint");
    }
    
    public void removeDatasetFromAccessGraph(String virtuoso, String datasetUuid) throws SQLException {

    	VirtuosoConfiguration vc =  virtuosoConfigurations.get(virtuoso);

		vc.executeStatement( "sparql delete where { graph <" + SEMAVocabulary.accessGraph.toString() + "> { ?s <" + SACCVocabulary.dataset.toString() + "> <" + SEMAVocabulary.getDataset(datasetUuid).toString() + "> } } ");
		vc.executeStatement("checkpoint" );
    }
    
    
//    public boolean publishAnnotationEdits(List<AnnotationEdit> edits, String annotationGraph, AnnotationType type) throws Exception {
//
//		try (Statement stmt = conn.createStatement()) {
//			for (AnnotationEdit edit : edits) {
//				
//				if (edit.getEditType() == AnnotationEditType.ADD) {
//					String uri = SEMAVocabulary.getAnnotation(UUID.randomUUID().toString()).toString();
//					
//					String annType = SEMAVocabulary.getAnnotationType(type).toString();
//						
//					
//					
//					String sparql =
//					"INSERT { GRAPH <" + annotationGraph + "> { " +
//					"  <" + uri + "> [ " +
//					"    a       <http://www.w3.org/ns/oa#Annotation>,<" + annType + "> ; " +
//					"   <http://purl.org/dc/terms/created> \"" + currentTime() + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ; " +
//					"   <http://www.w3.org/ns/oa#hasBody> <" + edit.getAnnotationValue() + "> ; "+ 
//					"   <http://www.w3.org/ns/oa#hasTarget> [ " +
//					"     <http://sw.islab.ntua.gr/annotation/onProperty> <" + edit.getPropertyUri() + "> ; " +
//					"     <http://sw.islab.ntua.gr/annotation/onValue> \"" + edit.getPropertyValue() + "\" ; " +
//					"     <http://www.w3.org/ns/oa#hasSource> ?s ]" +
//					"   <https://www.w3.org/ns/activitystreams#generator> <http://semaspace.islab.ntua.gr/> ] } }" +
//					"WHERE { GRAPH <" + SEMAVocabulary.getDataset(edit.getDatasetUuid()) + "> { " +
//					"  ?s <" + edit.getPropertyUri() + "> <" + edit.getPropertyValue() + "> ." +
//					"  ?s <http://www.w3.org/ns/oa#hasTarget> [ " +
//					"     <http://sw.islab.ntua.gr/annotation/onProperty> <" + edit.getPropertyUri() + "> ;" +
//					"     <http://sw.islab.ntua.gr/annotation/onValue> \"" + edit.getPropertyValue() + "\" ] ." +
//					"  ?s ?p1 ?o1 " +
//					"  OPTIONAL { ?o1 ?p2 ?o2 . FILTER(isBlank(?o1)) } } } ";					
//	
//					stmt.execute("sparql " + sparql);
//
//				} else if (edit.getEditType() == AnnotationEditType.DELETE) {
//					String sparql =
//					"WITH GRAPH <" + annotationGraph + "> { " + 
//					"DELETE { ?s ?p1 ?o1 . ?o1 ?p2 ?o2 } " +
//					"WHERE { graph <" + annotationGraph + "> { " +
//					"  ?s <http://www.w3.org/ns/oa#hasBody> <" + edit.getAnnotationValue() + "> ." +
//					"  ?s <http://www.w3.org/ns/oa#hasTarget> [ " +
//					"     <http://sw.islab.ntua.gr/annotation/onProperty> <" + edit.getPropertyUri() + "> ;" +
//					"     <http://sw.islab.ntua.gr/annotation/onValue> \"" + edit.getPropertyValue() + "\" ] ." +
//					"  ?s ?p1 ?o1 " +
//					"  OPTIONAL { ?o1 ?p2 ?o2 . FILTER(isBlank(?o1)) } } } ";					
//					stmt.execute("sparql " + sparql);
//				}
//			}
//			
//			stmt.execute("checkpoint");
//    	}
//
//		return true;
//
//    } 
    


 
}
