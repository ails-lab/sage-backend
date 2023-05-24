package ac.software.semantic.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
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
import org.apache.jena.rdf.model.ResourceFactory;
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
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.config.SFTPAdaptor.SftpDeleteGateway;
import ac.software.semantic.config.SFTPAdaptor.SftpUploadGateway;
import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.config.VocabulariesBean;
import ac.software.semantic.config.VocabulariesMap;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.VocabularizerDocument;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetType;
import ac.software.semantic.model.constants.MappingType;
import ac.software.semantic.model.constants.ResourceOptionType;
import ac.software.semantic.model.constants.SerializationType;
import ac.software.semantic.model.state.AnnotatorPublishState;
import ac.software.semantic.model.state.CreateDistributionState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCATVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import ac.software.semantic.vocs.SACCVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Component
public class TripleStore {

	Logger logger = LoggerFactory.getLogger(TripleStore.class);

    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

    @Value("${backend.server}")
    private String server;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Autowired
	ResourceLoader resourceLoader;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	AnnotationEditGroupRepository aegRepository;

	@Autowired
	MappingRepository mappingRepository;
	
	@Autowired
	SchemaService schemaService;
	
	@Autowired
	NamesService namesService;

	@Autowired
	AnnotationEditGroupService aegService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private Environment env;
	
    private FileSystemConfiguration fileSystemConfiguration;
    
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
	@Qualifier("triplestore-configurations") 
	ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
	@Autowired
	FolderService folderService;
	
	@Autowired
	@Qualifier("all-datasets")
	private VocabulariesMap vm;
	
    public TripleStore(@Qualifier("triplestore-configurations") ConfigurationContainer<TripleStoreConfiguration> vcs,
    		           @Qualifier("filesystem-configuration") FileSystemConfiguration fsc) throws Exception {
    	
    	this.fileSystemConfiguration = fsc;
    }

    
    //cannot publish two datasets at the same time : problem for linking annotations to annotations sets  
    public synchronized boolean publish(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, List<MappingDocument> mappings, List<FileDocument> files, boolean metadata) throws Exception {
    	
    	Set<String> uploadedFiles = new HashSet<>();
    	
    	boolean header = metadata;
    	
		for (MappingDocument map : mappings) {
			for (MappingInstance mi : map.getInstances()) {
				MappingExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());

				if (es != null && es.getExecuteState() == MappingState.EXECUTED) {
					for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
						File f = folderService.getMappingExecutionTrigFile(currentUser, dataset, map, mi, es, i);
//						System.out.println("Prepare to publish file " + i + " " + f + "[" + localImport + "]");  //should verify that  f != null
	    				String p = folderService.getParent(f);
	    				String n = folderService.getName(f);
						
						vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
						
						String targetGraph = null;
						if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET) {
		    				targetGraph = dataset.getAsProperty();
						} else {
			    			if (map.getType() == MappingType.HEADER) {
			    				targetGraph = resourceVocabulary.getContentGraphResource().toString();
				    			header = true;
			    			} else if (map.getType() == MappingType.CONTENT) {
			    				targetGraph = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();
					    	}
						}
	
		    			if (targetGraph != null) {
	    					vc.prepare(sftpUploadGateway, p, n, uploadedFiles);
	    					vc.executePrepareLoadStatement(p, n, targetGraph, fileSystemConfiguration.getDataFolder());
		    			}
	    			}
				}
	    	}
		}
		
		for (FileDocument fd : files) {
    		String targetGraph = null;
//			if (dataset.getType().equals("annotation-dataset")) {
    		if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET) {
				targetGraph = dataset.getAsProperty();
			} else {
				targetGraph = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();
			}
			
    		for (File f : folderService.getUploadedFiles(currentUser, dataset, fd, fd.getExecute())) {
				String p = folderService.getParent(f);
				String n = folderService.getName(f);
    			
    			vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
   				vc.prepare(sftpUploadGateway, p, n, uploadedFiles);
				vc.executePrepareLoadStatement(p, n, targetGraph, fileSystemConfiguration.getDataFolder());
    		}
		}
		
    	vc.executeLoadStatement();
	    
		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
    	
	    if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET) {	    
    		String insert = 
    			"INSERT { GRAPH <" + resourceVocabulary.getAccessGraphResource() + "> { " +
    					"<" + resourceVocabulary.getAnnotationSetAsResource(dataset.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?p } }" +
    					"WHERE { GRAPH <" + dataset.getAsProperty() + "> { " +
    					       " ?p a <" + OAVocabulary.Annotation + "> . } " +
    					"FILTER NOT EXISTS { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
    						   " ?g <" + DCTVocabulary.hasPart + "> ?p } } } ";
    					
    		vc.executeSparqlUpdateStatement(insert);
	    } 
	    
	    boolean schemaComputed = publishPostprocess(currentUser, vc, dataset, header);
	    
    	vc.executeCheckpointStatement();
	    	
		return schemaComputed;

    }
    
    public boolean setPublishLastModified(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, Date date) throws Exception {
    	
    	PublishState fps = dataset.checkFirstPublishState(vc.getId());
    	String insert = "insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { "; 

    	if (fps != null) {
			Date issueDate = fps.getPublishStartedAt();
			
    		insert += "<" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + "> <" + DCTVocabulary.issued + "> \"" + dateFormat.format(issueDate) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
    		insert += "<" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + "> <" + DCTVocabulary.modified + "> \"" + dateFormat.format(date) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
    	} else {
	    	insert += "<" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + "> <" + DCTVocabulary.issued + "> \"" + dateFormat.format(date) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
    	}
    	
    	insert += "} } WHERE { } ";
    	
   		vc.executeSparqlUpdateStatement(insert);
   		vc.executeCheckpointStatement();
	    	
		return true;

    }

    public boolean unpublishMapping(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, MappingDocument mapping, MappingInstance mi) throws Exception {
    	
    	MappingExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());

		for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
			File f = folderService.getMappingExecutionTrigFile(currentUser, dataset, mapping, mi, es, i);
				
			if (!f.exists()) {
				logger.warn("Failed to unpublish mapping " + mapping.getUuid());
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
		    	StringBuffer entry = new StringBuffer("DELETE DATA FROM <" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString() + "> {\n ");
		    	int count = 0;
			    while ((line = br.readLine()) != null) {
			    	
			    	if (line.length() == 0) {
					    if (count > 0) {
				    		entry.append("}");
				    		
//				    		System.out.println(entry);

				    		vc.executeSparqlUpdateStatement(entry.toString());
					    }
			    		
			    		entry = new StringBuffer("DELETE DATA FROM <" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString() + "> {\n ");
			    	} else {
			    		count++;
			    		entry.append(line);
			    	}
			    }
			    
			    if (count > 0) {
			    	entry.append("}");
		    		
//		    		System.out.println(entry);
			    	vc.executeSparqlUpdateStatement(entry.toString());
			    }
			}
			
			
//			vc.executeUpdateStatement(sparql);
		}
	    
//    	vc.executeStatement( "checkpoint");
	    	
		return true;

    }

	private boolean executeSaturate(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset) throws Exception {

		try (FileSystemRDFOutputHandler outhandler = folderService.createDatasetSaturateOutputHandler(currentUser, dataset, shardSize)) {

			Map<String, Object> params = new HashMap<>();

			params.put("iigraph", resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString());
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
					File f = folderService.getDatasetSaturateTrigFile(currentUser, dataset, i);
	    			String p = folderService.getParent(f);
	    			String n = folderService.getName(f);
	    			
		   			vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
		    				
					String targetGraph = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();
					vc.prepare(sftpUploadGateway, p, n, uploadedFiles);
 					vc.executePrepareLoadStatement(p, n, targetGraph, fileSystemConfiguration.getDataFolder());
				}
					
				vc.executeLoadStatement();
			    
				vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
				
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
	
    
    private boolean publishPostprocess(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, boolean header) throws Exception {
    	
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
	   	
	   	String datasetUri = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();

		DatasetCatalog dcg = schemaService.asCatalog(dataset.getUuid());
		String fromClause = schemaService.buildFromClause(dcg);

		
		if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET) {			

			String insert = 
			   "INSERT { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
                    "<" + resourceVocabulary.getAnnotationSetAsResource(dataset.getUuid()) + "> a <" + DCATVocabulary.Dataset + "> , <" + VOIDVocabulary.Dataset + "> . } } WHERE { }";
	   		vc.executeSparqlUpdateStatement(insert);

		} else if (dataset.getScope() == DatasetScope.ALIGNMENT && dataset.getDatasetType() == DatasetType.DATASET) {
			
			String insert = 
				"INSERT { GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
			         "<" + datasetUri + "> a <" + DCATVocabulary.Dataset + "> , <" + VOIDVocabulary.Dataset + "> . ";
					
 		    for (String s : dataset.getTypeUri()) {
			   insert  += "<" + datasetUri + "> a <" + s + "> . ";
		    }
 		   
 		    boolean bidirectional =  false;
 		    ResourceOption bro =  dataset.getOptionsByType(ResourceOptionType.BIDIRECTIONAL);
 		    if (bro != null && bro.getValue().toString().equalsIgnoreCase("true")) {
 		    	bidirectional = true;
 		    }
 		    
 		    String source = dataset.getLinkByType(ResourceOptionType.SOURCE).getValue().toString();
 		    String target = dataset.getLinkByType(ResourceOptionType.TARGET).getValue().toString();
 		    
 		    insert  += "<" + datasetUri + "> <" + SEMAVocabulary.isAlignmentOf.toString() + "> [ <" + SEMAVocabulary.source + "> <" + source + "> ; <" + SEMAVocabulary.target + "> <" + target + "> ] . ";
 		    
 		    insert += " } } WHERE { }";
 		    
	   		vc.executeSparqlUpdateStatement(insert);
 		   	
 		   	if (bidirectional) {
 		    	executeSaturate(currentUser, vc, dataset);

 				String sinsert = "insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { ";
 		    	sinsert  += "<" + datasetUri  + "> <" + SEMAVocabulary.isAlignmentOf + "> [ <" + SEMAVocabulary.source + "> <" + target + "> ; <" + SEMAVocabulary.target + "> <" + source + "> ] . ";
 		    	sinsert += " } } WHERE { }";
 		    	
 		   		vc.executeSparqlUpdateStatement(sinsert);
 		   	}
 		    
//	 		    if (bidirectional) {
//	 			  logger.info("Saturating bidirectional alignment dataset." );
//	 		   
//	 			  String bidir = "sparql insert { graph <" + datasetUri.toString() + "> { ?p <" + OWLVocabulary.sameAs + "> ?q } } WHERE { graph <" + datasetUri.toString() + "> { ?q <" + OWLVocabulary.sameAs + "> ?p } }";
//
//	 			  stmt.execute(bidir);
//	 		    }
 		    
		} else if (header) {
    	   	
			String insert = 
			   "INSERT { GRAPH <" + resourceVocabulary.getContentGraphResource() + "> { " +
	               "<" + datasetUri + "> a <" + DCATVocabulary.Dataset + "> , <" + VOIDVocabulary.Dataset + "> . ";
			
//			insert  += "<" + datasetUri.toString() + "> <" + DCVocabulary.issued + "> \"" + dateFormat.format(new Date()) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
			
 		    for (String s : dataset.getTypeUri()) {
			   insert  += "<" + datasetUri + "> a <" + s + "> . ";
		    }
 		    
 		    String label = "ASK FROM <" + resourceVocabulary.getContentGraphResource() + "> " +  
 		            " { <" + datasetUri + "> <" + DCTVocabulary.title  + "> ?label }";
	    	
 		    try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(label, Syntax.syntaxARQ))) {
	   		
 		    	if (!qe.execAsk()) {
 				   insert += "<" + datasetUri + "> <" + DCTVocabulary.title + "> \"" + ResourceFactory.createPlainLiteral(dataset.getName()) + "\" . ";
 		    	}
	    	}

 		    
 		    if (dataset.getLinks() != null) {
	 		    for (ResourceOption ro : dataset.getLinks()) {
	 		    	insert  += "<" + datasetUri + "> <" + ResourceOptionType.getProperty(ro.getType()) + "> <" + ro.getValue() + "> . ";
	 		    }
 		    }
 		    
//			Use void:endpoint instead; 		    
 		    String remote = "SELECT ?endpoint FROM <" + resourceVocabulary.getContentGraphResource() + "> " +  
 		            " { <" + datasetUri + "> <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/endpoint" : SEMAVocabulary.endpoint ) + "> ?endpoint }";
	    	
 		    try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(remote, Syntax.syntaxARQ))) {
	   		
 		    	ResultSet rs = qe.execSelect();
 		    	while (rs.hasNext()) {
 		    	   endpoint = rs.next().get("endpoint").toString();
 				}
 		    	
 		    	if (endpoint != null) {
 				   insert += "<" + datasetUri + "> a <" + SEMAVocabulary.RemoteDataset + "> . ";
 		    	}
	    	}
	    	
		    if (dataset.getTypeUri().contains(SEMAVocabulary.DataCatalog.toString()) && dataset.getDatasets() != null) {
		    	for (ObjectId memberId : dataset.getDatasets()) {
		    		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(memberId, new ObjectId(currentUser.getId()));
		    		
		    		if (doc.isPresent()) {
			    		insert  +=	"<" + datasetUri + "> <" + DCTVocabulary.hasPart + "> <" + resourceVocabulary.getDatasetAsResource(doc.get().getUuid())  + "> . ";
		    		}
		    	}	
		    }
		    
// 		    	System.out.println("ENDPOINT" + endpoint);
	    	
 		    // check if thesaurus is SKOS
 		    if (dataset.getTypeUri().contains(SEMAVocabulary.ThesaurusCollection.toString())) {
 		    	String sparql =
 		            "ASK " + (endpoint == null ? fromClause : "") +  
 		            " { ?p a <" + SKOSVocabulary.Concept + "> }";
 		    	
//	 		    	System.out.println(sparql);
 		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ), client)) {
	
	 		    	if (qe.execAsk()) {
	 				   insert += "<" + datasetUri + "> a <" + SEMAVocabulary.SKOSThesaurus + "> . ";
	 				   isSKOS = true;
	 				}
 		    	}
 		    	
 		    	sparql =
 		    			"ASK " + (endpoint == null ? "FROM <" + datasetUri + "> " : "") +  
	 		            " { ?p a <http://www.w3.org/2002/07/owl#Ontology> }";
	 		    	
//	 		    	System.out.println(sparql);
	 		    try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ), client)) {
		
		 		    if (qe.execAsk()) {
		 			   insert += "<" + datasetUri + "> a <" + SEMAVocabulary.OWLOntology + "> . ";
		 			}
	 		    }			 		    	
 		    }	
 		    
            insert += " } } WHERE { }";
            
   	   		vc.executeSparqlUpdateStatement(insert);
		}

		//publication date is not issuance date
//		if (!dataset.getType().equals("annotation-dataset")) {
//			String delete = 
//					   "sparql delete where { graph <" + SEMAVocabulary.contentGraph + "> { " +
//			                   "<" + datasetUri + "> <" + DCTVocabulary.issued + "> ?date } } ";
//
//	   		vc.executeStatement( delete);
//		   	
//			String insert = 
//			   "sparql insert { graph <" + SEMAVocabulary.contentGraph + "> { " +
//	                "<" + datasetUri + "> <" + DCTVocabulary.issued + "> \"" + dateFormat.format(new Date()) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . } } ";
//			
//	   		vc.executeStatement(insert);
//		}
		
		// compute schema 
		if (dataset.getTypeUri().contains(SEMAVocabulary.VocabularyCollection.toString()) || 
				dataset.getTypeUri().contains(SEMAVocabulary.DataCollection.toString()) || dataset.getTypeUri().contains(SEMAVocabulary.DataCatalog.toString())) {
			
			String id = dataset.getIdentifier();

			if (id != null) {
   				String insert = 
   						"insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " + 
   						"<" + datasetUri + ">  <" + DCTVocabulary.identifier + "> \"" + id + "\" . " + 
   				        "<" + datasetUri + ">  <" + VOIDVocabulary.sparqlEndpoint + "> <" + resourceVocabulary.getContentSparqlEnpoint(id) + "> } } WHERE { }" ;
	    				
   				vc.executeSparqlUpdateStatement(insert);
    				
   				// DCAT	    				
   				insert = 
   						"insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " + 
   						" <" + datasetUri + ">  <" + DCATVocabulary.distribution + "> <" + resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql") + "> . " +
                        " <" + resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql") + "> a <" + DCATVocabulary.Distribution + "> . " + 	    								
                        " <" + resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql") + "> <" + DCATVocabulary.accessService + "> <" + resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql") + "> ." +
                        " <" + resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql") + "> <" + DCATVocabulary.accessURL + "> <" + resourceVocabulary.getContentSparqlEnpoint(id) + "> ." +
                        " <" + resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql") + "> a <" + DCATVocabulary.DataService + "> . " +
                        " <" + resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql") + "> <" + DCTVocabulary.conformsTo + "> <https://www.w3.org/TR/sparql11-protocol/> . " +
                        " <" + resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql") + "> <" + DCATVocabulary.endpointURL + "> <" + resourceVocabulary.getContentSparqlEnpoint(id) + "> ." +
                        " <" + resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql") + "> <" + DCATVocabulary.servesDataset + "> <" + datasetUri + "> ." +
                        " } } WHERE { } ";

   				vc.executeSparqlUpdateStatement(insert);
	    	}	
			
	    	//add modified date
		}
		
	    // add languages for Vocabularies
	    if (dataset.getTypeUri().contains(SEMAVocabulary.VocabularyCollection.toString())) {
	    	if (endpoint == null) {
	    		// avoid join (join does not always work) : first get label property
	    		// if this fails also, only solution is to iterate over records
	    		String languages = legacyUris ?
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + resourceVocabulary.getContentGraphResource() + "> {" +
			    		    "<" + datasetUri + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?n . " +
			    		    "?n <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " + 
			    		    "?n <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProperty } } " : 
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + resourceVocabulary.getContentGraphResource() + "> {" +
			    		    "<" + datasetUri + "> <" + SEMAVocabulary.dataProperty + "> ?n . " +
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
			    	languages = "SELECT DISTINCT ?lang " + fromClause + " WHERE { ?p " + label + " ?r . BIND(LANG(?r) AS ?lang) } ";
			    	
//			    	System.out.println(languages);
			    	
			    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(languages, Syntax.syntaxARQ))) {
			    		ResultSet rs = qe.execSelect();
			    		while (rs.hasNext()) {
			    			String lang = rs.next().get("lang").toString();
			    			if (lang.length() > 0) {
			    				String insert = "insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " +
			    						"<" + datasetUri + "> <" + (legacyUris ? "http://purl.org/dc/elements/1.1/language" : DCTVocabulary.language) + ">  \"" + lang + "\" . } } WHERE { }";

				//		   		stmt.execute(insert);
								vc.executeSparqlUpdateStatement(insert);
			    			}
			    		}
			    	} catch (Exception ex) {
			    		ex.printStackTrace();
			    	}
		    	}
		    } else {
		    	String languages = legacyUris ?  
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + resourceVocabulary.getContentGraphResource() + "> {" +
			    		    "<" + datasetUri + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?n . " +
			    		    "?n <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " + 
			    		    "?n <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProperty } } " : 
	    		"SELECT DISTINCT ?labelProperty WHERE { " +
	    		  "GRAPH <" + resourceVocabulary.getContentGraphResource() + "> {" +
	    		    "<" + datasetUri + "> <" + SEMAVocabulary.dataProperty + "> ?n . " +
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
									String insert = "insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " +
								                    "<" + datasetUri + "> <" + (legacyUris ? "http://purl.org/dc/elements/1.1/language" : DCTVocabulary.language) + ">  \"" + lang + "\" . } } WHERE { }";
								    		
									vc.executeSparqlUpdateStatement(insert);
				    			}
				    		}
				    	} catch (Exception ex) {
				    		ex.printStackTrace();
				    	}
			    	}
		    	}
	    
	    
		    if (isSKOS) {
		    	
		    	String schemes = 
		    		"SELECT DISTINCT ?scheme " + (endpoint == null ? fromClause : "") + " WHERE { " +
		    		   " ?p <" + SKOSVocabulary.inScheme + "> ?scheme }";
		    	
//		    	System.out.println(schemes);
	   		
		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(schemes, Syntax.syntaxARQ), client)) {
		    		ResultSet rs = qe.execSelect();
		    		while (rs.hasNext()) {
		    			String scheme = rs.next().get("scheme").toString();
		    			
						String insert = "insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " +
                                        "<" + datasetUri.toString() + "> <" + SEMAVocabulary.scheme + "> <" + scheme + "> . } } WHERE { }";
					    		
						vc.executeSparqlUpdateStatement(insert);
					}
		    	} catch (Exception ex) {
		    		ex.printStackTrace();
		    	}
		    }		    
	    }
	    
	    // update prefixes maps
	    
//	    if (dataset.getType().equals("vocabulary-dataset")) {
	    if (dataset.getScope() == DatasetScope.VOCABULARY && dataset.getDatasetType() == DatasetType.DATASET) {	    	
	    	((VocabulariesBean)context.getBean("vocabularies")).setMap(namesService.createVocabulariesMap(vc, legacyUris));
	    }
	    
//    	((VocabulariesBean)context.getBean("all-datasets")).setMap(AppConfiguration.createDatasetsMap(vc, legacyUris));
	    vm.removeMap(datasetUri);
	    namesService.createDatasetsMap(vm, vc, datasetUri, legacyUris);
	    
	    
	    try {
			if (dataset.getTypeUri().contains(SEMAVocabulary.VocabularyCollection.toString()) || 
					dataset.getTypeUri().contains(SEMAVocabulary.DataCollection.toString()) || dataset.getTypeUri().contains(SEMAVocabulary.DataCatalog.toString())) {
				
				logger.info("Computing schema for " + datasetUri);
				
				Model schema = schemaService.buildSchema(datasetUri.toString(), true);
				schema.clearNsPrefixMap();
				
				Writer sw = new StringWriter();
	
				RDFDataMgr.write(sw, schema, RDFFormat.TTL) ;
				
				String insert = 
						"insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " + sw.toString() + " } } WHERE { }" ;
				
				vc.executeSparqlUpdateStatement(insert);
			}
	    } catch (Exception ex) {
	    	ex.printStackTrace();
	    	return false;
	    }

	    return true;
    }
    

    public void clearDistributionToMetadata(Dataset dataset, TripleStoreConfiguration vc) throws Exception {
    	String datasetUri = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();
    	String datasetUuid = dataset.getUuid();
		
		String graph = resourceVocabulary.getContentGraphResource().toString();

		vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + datasetUri + "> <" + DCATVocabulary.distribution + "> <" + resourceVocabulary.getDistributionAsResource(datasetUuid + "/ttl") + "> } WHERE { }");
		vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + datasetUri + "> <" + DCATVocabulary.distribution + "> <" + resourceVocabulary.getDistributionAsResource(datasetUuid + "/nt") + "> } WHERE { }");
		
		vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(datasetUuid) + "/ttl> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(datasetUuid) + "/ttl> ?p1 ?o1 }");
		vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(datasetUuid) + "/nt> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(datasetUuid) + "/nt> ?p1 ?o1 }");
	
		vc.executeCheckpointStatement();
    }
    
    
    public void addDistributionToMetadata(Dataset dataset, TripleStoreConfiguration vc) throws Exception {
		String id = dataset.getIdentifier();
		String datasetUri = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();
		String datasetUuid = dataset.getUuid();
		
		CreateDistributionState cds = dataset.checkCreateDistributionState(fileSystemConfiguration.getId(), vc.getId());
		
		if (id != null && cds != null) {
			for (SerializationType st : cds.getSerialization()) {
				String format = st.toString().toLowerCase();
				String insert = 
					"insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " + 
					" <" + datasetUri + ">  <" + DCATVocabulary.distribution + "> <" + resourceVocabulary.getDistributionAsResource(datasetUuid + "/" + format) + "> . " +
	                " <" + resourceVocabulary.getDistributionAsResource(datasetUuid + "/" + format) + "> a <" + DCATVocabulary.Distribution + "> . " + 	    								
	                " <" + resourceVocabulary.getDistributionAsResource(datasetUuid + "/" + format) + "> <" + DCTVocabulary.format + "> <" + SerializationType.toFormats(st) + "> . " +
	                " <" + resourceVocabulary.getDistributionAsResource(datasetUuid + "/" + format) + "> <" + DCATVocabulary.accessURL + "> <" + resourceVocabulary.getContentDistribution(id, st) + "> ." +
	                " <" + resourceVocabulary.getDistributionAsResource(datasetUuid + "/" + format) + "> <" + DCATVocabulary.downloadURL + "> <" + resourceVocabulary.getContentDistribution(id, st) + "> ." +
	                " } } WHERE { } ";
	
				vc.executeSparqlUpdateStatement(insert);
			}
    	}	
    }
    
    public boolean unpublish(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, List<MappingDocument> mappings, List<FileDocument> files, boolean metadata, boolean content) throws Exception {

		for (MappingDocument map : mappings) {
			for (MappingInstance mi : map.getInstances()) {
				MappingExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());
				
				if (es != null && es.getExecuteState() == MappingState.EXECUTED) { 
   					for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
   						File f = folderService.getMappingExecutionTrigFile(currentUser, dataset, map, mi, es, i);
   						if (f != null) {
   							vc.executePrepareDeleteStatement(folderService.getParent(f), folderService.getName(f), fileSystemConfiguration.getDataFolder());
   						}
   					}
				}
			}
		}
		
		for (FileDocument fd : files) {
			ProcessStateContainer psv = fd.getCurrentPublishState(virtuosoConfigurations.values());
			FilePublishState ps = (FilePublishState)psv.getProcessState();
			
			if (ps != null) {
				List<File> ff = folderService.getUploadedFiles(currentUser, dataset, fd, ps.getExecute());
				if (files != null) {
					for (File f : ff) {
						vc.executePrepareDeleteStatement(folderService.getParent(f), folderService.getName(f), fileSystemConfiguration.getDataFolder());
					}
	    		}
			}
		}
    	
//    	if (content && dataset.getType().equals("annotation-dataset")) {
    	if (content && dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET) {
    		String sparql =
    			"DELETE { GRAPH <" + dataset.getAsProperty() + "> {" +
	            " ?annId ?p1 ?o1 ." + 
	            " ?o1 ?p2 ?o2 . } } " +            		
	    		"WHERE { " + 
	            "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	            "    <" + resourceVocabulary.getAnnotationSetAsResource(dataset.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
	            "  GRAPH <" + dataset.getAsProperty() + "> {" +
	            "    ?annId ?p1 ?o1 . " +
	            "    OPTIONAL { ?o1 ?p2 ?o2 } } }  ";
	    		
	    	vc.executeSparqlUpdateStatement(sparql);
	    		
	    		sparql =
	            "DELETE WHERE { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " + 
	    		    "<" + resourceVocabulary.getAnnotationSetAsResource(dataset.getUuid()) + "> ?p ?q . } }";
	    		
			vc.executeSparqlUpdateStatement(sparql);
			vc.executeCheckpointStatement();

    	} else {
    		if (metadata) {
//    			ivirtuoso.nestedDelete(vc, SEMAVocabulary.contentGraph.toString(), datasetUri.toString());
    			nestedDelete(vc, resourceVocabulary.getContentGraphResource().toString(), resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString());
    		}
	    	//TODO: unpublish annotations
	    	
	    	if (content) {
		    	logger.info("Clearing graph <" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString() + ">");
	    		vc.executeClearGraphStatement(resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString());
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
    
    public boolean publish(TripleStoreConfiguration vc, AnnotatorContainer ac) throws Exception {

    	Set<String> uploadedFiles = new HashSet<>();
    	
    	
//    	System.out.println("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
    	
//    	String af = folderService.getAnnotationsFolder(currentUser);

    	logger.info("Preparing publication");

    	AnnotatorDocument adoc = ac.getAnnotatorDocument();
		Dataset dataset = ac.getDataset();
		UserPrincipal currentUser = ac.getCurrentUser();

		
//		for (AnnotatorDocument adoc : docs) {
			MappingExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());
			
			if (es.getExecuteShards() > 0) {
				File f = folderService.getAnnotatorExecutionCatalogTrigFile(currentUser, dataset, adoc, es);

				String p = folderService.getParent(f);
				String n = folderService.getName(f);
				
				vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
	
				logger.info("Publication prepare delete " + f.getCanonicalPath());
				
				vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
				vc.prepare(sftpUploadGateway, p, n, uploadedFiles);
	    		
	    		logger.info("Publication prepare load " + f.getCanonicalPath() + " >> " + resourceVocabulary.getAnnotationGraphResource());
	    		
	    		vc.executePrepareLoadStatement(p, n, resourceVocabulary.getAnnotationGraphResource().toString(), fileSystemConfiguration.getDataFolder());
	
	    		logger.info("Publication shards " + Math.max(1, es.getExecuteShards()));
	    		
				for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
	//				String fileName = adoc.getUuid() + (i == 0 ? "" : "_#" + i) + ".trig";
					File ff = folderService.getAnnotatorExecutionTrigFile(currentUser, dataset, adoc, es, i);
					if (ff != null) {
						String fp = folderService.getParent(ff);
						String fn = folderService.getName(ff);
						
						vc.executePrepareDeleteStatement(fp, fn, fileSystemConfiguration.getDataFolder());
						vc.prepare(sftpUploadGateway, fp, fn, uploadedFiles);
			    		vc.executePrepareLoadStatement(fp, fn, adoc.getAsProperty(), fileSystemConfiguration.getDataFolder());
					}
	    		}
			}
//		}
    	
		logger.info("Executing publication");
		
		vc.executeLoadStatement();
    	
//    	logger.info("Deleting publication uploaded files");
	    	
		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
		
	    logger.info("Updating publication annotationset graph");
	    
//		for (AnnotatorDocument adoc : docs) {
			String insert = 
			   "insert { graph <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
                    "<" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()).toString() + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . } } WHERE { }";

    		vc.executeSparqlUpdateStatement(insert);
    		
    		AnnotationEditGroup aeg = aegRepository.findById(adoc.getAnnotatorEditGroupId()).get();
    		String onPropertyString = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
    		
			DatasetCatalog dcg = schemaService.asCatalog(dataset.getUuid());
			String usingClause = schemaService.buildUsingClause(dcg);

    		String sinsert = 
//    		   "INSERT { GRAPH <" + adoc.getAsProperty() + "> { ?target <" + DCTVocabulary.isReferencedBy + "> ?property } } WHERE { " +
//    		   "  GRAPH <" + resourceVocabulary.getDatasetAsResource(adoc.getDatasetUuid()) + "> { " +
//    		   "     ?s ?property ?body ." + 
//    		   "  } " +
//    		   "  GRAPH <" + adoc.getAsProperty() + "> { " +
//    		   "    ?v a <" + OAVocabulary.Annotation + "> . " +
//    		   "    ?v <" + OAVocabulary.hasTarget + "> ?target . " + 
//    		   "    ?target <" + OAVocabulary.hasSource + ">  ?s . " +
//    		   "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
//    		   "    ?v <" + OAVocabulary.hasBody + ">  ?body . FILTER ( !isBlank(?body) ) " +
//    		   "    ?v <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()) + "> . " +
//    		   "  } " +
//    		   "}";
    	    		   "INSERT { " +
                       "  GRAPH <" + adoc.getAsProperty() + "> { ?target <" + DCTVocabulary.isReferencedBy + "> ?property } } " +
    	    		   usingClause +
    	    		   "USING NAMED <" + adoc.getAsProperty() + "> " + 
    	    		   "WHERE { " +
    	    		   "  ?s ?property ?body ." + 
    	    		   "  GRAPH <" + adoc.getAsProperty() + "> { " +
    	    		   "    ?v a <" + OAVocabulary.Annotation + "> . " +
    	    		   "    ?v <" + OAVocabulary.hasTarget + "> ?target . " + 
    	    		   "    ?target <" + OAVocabulary.hasSource + ">  ?s . " +
    	    		   "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
    	    		   "    ?v <" + OAVocabulary.hasBody + ">  ?body . FILTER ( !isBlank(?body) ) " +
    	    		   "    ?v <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()) + "> . " +
    	    		   "  } " +
    	    		   "}";    				
    		
//    		System.out.println(sinsert);
    		
//    		UpdateRequest ur = UpdateFactory.create();
//    		ur.add(sinsert);
    		
    		vc.executeSparqlUpdateStatement(sinsert);
    		
//    		System.out.println("OK");
    		
//		}
    	
		vc.executeCheckpointStatement();

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
    
    public boolean unpublish(TripleStoreConfiguration vc, AnnotatorContainer ac) throws Exception {
    	
//    	virtuoso.nestedDelete(SEMAVocabulary.contentGraph.toString(), SEMAVocabulary.getDataset(dataset.getUuid()).toString());
    	
//		for (AnnotatorDocument doc : docs) {
    	
    	AnnotatorDocument adoc = ac.getAnnotatorDocument();

			logger.info("Annotation unpublishing " + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()));

			int iter = 0;
			while (true) {
	  			AnnotatorPublishState aps = adoc.checkPublishState(vc.getId());
    			String publishedAsProperty = aps.getAsProperty();
    			
    			// for compatibility
    			if (publishedAsProperty == null) { 
    				publishedAsProperty =  adoc.getAsProperty();
    			}
    			
    			String select = "SELECT (count(?annId) AS ?count) WHERE { " +
    		            "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
    		            "    <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
    		            "  GRAPH <" + publishedAsProperty + "> {" +
    				    "    ?annId a <" + OAVocabulary.Annotation + "> } }"; 
//    			String select = "SELECT (count(?annId) AS ?count) WHERE { " +
//	            "  GRAPH <" + publishedAsProperty + "> {" +
//			    "    ?annId a <" + OAVocabulary.Annotation + "> ; <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotatorAsResource(doc.getUuid()) + "> } }"; 
    			
    			logger.info("Counting annotations to unpublish " + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()));
//	    			System.out.println(QueryFactory.create(select, Syntax.syntaxSPARQL_11));
    			
    			int count = TripleStoreUtils.count(vc, select);
    			
    			logger.info("Annotations to unpublish " + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + ": " + count);
    			
    			if (count == 0) {
    				break;
    			}
    			
      		String sparql =
	            "DELETE { GRAPH <" + publishedAsProperty + "> {" +
	            " ?annId ?p1 ?o1 ." + 
	            " ?o1 ?p2 ?o2 . } } " +            		
	    		"WHERE { " + 
	            "  GRAPH <" + publishedAsProperty + "> {" +
	            "    ?annId ?p1 ?o1 . " +
	            "    OPTIONAL { ?o1 ?p2 ?o2 } } " + 
	            "  { SELECT ?annId WHERE { " +
	            "      GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	            "        <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
	            "   } LIMIT 100000 OFFSET " + iter*100000 + " " +
	            " } } ";
	    		
	    		logger.info("Unpublishing " + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + " batch #" + iter);
	    		
	    		vc.executeSparqlUpdateStatement(sparql);
	    		
	    		logger.info("Unpublishing " + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + " batch #" + iter + " completed.");

	    		iter++;
			}
			
			logger.info("Unpublishing annotationset for " + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()));
			
    		String sparql =
            "DELETE WHERE { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
            " <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> ?p ?q . } }";
    		
    		vc.executeSparqlUpdateStatement(sparql);
    		
    		logger.info("Unpublication of " + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + " completed.");
//		}
		
		vc.executeCheckpointStatement();

		return true;

    }    
    
    public boolean publish(TripleStoreConfiguration vc, EmbedderContainer ec) throws Exception {

    	Set<String> uploadedFiles = new HashSet<>();
    	
    	logger.info("Preparing embedder publication");

    	EmbedderDocument edoc = ec.getEmbedderDocument();
		Dataset dataset = ec.getDataset();
		MappingExecuteState es = edoc.getExecuteState(fileSystemConfiguration.getId());
		
		String targetGraph = resourceVocabulary.getDatasetEmbeddingsAsResource(dataset.getUuid()).toString();
			
		if (es.getExecuteShards() > 0) {
			for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
				File ff = folderService.getExecutionTrigFile(ec, es, i);
				if (ff != null) {
					String fp = folderService.getParent(ff);
					String fn = folderService.getName(ff);
					
					vc.executePrepareDeleteStatement(fp, fn, fileSystemConfiguration.getDataFolder());
					vc.prepare(sftpUploadGateway, fp, fn, uploadedFiles);
		    		vc.executePrepareLoadStatement(fp, fn, targetGraph, fileSystemConfiguration.getDataFolder());
				}
    		}
		}
    	
		logger.info("Publishing embedder");
		
		vc.executeLoadStatement();
		vc.executeCheckpointStatement();
		
		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
		
		logger.info("Embedder publication completed");
		
		return true;
    }       
    
    public boolean unpublish(TripleStoreConfiguration vc, EmbedderContainer ec) throws Exception {

    	EmbedderDocument edoc = ec.getEmbedderDocument();
    	Dataset dataset = ec.getDataset();
    	
    	String targetGraph = resourceVocabulary.getDatasetEmbeddingsAsResource(dataset.getUuid()).toString();
    	
		logger.info("Embedder unpublishing " + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()));

		int iter = 0;
		while (true) {

			String select = 
   				"SELECT (count(?annId) AS ?count) WHERE { " +
	            "  GRAPH <" + targetGraph + "> {" +
			    "    ?annId a <" + OAVocabulary.Annotation + "> ; <" + ASVocabulary.generator + "> <" + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()) + "> } }"; 
			
			logger.info("Counting annotations to unpublish " + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()));
			
			int count = TripleStoreUtils.count(vc, select);
			
			logger.info("Annotations to unpublish " + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()) + ": " + count);
			
			if (count == 0) {
				break;
			}
			
	  		String sparql =
	            "DELETE { GRAPH <" + targetGraph + "> {" +
	            " ?annId ?p1 ?o1 ." + 
	            " ?o1 ?p2 ?o2 . } } " +            		
	    		"WHERE { " + 
	            "  GRAPH <" + targetGraph + "> {" +
	            "    ?annId ?p1 ?o1 . " +
	            "    OPTIONAL { ?o1 ?p2 ?o2 } } " + 
	            "  { SELECT ?annId WHERE { " +
	            "      GRAPH <" + targetGraph + "> { " +
	            "         ?annId a <" + OAVocabulary.Annotation + "> ; <" + ASVocabulary.generator + "> <" + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()) + "> } " +
	            "   } LIMIT 100000 OFFSET " + iter*100000 + " " +
	            " } } ";
	    		
    		logger.info("Unpublishing " + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()) + " batch #" + iter);
    		
    		vc.executeSparqlUpdateStatement(sparql);
    		
    		logger.info("Unpublishing " + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()) + " batch #" + iter + " completed.");

    		iter++;
		}
		
		logger.info("Unpublication of " + resourceVocabulary.getEmbedderAsResource(edoc.getUuid()) + " completed.");
		
		vc.executeCheckpointStatement();

		return true;
	}
    
//    public boolean publish(UserPrincipal currentUser, String virtuoso, PagedAnnotationValidation pav, List<AnnotationEdit> deletes) throws Exception {
//
//    	TripleStoreConfiguration vc =  virtuosoConfigurations.get(virtuoso);
//    	
////    	System.out.println("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
//    	
//    	//DELETE first
//	
//		String onPropertyString = AnnotationEditGroup.onPropertyListAsString(pav.getOnProperty());
//		String annfilter = aegService.annotatorFilter("v", pav.getAnnotatorDocumentUuid());
//		
//		for (AnnotationEdit edit :  deletes) {
//			String sparql = 
//				"DELETE { " + 
//			    "  GRAPH <" + pav.getAsProperty() + "> {" +
//		        "    ?annId ?p1 ?o1 ." + 
//		        "    ?o1 ?p2 ?o2 . } " +
//		        "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
//                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +
//		        "} " +            		
//		    	"WHERE { " + 
//		        "  GRAPH <" + pav.getAsProperty() + "> { " + 
//			    "   ?v a <" + OAVocabulary.Annotation + "> ?annId . " + 
//		        "   ?annId <" + OAVocabulary.hasBody + "> <" + edit.getAnnotationValue() + "> . " +
//		        "   ?annId <" + OAVocabulary.hasTarget + "> ?target . " +
//			    annfilter +
//		        "   ?target  <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
//		        "            <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " ; " +
//		        "            <" + OAVocabulary.hasSource + "> ?s  . " +
//	            "   ?annId ?p1 ?o1 . " +
//	            "   OPTIONAL { ?o1 ?p2 ?o2 } } . " +	    			        
//		        " GRAPH <" + resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString() + "> { " +
//		        "  ?s " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
//                " GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
//                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +		    			        
//		        "}";
//			
////				System.out.println(sparql);
//		    	
//			vc.executeSparqlUpdateStatement(sparql);
//		}
//
//    	Set<String> uploadedFiles = new HashSet<>();
//
//    	String mf = folderService.getAnnotationsFolder(currentUser) + "/" + pav.getDatasetUuid();
//    	
//    	//INSERT then
//		vc.executePrepareDeleteStatement(mf, pav.getUuid() + "_add_catalog.trig", fileSystemConfiguration.getDataFolder());
//		vc.executePrepareDeleteStatement(mf, pav.getUuid() + "_add.trig", fileSystemConfiguration.getDataFolder());
//
//		vc.prepare(sftpUploadGateway, mf, pav.getId() + "_add_catalog.trig", uploadedFiles);
//		vc.executePrepareLoadStatement(mf, pav.getUuid() + "_add_catalog.trig", resourceVocabulary.getAnnotationGraphResource().toString(), fileSystemConfiguration.getDataFolder());
//		vc.prepare(sftpUploadGateway, mf, pav.getId() + "_add.trig", uploadedFiles);
//		vc.executePrepareLoadStatement(mf, pav.getUuid() + "_add.trig", pav.getAsProperty(), fileSystemConfiguration.getDataFolder());
//
//		vc.executeLoadStatement();
//    	
//	    for (String f : uploadedFiles) {
//			vc.deleteFile(sftpDeleteGateway, f);
//		}
//		
//	    vc.executeCheckpointStatement();
//
//		return true;
//
//    }    
    
//    public boolean unpublish(UserPrincipal currentUser, String virtuoso, PagedAnnotationValidation pav, List<AnnotationEdit> adds) throws Exception {
//
//    	TripleStoreConfiguration vc =  virtuosoConfigurations.get(virtuoso);
//    	
//    	Set<String> uploadedFiles = new HashSet<>();
////    	System.out.println("ld_dir('" + annotationsFolder.substring(0, annotationsFolder.length() - 1) + "', '" + adoc.getUuid() + ".trig', '" + adoc.getAsProperty() + "')");
//    	
//    	String mf = folderService.getAnnotationsFolder(currentUser) + "/" + pav.getDatasetUuid();
//    	
//    	// INSERT DELETES
//		vc.executePrepareDeleteStatement(mf, pav.getUuid() + "_delete_catalog.trig", fileSystemConfiguration.getDataFolder());
//		vc.executePrepareDeleteStatement(mf, pav.getUuid() + "delete.trig", fileSystemConfiguration.getDataFolder());
//
//		vc.prepare(sftpUploadGateway, mf, pav.getUuid() + "_delete_catalog.trig", uploadedFiles);
//		vc.executePrepareLoadStatement(mf, pav.getUuid() + "_delete_catalog.trig", resourceVocabulary.getAnnotationGraphResource().toString(), fileSystemConfiguration.getDataFolder());
//		vc.prepare(sftpUploadGateway, mf, pav.getId() + "_delete.trig", uploadedFiles);
//		vc.executePrepareLoadStatement(mf, pav.getUuid() + "_delete.trig", pav.getAsProperty(), fileSystemConfiguration.getDataFolder());
//		
//		vc.executeLoadStatement();
//	    
//    	for (String f : uploadedFiles) {
//			vc.deleteFile(sftpDeleteGateway, f);
//		}
//		
//    	//DELETE ADDS
//		String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());
//		String annfilter = aegService.annotatorFilter("v", pav.getAnnotatorDocumentUuid());
//
//		for (AnnotationEdit edit :  adds) {
//			String sparql =
//				"DELETE { " + 
//			    "  GRAPH <" + pav.getAsProperty() + "> {" +
//		        "    ?annId ?p1 ?o1 ." + 
//		        "    ?o1 ?p2 ?o2 . } " +
//		        "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
//                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +
//		        "} " +            		
//		    	"WHERE { " + 
//		        "  GRAPH <" + pav.getAsProperty() + "> { " + 
//			    "   ?v a <" + OAVocabulary.Annotation + "> ?annId . " + 
//		        "   ?annId <" + OAVocabulary.hasBody + "> <" + edit.getAnnotationValue() + "> . " +
//		        "   ?annId <" + OAVocabulary.hasTarget + "> ?target . " +
//			    annfilter +
//		        "   ?target  <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
//		        "            <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " ; " +
//		        "            <" + OAVocabulary.hasSource + "> ?s  . " +
//	            "   ?annId ?p1 ?o1 . " +
//	            "   OPTIONAL { ?o1 ?p2 ?o2 } } . " +
//		        " GRAPH <" + resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString() + "> { " +
//		        "  ?s " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
//                " GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
//                "    ?adocid <" + DCTVocabulary.hasPart + "> ?annId . } " +		    			        
//		    	"} ";
//			
////				System.out.println(sparql);
//		    	
//			vc.executeSparqlUpdateStatement(sparql);
//		}
//    	
//		vc.executeCheckpointStatement();
//
//		return true;
//
//    }        
    
    public boolean publish(TripleStoreConfiguration vc, AnnotationValidationContainer avc) throws Exception {

    	logger.info("Preparing annotation validation publication");
    	
    	AnnotationValidation av = avc.getAnnotationValidation();
		Dataset dataset = avc.getDataset();
    	MappingExecuteState es = av.getExecuteState(fileSystemConfiguration.getId());
    		
    	String targetGraph = av.getAsProperty();
    	
    	Set<String> uploadedFiles = new HashSet<>();

    	if (es.getExecuteShards() > 0) {
			for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
				File ff = folderService.getExecutionTrigFile(avc, es, i);
				if (ff != null) {
					String fp = folderService.getParent(ff);
					String fn = folderService.getName(ff);
		
					vc.executePrepareDeleteStatement(fp, fn, fileSystemConfiguration.getDataFolder());
					vc.prepare(sftpUploadGateway, fp, fn, uploadedFiles);
		    		vc.executePrepareLoadStatement(fp, fn, targetGraph, fileSystemConfiguration.getDataFolder());
				}
			}
    	}    	
    	
		logger.info("Executing annotation validation publication");
		
		vc.executeLoadStatement();
    	
		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
		
	    logger.info("Updating annotation validation annotationset graph");
	    
		String insert = 
    			"INSERT { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
    					"  <" + resourceVocabulary.getAnnotationSetAsResource(av.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation } }" +
    					"WHERE { " +
    					"  GRAPH <" + av.getAsProperty() + "> { " +
    					"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
    					"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
    					"    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> ." + // added annotations
    					"    ?annotation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> . } " +
    					"} ";
    					
		vc.executeSparqlUpdateStatement(insert);
		vc.executeCheckpointStatement();
    	
    	AnnotationEditGroup aeg = aegRepository.findById(av.getAnnotationEditGroupId()).get();
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
    		
		DatasetCatalog dcg = schemaService.asCatalog(dataset.getUuid());
		String usingClause = schemaService.buildUsingClause(dcg);
		
    	String sinsert = 
    		   "INSERT { GRAPH <" + av.getAsProperty() + "> { " + 
    	       "   ?target <" + DCTVocabulary.isReferencedBy + "> ?property } }" +
    	       usingClause + 
    	       "USING NAMED <" + av.getAsProperty() + "> " +
    	       "WHERE { " +
     		   "  ?source ?property ?body . " + 
    		   "  GRAPH <" + av.getAsProperty() + "> { " +
    		   "    ?annotation a <" + OAVocabulary.Annotation + "> . " +
    		   "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    		   "    ?target <" + OAVocabulary.hasSource + ">  ?source . " +
    		   "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
    		   "    ?annotation <" + OAVocabulary.hasBody + ">  ?body . " +
			   "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
    		   "    ?annotation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> . " + // added annotations
			   "    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> ." +     		   
    		   "  } " +
    		   "}";
    		
//  		System.out.println(sinsert);

		vc.executeSparqlUpdateStatement(sinsert);
		vc.executeCheckpointStatement();
		
    	logger.info("Annotation validation publication completed");
    		
		return true;

    }        
    
    public boolean unpublish(TripleStoreConfiguration vc, AnnotationValidationContainer avc) throws Exception {

		logger.info("Unpublishing annotation validation");
		
    	AnnotationValidation av = avc.getAnnotationValidation();

		// delete added paged validations
		String sparql =
				"DELETE { " + 
			    "  GRAPH <" + av.getAsProperty() + "> {" +
			    "    ?annotation ?p1 ?o1 . " +
			    "    ?o1 ?p2 ?p2 . } " +
		        "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
                "    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " +
		        "} WHERE { " + 
		        "  GRAPH <" + av.getAsProperty() + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
				"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
				"    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> ." + // added annotations validator = generator
				"    ?annotation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> ." +
				"    ?annotation ?p1 ?o1 . " +
				"    OPTIONAL { ?o1 ?p2 ?o2 } } " +
				"  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
				"    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation } " +
		    	"} ";
		
		vc.executeSparqlUpdateStatement(sparql);

		// delete validations in original annotations
		sparql =
				"DELETE { " + 
			    "  GRAPH <" + av.getAsProperty() + "> {" +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation ?p ?o } " +
		        "} WHERE { " + 
		        "  GRAPH <" + av.getAsProperty() + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> . " +
			    "    ?validation ?p ?o } " + 
		    	"} ";
		
		vc.executeSparqlUpdateStatement(sparql);
		vc.executeCheckpointStatement();

    	logger.info("Annotation validation unpublication completed");
    	
		return true;

    }  


    public boolean publish(UserPrincipal currentUser, String virtuoso, VocabularizerDocument doc) throws Exception {

    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);
    	
    	Set<String> uploadedFiles = new HashSet<>();
    	
    	String vf = folderService.getVocabularizerFolder(currentUser);
    	
		vc.executePrepareDeleteStatement(vf, doc.getUuid() + "_catalog.trig", fileSystemConfiguration.getDataFolder());
		vc.executePrepareDeleteStatement(vf, doc.getUuid() + ".trig", fileSystemConfiguration.getDataFolder());

		vc.prepare(sftpUploadGateway, vf, doc.getUuid() + "_catalog.trig", uploadedFiles);
		vc.executePrepareLoadStatement(vf, doc.getUuid() + "_catalog.trig", resourceVocabulary.getContentGraphResource().toString(), fileSystemConfiguration.getDataFolder());
		vc.prepare(sftpUploadGateway, vf, doc.getUuid() + ".trig", uploadedFiles);
		vc.executePrepareLoadStatement(vf, doc.getUuid() + ".trig", resourceVocabulary.getDatasetAsResource(doc.getUuid()).toString(), fileSystemConfiguration.getDataFolder());
    	
		vc.executeLoadStatement();
    	
		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
		
		String insert = 
		   "insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " +
                   "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . " +
                   "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.Autogenerated + "> . " +
		           "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.VocabularyCollection + "> . " +
		           "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.ThesaurusCollection + "> . " + 
		           "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.SKOSThesaurus + "> . } } WHERE { }";

		vc.executeSparqlUpdateStatement(insert);
		vc.executeCheckpointStatement();
    	
		return true;

    }        
    
    public boolean unpublish(String virtuoso, VocabularizerDocument doc) throws Exception {
    	
    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);

//    	ivirtuoso.nestedDelete(vc, SEMAVocabulary.contentGraph.toString(), SEMAVocabulary.getDataset(doc.getUuid()).toString());
    	nestedDelete(vc, resourceVocabulary.getContentGraphResource().toString(), resourceVocabulary.getDatasetAsResource(doc.getUuid()).toString());
    	//TODO: unpublish annotations
    	
//    	System.out.println("sparql clear graph <" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + ">");
		vc.executeClearGraphStatement(resourceVocabulary.getDatasetAsResource(doc.getUuid()).toString());
		vc.executeCheckpointStatement();

		return true;

    }    
    
    public void resetAccessGraph(String virtuoso) throws Exception {
    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);
    	
		vc.executeClearGraphStatement(resourceVocabulary.getAccessGraphResource().toString());
		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> a <" + SACCVocabulary.Group.toString() + "> } } WHERE { }");
		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> a <" + SACCVocabulary.PublicGroup.toString() + "> } } WHERE { }");
		vc.executeCheckpointStatement();
    }
    
    public void addUserToAccessGraph(String virtuoso, String uuid) throws Exception {

    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);
    	
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getUser(uuid).toString() + "> a <" + SACCVocabulary.User.toString() + "> } } ");
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getGroup(uuid).toString() + "> a <" + SACCVocabulary.Group.toString() + "> } } ");    		
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.defaultGroup.toString() + "> <" + SACCVocabulary.member.toString() + "> <" + SEMAVocabulary.getUser(uuid).toString() + "> } } ");
//    	System.out.println("sparql insert { graph <" + SEMAVocabulary.accessGraph.toString() + "> { <" + SEMAVocabulary.getGroup(uuid).toString() + "> <" + SACCVocabulary.member.toString() + ">  <" + SEMAVocabulary.getUser(uuid).toString() + "> } } ");    		

		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getUserAsResource(uuid)+ "> a <" + SACCVocabulary.User + "> } } WHERE { }");
		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getGroupAsResource(uuid) + "> a <" + SACCVocabulary.Group + "> } } WHERE { } ");
		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> <" + SACCVocabulary.member + "> <" + resourceVocabulary.getUserAsResource(uuid) + "> } } WHERE { } ");
		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getGroupAsResource(uuid) + "> <" + SACCVocabulary.member + ">  <" + resourceVocabulary.getUserAsResource(uuid) + "> } } WHERE { }");
		vc.executeCheckpointStatement();
    }
    
    public void addDatasetToAccessGraph(UserPrincipal currentUser, TripleStoreConfiguration vc, String datasetUuid, boolean isPublic) throws Exception {

    	removeDatasetFromAccessGraph(vc, datasetUuid);
    	
		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getGroupAsResource(currentUser.getUuid()) + "> <" + SACCVocabulary.dataset + "> <" + resourceVocabulary.getDatasetAsResource(datasetUuid) + "> } } WHERE { }");
   		if (isPublic) {
			vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> <" + SACCVocabulary.dataset + "> <" + resourceVocabulary.getDatasetAsResource(datasetUuid) + "> } } WHERE { }");
    	}
   		
   		vc.executeCheckpointStatement();
    }
    
    public void removeDatasetFromAccessGraph(TripleStoreConfiguration vc, String datasetUuid) throws Exception {

		vc.executeSparqlUpdateStatement("delete where { graph <" + resourceVocabulary.getAccessGraphResource() + "> { ?s <" + SACCVocabulary.dataset + "> <" + resourceVocabulary.getDatasetAsResource(datasetUuid) + "> } } ");
		vc.executeCheckpointStatement();
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
    


    private void nestedDelete(TripleStoreConfiguration vc, String graph, String uri) throws Exception {
    	
    	logger.info("Nested dataset metadata delete: " + graph + " : " + uri);
    	
		StringBuffer sb = null;
		int i;
		for (i = 1; ; i++) {
			sb = new StringBuffer();
			
//			sb.append("SELECT DISTINCT COUNT(?o" + i + ")  FROM <" + graph + "> WHERE { <" + uri + "> ?p1 ?o1 . ");
			sb.append("SELECT (COUNT(DISTINCT ?o" + i + ") AS ?count)  FROM <" + graph + "> WHERE { <" + uri + "> ?p1 ?o1 . ");
			for (int j = 2; j <= i; j++) {
				sb.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
			}
			for (int j = 2; j <= i; j++) {
				sb.append(" } ");
			}
			
			sb.append("}");
			
//			System.out.println(QueryFactory.create(sb.toString(), Syntax.syntaxSPARQL_11));
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sb.toString(), Syntax.syntaxSPARQL_11))) {
				ResultSet results = qe.execSelect();
				String name = results.getResultVars().get(0);
				
				QuerySolution qs = results.next();
	
//				System.out.println(qs.get(name));
	
				if (qs.get(name).asLiteral().getInt() == 0) {
					break;
				}
			}
		}
		
		StringBuffer db = null;
		for (int k = 1; k < i; k++) {
			db = new StringBuffer();
			
			db.append("WITH <" + graph + "> DELETE { ?s ?p1 ?o1 . ");
			for (int j = 2; j <= k; j++) {
				db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
			}
			db.append("} WHERE { VALUES ?s { <" + uri + "> }  ?s ?p1 ?o1 . ");
			for (int j = 2; j <= k; j++) {
				db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
			}			
			for (int j = 2; j <= k; j++) {
				db.append(" } ");
			}
			db.append("}");
			
		}
		
		if (db != null) {
			logger.info("Deleting dataset metadata: " + db.toString());
			
			vc.executeSparqlUpdateStatement(db.toString());
			
			if (uri.startsWith(resourceVocabulary.getDatasetAsResource("").toString())) {
				String uuid = resourceVocabulary.getUuidFromResourceUri(uri);
				
				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/sparql> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/sparql> ?p1 ?o1 }");
				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDataServiceAsResource(uuid) + "/sparql> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDataServiceAsResource(uuid) + "/sparql> ?p1 ?o1 }");
				
				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/ttl> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/ttl> ?p1 ?o1 }");
				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/nt> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/nt> ?p1 ?o1 }");
			}
			
			vc.executeCheckpointStatement();
		}

    }
 
}
