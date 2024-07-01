package ac.software.semantic.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.http.client.HttpClient;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.config.SFTPAdaptor.SftpDeleteGateway;
import ac.software.semantic.config.SFTPAdaptor.SftpUploadGateway;
import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.RemoteTripleStore;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.TripleStoreConfiguration;
//import ac.software.semantic.model.VocabularizerDocument;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.MappingType;
import ac.software.semantic.model.constants.type.ResourceOptionType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.FileDocumentRepository;
import ac.software.semantic.repository.core.MappingDocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.FileService.FileContainer;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.container.AnnotationValidationContainer;
import ac.software.semantic.service.container.SideExecutableContainer;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.DCATVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import ac.software.semantic.vocs.SACCVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;

@Component
public class TripleStore {

	private Logger logger = LoggerFactory.getLogger(TripleStore.class);

    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

    @Value("${backend.server}")
    private String server;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;
	
	@Value("${virtuoso.graphs.separate:#{true}}")
	private boolean separateGraphs;

	@Autowired
	private ResourceLoader resourceLoader;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private MappingDocumentRepository mappingRepository;

	@Autowired
	private FileDocumentRepository fileRepository;

	@Autowired
	private ServiceUtils serviceUtils;
	
	@Autowired
	private SchemaService schemaService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private SparqlQueryService sparqlQueryService;
	
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
	@Qualifier("triplestore-configurations") 
	private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
	@Autowired
	private FolderService folderService;

	@Lazy
	@Autowired
	private DatasetService datasetService;

    public TripleStore(@Qualifier("triplestore-configurations") ConfigurationContainer<TripleStoreConfiguration> vcs,
    		           @Qualifier("filesystem-configuration") FileSystemConfiguration fsc) throws Exception {
    	
    	this.fileSystemConfiguration = fsc;
    }

    
    public boolean setPublishLastModified(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, Date date) throws Exception {
    	
    	PublishState<?> fps = dataset.checkFirstPublishState(vc.getId());
    	String insert = "INSERT { GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { "; 

    	if (fps != null) {
			Date issueDate = fps.getPublishStartedAt();
			
    		insert += "<" + dataset.asResource(resourceVocabulary) + "> <" + DCTVocabulary.issued + "> \"" + dateFormat.format(issueDate) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
    		insert += "<" + dataset.asResource(resourceVocabulary) + "> <" + DCTVocabulary.modified + "> \"" + dateFormat.format(date) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
    	} else {
	    	insert += "<" + dataset.asResource(resourceVocabulary) + "> <" + DCTVocabulary.issued + "> \"" + dateFormat.format(date) + "\"^^<" + XSDDatatype.XSDdateTime.getURI() + "> . ";
    	}
    	
    	insert += "} } WHERE { } ";
    	
   		vc.executeSparqlUpdateStatement(insert);
   		vc.executeCheckpointStatement();
	    	
		return true;

    }

    
    public boolean publish(UserPrincipal currentUser, DatasetContainer dc, MappingContainer mc) throws Exception {

    	Dataset dataset = dc.getObject();
    	
    	if (dataset.isLocal()) {
        	TripleStoreConfiguration vc = dc.getDatasetTripleStoreVirtuosoConfiguration();

			MappingExecuteState es = mc.checkExecuteState();
			MappingDocument mdoc = mc.getObject();
			MappingType type = mdoc.getType();
			
			if (mdoc.isActive() && es != null && es.getExecuteState() == MappingState.EXECUTED) {
		    	Set<String> uploadedFiles = new HashSet<>();

				for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
					File f = folderService.getMappingExecutionTrigFile(currentUser, dataset, mc.getObject(), mc.getMappingInstance(), es, i);
    				String p = folderService.getParent(f);
    				String n = folderService.getName(f);
					
					vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
					
					String targetGraph = null;
					if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET && dataset.getAsProperty() != null) { // legacy
	    				targetGraph = dataset.getAsProperty();
					} else {
		    			if (type == MappingType.HEADER) {
		    				targetGraph = dataset.getMetadataTripleStoreGraph(resourceVocabulary);
		    			} else if (type == MappingType.CONTENT) {
		    				targetGraph = dataset.getContentTripleStoreGraph(resourceVocabulary, mdoc.getGroup());
				    	}
					}

	    			if (targetGraph != null) {
    					vc.prepare(sftpUploadGateway, p, n, uploadedFiles);
    					vc.executePrepareLoadStatement(p, n, targetGraph, fileSystemConfiguration.getDataFolder());
	    			}
    			}
				
		    	vc.executeLoadStatement();
			    
				vc.deleteFiles(sftpDeleteGateway, uploadedFiles);

			}
			
		    if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET && dataset.getAsProperty() != null) { // legacy	    
	    		String insert = 
	    			"INSERT { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	    					"<" + resourceVocabulary.getAnnotationSetAsResource(dataset.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?p } }" +
	    					"WHERE { GRAPH <" + dataset.getAsProperty() + "> { " +
	    					       " ?p a <" + OAVocabulary.Annotation + "> . } " +
	    					"FILTER NOT EXISTS { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	    						   " ?g <" + DCTVocabulary.hasPart + "> ?p } } } ";
	    					
	    		vc.executeSparqlUpdateStatement(insert);
		    } 
		    
	    	vc.executeCheckpointStatement();
    	}
	    	
		return true;

    }
    
    public boolean unpublish(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, MappingDocument mapping, MappingInstance mi) throws Exception {
    	
    	MappingExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());

		for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
			File f = folderService.getMappingExecutionTrigFile(currentUser, dataset, mapping, mi, es, i);
				
			if (!f.exists()) {
				logger.warn("Failed to unpublish mapping " + mapping.getUuid());
				return false;
			}
			
			try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			    String line;
		    	StringBuffer entry = new StringBuffer("DELETE DATA FROM <" + dataset.asResource(resourceVocabulary) + "> {\n ");
		    	int count = 0;
			    while ((line = br.readLine()) != null) {
			    	
			    	if (line.length() == 0) {
					    if (count > 0) {
				    		entry.append("}");
				    		
//				    		System.out.println(entry);

				    		vc.executeSparqlUpdateStatement(entry.toString());
					    }
			    		
			    		entry = new StringBuffer("DELETE DATA FROM <" + dataset.asResource(resourceVocabulary) + "> {\n ");
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
    
    public boolean publish(UserPrincipal currentUser, DatasetContainer dc, FileContainer fc) throws Exception {

    	Dataset dataset = dc.getObject();
    	TripleStoreConfiguration vc = dc.getDatasetTripleStoreVirtuosoConfiguration();
    	
    	if (dataset.isLocal()) {
	    	Set<String> uploadedFiles = new HashSet<>();

	    	FileExecuteState es = fc.checkExecuteState();
	    	FileDocument fd = fc.getObject();
	    	
	    	if (fd.isActive() && es != null && es.getExecuteState() == MappingState.EXECUTED) {
				String targetGraph = dataset.getContentTripleStoreGraph(resourceVocabulary, fd.getGroup());
				
	    		for (File f : folderService.getUploadedFiles(currentUser, dataset, fd, es)) {
					String p = folderService.getParent(f);
					String n = folderService.getName(f);
	    			
	    			vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
	   				vc.prepare(sftpUploadGateway, p, n, uploadedFiles);
					vc.executePrepareLoadStatement(p, n, targetGraph, fileSystemConfiguration.getDataFolder());
				}
			
		    	vc.executeLoadStatement();
			    
				vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
	    	}
    	
		    if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET && dataset.getAsProperty() != null) { // legacy	    
	    		String insert = 
	    			"INSERT { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	    					"<" + resourceVocabulary.getAnnotationSetAsResource(dataset.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?p } }" +
	    					"WHERE { GRAPH <" + dataset.getAsProperty() + "> { " +
	    					       " ?p a <" + OAVocabulary.Annotation + "> . } " +
	    					"FILTER NOT EXISTS { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
	    						   " ?g <" + DCTVocabulary.hasPart + "> ?p } } } ";
	    					
	    		vc.executeSparqlUpdateStatement(insert);
		    } 
		    
	    	vc.executeCheckpointStatement();
    	}
    	
		return true;

    }

    public boolean unpublish(DatasetContainer dc, boolean metadata, boolean content, int group) throws Exception {

    	UserPrincipal currentUser = dc.getCurrentUser();
    	Dataset dataset = dc.getObject();
    	TripleStoreConfiguration vc = dc.getDatasetTripleStoreVirtuosoConfiguration();
    	
		List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(dataset.getId(), new ObjectId(currentUser.getId()));
		
		for (MappingDocument map : mappings) {
			if (map.getGroup() != group) {
				continue;
			}
			
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

		List<FileDocument> files = fileRepository.findByDatasetIdInAndFileSystemConfigurationIdAndUserId(Arrays.asList(new ObjectId[] {dataset.getId()}), fileSystemConfiguration.getId(), new ObjectId(currentUser.getId()));
		
		for (FileDocument fd : files) {
			if (fd.getGroup() != group) {
				continue;
			}
			
			ProcessStateContainer<FilePublishState> psv = fd.getCurrentPublishState(virtuosoConfigurations.values());
			if (psv != null) {
				FilePublishState ps = psv.getProcessState();
				
				List<File> ff = folderService.getUploadedFiles(currentUser, dataset, fd, ps.getExecute());
				if (files != null) {
					for (File f : ff) {
						vc.executePrepareDeleteStatement(folderService.getParent(f), folderService.getName(f), fileSystemConfiguration.getDataFolder());
					}
				}
			}
		}
    	
    	if (content && dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET && dataset.getAsProperty() != null) {
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
    			nestedDelete(vc, dataset);
    		}
	    	//TODO: unpublish annotations
	    	
	    	if (content) {
    			String targetGraph = dataset.getContentTripleStoreGraph(resourceVocabulary, group);
		    	
    			logger.info("Clearing graph <" + targetGraph + ">");
	    		vc.executeClearGraphStatement(targetGraph);

	    	}
	
    	}
		return true;
    	
    }    
    
    
    public <D extends SideSpecificationDocument, M extends ExecuteState, I extends EnclosingDocument> 
    	boolean publish(TripleStoreConfiguration vc, SideExecutableContainer<D,?,M,I> ac) throws Exception {

    	Set<String> uploadedFiles = new HashSet<>();
    	
    	D doc = ac.getObject();
		M es = ac.getExecuteState();
		
		String annotationGraph = doc.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		logger.info("Publishing of " + doc.asResource(resourceVocabulary) + " started.");
		
		if (es.getExecuteShards() > 0) {
			
			if (!separateGraphs) {
				File f = folderService.getExecutionCatalogFile(ac, es);
		
				String p = folderService.getParent(f);
				String n = folderService.getName(f);
	
				logger.info("Publication prepare delete " + f.getCanonicalPath());
	
				vc.executePrepareDeleteStatement(p, n, fileSystemConfiguration.getDataFolder());
				vc.prepare(sftpUploadGateway, p, n, uploadedFiles);
		    		
	    		logger.info("Publication prepare load " + f.getCanonicalPath() + " >> " + resourceVocabulary.getAnnotationGraphResource());
		
	    		String annotationSetGraph = doc.getTOCGraph(resourceVocabulary, separateGraphs);
	   			vc.executePrepareLoadStatement(p, n, annotationSetGraph, fileSystemConfiguration.getDataFolder());
			}
			
    		logger.info("Publication shards " + Math.max(1, es.getExecuteShards()));
    		
			for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
				File ff = folderService.getExecutionTrigFile(ac, es, i);
				if (ff != null) {
					String fp = folderService.getParent(ff);
					String fn = folderService.getName(ff);
					
					vc.executePrepareDeleteStatement(fp, fn, fileSystemConfiguration.getDataFolder());
					vc.prepare(sftpUploadGateway, fp, fn, uploadedFiles);
		    		vc.executePrepareLoadStatement(fp, fn, annotationGraph, fileSystemConfiguration.getDataFolder());
				}
    		}
		}
    	
		vc.executeLoadStatement();
    	
		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
		
		if (!separateGraphs) {
			vc.executeSparqlUpdateStatement(sparqlQueryService.declareAnnotationSet(doc));
		}
	    
	    String extra = sparqlQueryService.insertReferenedByInAnnotations(ac);
	    if (extra != null) {
	    	vc.executeSparqlUpdateStatement(extra);
	    }
    	
		vc.executeCheckpointStatement();

		logger.info("Publishing of " + doc.asResource(resourceVocabulary) + " completed.");
		
		return true;
    }    

//  for virtuoso >= 7.2.7 -- ResultSetMax max deleted triples per query     
    public <D extends SideSpecificationDocument> 
    	boolean unpublish(TripleStoreConfiguration vc, SideExecutableContainer<D,?,?,?> ac) throws Exception {
    	
		D doc = ac.getObject();
	
		logger.info("Unpublishing of " + doc.asResource(resourceVocabulary) + " started.");
		
		if (separateGraphs) {
			
			String annotationGraph = doc.getTripleStoreGraph(resourceVocabulary, separateGraphs);
			vc.executeClearGraphStatement(annotationGraph);
			
		} else {
			
			unpublishIter(sparqlQueryService.countAnnotationsL3(doc), sparqlQueryService.deleteAnnotationsL3(doc), vc, "Annotations L3 for " + doc.asResource(resourceVocabulary));
			unpublishIter(sparqlQueryService.countAnnotationsL2(doc), sparqlQueryService.deleteAnnotationsL2(doc), vc, "Annotations L2 for " + doc.asResource(resourceVocabulary));
			unpublishIter(sparqlQueryService.countAnnotationsL1(doc), sparqlQueryService.deleteAnnotationsL1(doc), vc, "Annotations L1 for " + doc.asResource(resourceVocabulary));	    	
			unpublishIter(sparqlQueryService.countAnnotationSet(doc), sparqlQueryService.deleteAnnotationSet(doc), vc, "Annotations L0 for " + doc.asResource(resourceVocabulary));
			
			if (doc instanceof AnnotationValidationContainer) {
				AnnotationValidation av = (AnnotationValidation)doc;
				
				unpublishIter(sparqlQueryService.countAnnotationValidationsL2(av), sparqlQueryService.deleteAnnotationValidationsL2(av), vc, "Annotation validations B L2 for " + av.asResource(resourceVocabulary));
				unpublishIter(sparqlQueryService.countAnnotationValidationsL1(av), sparqlQueryService.deleteAnnotationValidationsL1(av), vc, "Annotation validations B L1 for " + av.asResource(resourceVocabulary));	    	
				unpublishIter(sparqlQueryService.countAnnotationValidationsL0(av), sparqlQueryService.deleteAnnotationValidationsL0(av), vc, "Annotation validations B L0 for " + av.asResource(resourceVocabulary));
			}
		}
		
		logger.info("Unpublishing of " + doc.asResource(resourceVocabulary) + " completed.");
		
		vc.executeCheckpointStatement();
	
		return true;
    }  
    
// for virtuoso < 7.2.7 -- vec memory error when too many annotations  
//    public boolean unpublish(TripleStoreConfiguration vc, AnnotatorContainer ac) throws Exception {
//    	
//		AnnotatorDocument adoc = ac.getObject();
//	
//		logger.info("Annotation unpublishing " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()));
//
//		String annotationGraph = adoc.getTripleStoreGraph(resourceVocabulary);
//		String annotationSetGraph = adoc.getAsProperty() != null ? resourceVocabulary.getAnnotationGraphResource().toString() : adoc.getTripleStoreGraph(resourceVocabulary);
//
//		int iter = 0;
//		while (true) {
//			String select;
//			if (adoc.getAsProperty() != null) { // legacy
//				select = "SELECT (count(?annId) AS ?count) WHERE { " +
//			            "  GRAPH <" + annotationSetGraph + "> { " +
//			            "    <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
//			            "  GRAPH <" + annotationGraph + "> {" +
//					    "    ?annId a <" + OAVocabulary.Annotation + "> } }";
//			} else {
//				select = "SELECT (count(?annId) AS ?count) WHERE { " +
//			            "  GRAPH <" + annotationGraph + "> { " +
//			            "    <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . " +
//					    "    ?annId a <" + OAVocabulary.Annotation + "> } }";
//			}
//			
//			logger.info("Counting annotations to unpublish " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()));
////	    			System.out.println(QueryFactory.create(select, Syntax.syntaxSPARQL_11));
//			
//			int count = TripleStoreUtils.count(vc, select);
//			
//			logger.info("Annotations to unpublish " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()) + ": " + count);
//			
//			if (count == 0) {
//				break;
//			}
//    			
//			String sparql =
//	            "DELETE { GRAPH <" + annotationGraph + "> {" +
//	            " ?annId ?p1 ?o1 ." + 
//	            " ?o1 ?p2 ?o2 . } } " +            		
//	    		"WHERE { " + 
//	            "  GRAPH <" + annotationGraph + "> {" +
//	            "    ?annId ?p1 ?o1 . " +
//	            "    OPTIONAL { ?o1 ?p2 ?o2 } } " + 
//	            "  { SELECT ?annId WHERE { " +
//	            "      GRAPH <" + annotationSetGraph + "> { " +
//	            "        <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annId . } " +
//	            "   } LIMIT 100000 OFFSET " + iter*100000 + " " + 
//	            " } } ";
//	      		
//	    	logger.info("Unpublishing " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()) + " batch #" + iter);
//	    		
//	    	vc.executeSparqlUpdateStatement(sparql);
//	    		
//	    	logger.info("Unpublishing " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()) + " batch #" + iter + " completed.");
//
//	    	iter++;
//		}
//			
//		logger.info("Unpublishing annotationset for " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()));
//		
//		String sparql =
//			"DELETE WHERE { GRAPH <" + annotationSetGraph + "> { " +
//				" <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> ?p ?q . } }";
//		
//		vc.executeSparqlUpdateStatement(sparql);
//		
//		logger.info("Unpublication of " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()) + " completed.");
//		
//		vc.executeCheckpointStatement();
//	
//		return true;
//    }    
    
    private void unpublishIter(String countQuery, String deleteQuery, TripleStoreConfiguration vc, String msg) throws Exception {
    	int iter = 0;
		
    	while (true) {
			
			int count = TripleStoreUtils.count(vc, countQuery);
			
			logger.info(msg + ": " + count);
			
			if (count == 0) {
				break;
			}
			
	    	logger.info(msg + " batch #" + iter);
	    		
	    	vc.executeSparqlUpdateStatement(deleteQuery);
	    		
	    	logger.info(msg + " batch #" + iter + " completed.");
	    	
	    	iter++;
		}
    }
       
    
//    public boolean unpublish(TripleStoreConfiguration vc, EmbedderContainer ec) throws Exception {
//
//    	EmbedderDocument edoc = ec.getObject();
//    	Dataset dataset = ec.getEnclosingObject();
//    	
//    	String targetGraph = resourceVocabulary.getDatasetEmbeddingsAsResource(dataset.getUuid()).toString();
//    	
//		logger.info("Embedder unpublishing " + edoc.asResource(resourceVocabulary));
//
//		int iter = 0;
//		while (true) {
//
//			String select = 
//   				"SELECT (count(?annId) AS ?count) WHERE { " +
//	            "  GRAPH <" + targetGraph + "> {" +
//			    "    ?annId a <" + OAVocabulary.Annotation + "> ; <" + ASVocabulary.generator + "> <" + edoc.asResource(resourceVocabulary) + "> } }"; 
//			
//			logger.info("Counting annotations to unpublish " + edoc.asResource(resourceVocabulary));
//			
//			int count = TripleStoreUtils.count(vc, select);
//			
//			logger.info("Annotations to unpublish " + edoc.asResource(resourceVocabulary) + ": " + count);
//			
//			if (count == 0) {
//				break;
//			}
//			
//	  		String sparql =
//	            "DELETE { GRAPH <" + targetGraph + "> {" +
//	            " ?annId ?p1 ?o1 ." + 
//	            " ?o1 ?p2 ?o2 . } } " +            		
//	    		"WHERE { " + 
//	            "  GRAPH <" + targetGraph + "> {" +
//	            "    ?annId ?p1 ?o1 . " +
//	            "    OPTIONAL { ?o1 ?p2 ?o2 } } " + 
//	            "  { SELECT ?annId WHERE { " +
//	            "      GRAPH <" + targetGraph + "> { " +
//	            "         ?annId a <" + OAVocabulary.Annotation + "> ; <" + ASVocabulary.generator + "> <" + edoc.asResource(resourceVocabulary) + "> } " +
//	            "   } LIMIT 100 OFFSET " + iter*100 + " " +
//	            " } } ";
//	    		
//    		logger.info("Unpublishing " + edoc.asResource(resourceVocabulary) + " batch #" + iter);
//    		
//    		vc.executeSparqlUpdateStatement(sparql);
//    		
//    		logger.info("Unpublishing " + edoc.asResource(resourceVocabulary) + " batch #" + iter + " completed.");
//
//    		iter++;
//		}
//		
//		logger.info("Unpublication of " + edoc.asResource(resourceVocabulary) + " completed.");
//		
//		vc.executeCheckpointStatement();
//
//		return true;
//	}
    
//    public boolean publish(TripleStoreConfiguration vc, AnnotationValidationContainer<?,?,Dataset> avc) throws Exception {
//
//    	logger.info("Preparing annotation validation publication");
//    	
//    	AnnotationValidation av = avc.getAnnotationValidation();
//		Dataset dataset = avc.getEnclosingObject();
//    	MappingExecuteState es = av.getExecuteState(fileSystemConfiguration.getId());
//    		
//    	String targetGraph = av.getTripleStoreGraph(resourceVocabulary);
//    	
//    	Set<String> uploadedFiles = new HashSet<>();
//
//    	if (es.getExecuteShards() > 0) {
//			for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
//				File ff = folderService.getExecutionTrigFile(avc, es, i);
//				if (ff != null) {
//					String fp = folderService.getParent(ff);
//					String fn = folderService.getName(ff);
//		
//					vc.executePrepareDeleteStatement(fp, fn, fileSystemConfiguration.getDataFolder());
//					vc.prepare(sftpUploadGateway, fp, fn, uploadedFiles);
//		    		vc.executePrepareLoadStatement(fp, fn, targetGraph, fileSystemConfiguration.getDataFolder());
//				}
//			}
//    	}    	
//    	
//		logger.info("Executing annotation validation publication");
//		
//		vc.executeLoadStatement();
//    	
//		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
//		
//		if (av.getAsProperty() != null) { // legacy
//		    logger.info("Updating annotation validation annotationset graph");
//		    
//			String insert = 
//	    			"INSERT { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
//	    					"  <" + resourceVocabulary.getAnnotationSetAsResource(av.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation } }" +
//	    					"WHERE { " +
//	    					"  GRAPH <" + av.getTripleStoreGraph(resourceVocabulary) + "> { " +
//	    					"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//	    					"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//	    					"    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av) + "> ." + // added annotations
//	    					"    ?annotation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av) + "> . } " +
//	    					"} ";
//	    					
//			vc.executeSparqlUpdateStatement(insert);
//			vc.executeCheckpointStatement();
//		}
//		
//    	AnnotationEditGroup aeg = aegRepository.findById(av.getAnnotationEditGroupId()).get();
//    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
//    		
//		DatasetCatalog dcg = schemaService.asCatalog(dataset.getUuid());
//		String usingClause = schemaService.buildUsingClause(dcg);
//		
//    	String sinsert = 
//    		   "INSERT { GRAPH <" + av.getTripleStoreGraph(resourceVocabulary) + "> { " + 
//    	       "   ?target <" + DCTVocabulary.isReferencedBy + "> ?property } }" +
//    	       usingClause + 
//    	       "USING NAMED <" + av.getTripleStoreGraph(resourceVocabulary) + "> " +
//    	       "WHERE { " +
//     		   "  ?source ?property ?body . " + 
//    		   "  GRAPH <" + av.getTripleStoreGraph(resourceVocabulary) + "> { " +
//    		   "    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//    		   "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
//    		   "    ?target <" + OAVocabulary.hasSource + ">  ?source . " +
//    		   "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
//    		   "    ?annotation <" + OAVocabulary.hasBody + ">  ?body . " +
//			   "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//    		   "    ?annotation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av) + "> . " + // added annotations
//			   "    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av) + "> ." +     		   
//    		   "  } " +
//    		   "}";
//    		
////  		System.out.println(sinsert);
//
//		vc.executeSparqlUpdateStatement(sinsert);
//		vc.executeCheckpointStatement();
//		
//    	logger.info("Annotation validation publication completed");
//    		
//		return true;
//
//    }        
    
//    public boolean unpublish(TripleStoreConfiguration vc, AnnotationValidationContainer<?,?,Dataset> avc) throws Exception {
//
//		logger.info("Unpublishing annotation validation");
//		
//    	AnnotationValidation av = avc.getAnnotationValidation();
//
////		unpublishIter(sparqlQueryService.countAnnotationValidations(av), sparqlQueryService.deleteAnnotationValidations(av), vc, 
////				"Annotation validations for " + resourceVocabulary.getAnnotatorAsResource(adoc.getUuid()));
//
//		// delete added paged validations
//		String sparql =
//				"DELETE { " + 
//			    "  GRAPH <" + av.getTripleStoreGraph(resourceVocabulary) + "> {" +
//			    "    ?annotation ?p1 ?o1 . " +
//			    "    ?o1 ?p2 ?p2 . } " +
//			    (av.getAsProperty() != null ? "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " : "") +
//		        "} WHERE { " + 
//		        "  GRAPH <" + av.getTripleStoreGraph(resourceVocabulary) + "> { " + 
//				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//				"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//				"    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." + // added annotations validator = generator
//				"    ?annotation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." +
//				"    ?annotation ?p1 ?o1 . " +
//				"    OPTIONAL { ?o1 ?p2 ?o2 } } " +
//				(av.getAsProperty() != null ? "  GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation } " : "") +
//		    	"} ";
//		
//		vc.executeSparqlUpdateStatement(sparql);
//
//		// delete validations in original annotations
//		sparql =
//				"DELETE { " + 
//			    "  GRAPH <" + av.getTripleStoreGraph(resourceVocabulary) + "> {" +
//		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//			    "    ?validation ?p ?o } " +
//		        "} WHERE { " + 
//		        "  GRAPH <" + av.getTripleStoreGraph(resourceVocabulary) + "> { " + 
//				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//			    "    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> . " +
//			    "    ?validation ?p ?o } " + 
//		    	"} ";
//		
//		vc.executeSparqlUpdateStatement(sparql);
//		vc.executeCheckpointStatement();
//
//    	logger.info("Annotation validation unpublication completed");
//    	
//		return true;
//
//    }  


//    public boolean publish(UserPrincipal currentUser, String virtuoso, VocabularizerDocument doc) throws Exception {
//
//    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);
//    	
//    	Set<String> uploadedFiles = new HashSet<>();
//    	
//    	String vf = folderService.getVocabularizerFolder(currentUser);
//    	
//		vc.executePrepareDeleteStatement(vf, doc.getUuid() + "_catalog.trig", fileSystemConfiguration.getDataFolder());
//		vc.executePrepareDeleteStatement(vf, doc.getUuid() + ".trig", fileSystemConfiguration.getDataFolder());
//
//		vc.prepare(sftpUploadGateway, vf, doc.getUuid() + "_catalog.trig", uploadedFiles);
//		vc.executePrepareLoadStatement(vf, doc.getUuid() + "_catalog.trig", resourceVocabulary.getContentGraphResource().toString(), fileSystemConfiguration.getDataFolder());
//		vc.prepare(sftpUploadGateway, vf, doc.getUuid() + ".trig", uploadedFiles);
//		vc.executePrepareLoadStatement(vf, doc.getUuid() + ".trig", resourceVocabulary.getDatasetAsResource(doc.getUuid()).toString(), fileSystemConfiguration.getDataFolder());
//    	
//		vc.executeLoadStatement();
//    	
//		vc.deleteFiles(sftpDeleteGateway, uploadedFiles);
//		
//		String insert = 
//		   "insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " +
//                   "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <http://www.w3.org/ns/dcat#Dataset> , <" + VOIDVocabulary.Dataset + "> . " +
//                   "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.Autogenerated + "> . " +
//		           "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.VocabularyCollection + "> . " +
////		           "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.ThesaurusCollection + "> . " + 
//		           "<" + resourceVocabulary.getDatasetAsResource(doc.getUuid()) + "> a <" + SEMAVocabulary.SKOSThesaurus + "> . } } WHERE { }";
//
//		vc.executeSparqlUpdateStatement(insert);
//		vc.executeCheckpointStatement();
//    	
//		return true;
//
//    }        
//    
//    public boolean unpublish(String virtuoso, VocabularizerDocument doc) throws Exception {
//    	
//    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);
//
////    	ivirtuoso.nestedDelete(vc, SEMAVocabulary.contentGraph.toString(), SEMAVocabulary.getDataset(doc.getUuid()).toString());
//    	nestedDelete(vc, resourceVocabulary.getContentGraphResource().toString(), resourceVocabulary.getDatasetAsResource(doc.getUuid()).toString());
//    	//TODO: unpublish annotations
//    	
////    	System.out.println("sparql clear graph <" + SEMAVocabulary.getDataset(doc.getUuid()).toString() + ">");
//		vc.executeClearGraphStatement(resourceVocabulary.getDatasetAsResource(doc.getUuid()).toString());
//		vc.executeCheckpointStatement();
//
//		return true;
//
//    }    
    
//    public void resetAccessGraph(String virtuoso) throws Exception {
//    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);
//    	
//		vc.executeClearGraphStatement(resourceVocabulary.getAccessGraphResource().toString());
//		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> a <" + SACCVocabulary.Group.toString() + "> } } WHERE { }");
//		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> a <" + SACCVocabulary.PublicGroup.toString() + "> } } WHERE { }");
//		vc.executeCheckpointStatement();
//    }
    
//    public void addUserToAccessGraph(String virtuoso, String uuid) throws Exception {
//
//    	TripleStoreConfiguration vc =  virtuosoConfigurations.getByName(virtuoso);
//
//		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getUserAsResource(uuid)+ "> a <" + SACCVocabulary.User + "> } } WHERE { }");
//		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getGroupAsResource(uuid) + "> a <" + SACCVocabulary.Group + "> } } WHERE { } ");
//		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> <" + SACCVocabulary.member + "> <" + resourceVocabulary.getUserAsResource(uuid) + "> } } WHERE { } ");
//		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getGroupAsResource(uuid) + "> <" + SACCVocabulary.member + ">  <" + resourceVocabulary.getUserAsResource(uuid) + "> } } WHERE { }");
//		vc.executeCheckpointStatement();
//    }
    
//    public void addDatasetToAccessGraph(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset, boolean isPublic) throws Exception {
//
//    	removeDatasetFromAccessGraph(vc, dataset);
//    	
//		vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getGroupAsResource(currentUser.getUuid()) + "> <" + SACCVocabulary.dataset + "> <" + dataset.asResource(resourceVocabulary) + "> } } WHERE { }");
//   		if (isPublic) {
//			vc.executeSparqlUpdateStatement("insert { graph <" + resourceVocabulary.getAccessGraphResource() + "> { <" + resourceVocabulary.getDefaultGroupResource() + "> <" + SACCVocabulary.dataset + "> <" + dataset.asResource(resourceVocabulary) + "> } } WHERE { }");
//    	}
//   		
//   		vc.executeCheckpointStatement();
//    }
    
//    public void removeDatasetFromAccessGraph(TripleStoreConfiguration vc, Dataset dataset) throws Exception {
//
//		vc.executeSparqlUpdateStatement("delete where { graph <" + resourceVocabulary.getAccessGraphResource() + "> { ?s <" + SACCVocabulary.dataset + "> <" + dataset.asResource(resourceVocabulary) + "> } } ");
//		vc.executeCheckpointStatement();
//    }
    
    private void nestedDelete(TripleStoreConfiguration vc, Dataset dataset) throws Exception {
    	String graph = dataset.getMetadataTripleStoreGraph(resourceVocabulary);
    	String uri = dataset.getContentTripleStoreGraph(resourceVocabulary);
    	
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
		if (i == 1) {
			db = new StringBuffer();
			db.append("WITH <" + graph + "> DELETE { ?s ?p1 ?o1 . } WHERE { VALUES ?s { <" + uri + "> }  ?s ?p1 ?o1 . }");
		} else {
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
		}
		
		if (db != null) {
			logger.info("Deleting dataset metadata: " + db.toString());
			
			vc.executeSparqlUpdateStatement(db.toString());
			
//			if (uri.startsWith(resourceVocabulary.getDatasetAsResource("").toString())) {
//				String uuid = resourceVocabulary.getUuidFromResourceUri(uri);
//				
//				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/sparql> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/sparql> ?p1 ?o1 }");
//				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDataServiceAsResource(uuid) + "/sparql> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDataServiceAsResource(uuid) + "/sparql> ?p1 ?o1 }");
//				
//				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/ttl> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/ttl> ?p1 ?o1 }");
//				vc.executeSparqlUpdateStatement("WITH <" + graph + "> DELETE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/nt> ?p1 ?o1 } WHERE { <" + resourceVocabulary.getDistributionAsResource(uuid) + "/nt> ?p1 ?o1 }");
//			}
			
			vc.executeCheckpointStatement();
		}

    }
    
    
    public boolean publishDatasetMetadata(DatasetContainer dc) throws Exception {
//    	System.out.println("TS PUBLISH 1");
    	
    	Dataset dataset = dc.getObject();
    	UserPrincipal currentUser = dc.getCurrentUser();
    	TripleStoreConfiguration vc = dc.getDatasetTripleStoreVirtuosoConfiguration();
    	
    	boolean header = true;
    	
	    boolean isSKOS = false;
	   	
	    String endpoint = null;
	   	String remoteFromClause = "";
	   	
	   	if (dataset.isRemote()) {
	   		endpoint = dataset.getRemoteTripleStore().getSparqlEndpoint();
	   		remoteFromClause = RemoteTripleStore.buildFromClause(dataset.getRemoteTripleStore());
	   	}
	   	
	   	String datasetUri = dataset.asResource(resourceVocabulary).toString();

		DatasetCatalog dcg = schemaService.asCatalog(dataset.getUuid());
		String fromClause = schemaService.buildFromClause(dcg);

		String effectiveFromClause = (endpoint == null ? fromClause : remoteFromClause);

    	HttpClient client = serviceUtils.getSSHClient(); 

//    	System.out.println("TS PUBLISH 2");
    	
		if (dataset.getScope() == DatasetScope.ANNOTATION && dataset.getDatasetType() == DatasetType.DATASET && dataset.getAsProperty() != null) { // legacy			

			String insert = 
			   "INSERT { GRAPH <" + resourceVocabulary.getAnnotationGraphResource() + "> { " +
                    "<" + resourceVocabulary.getAnnotationSetAsResource(dataset.getUuid()) + "> a <" + DCATVocabulary.Dataset + "> , <" + VOIDVocabulary.Dataset + "> . } } WHERE { }";
	   		vc.executeSparqlUpdateStatement(insert);

		} else if (dataset.getScope() == DatasetScope.ALIGNMENT && dataset.getDatasetType() == DatasetType.DATASET) {
			
			String insert = 
				"INSERT { GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " +
			         "<" + datasetUri + "> a <" + DCATVocabulary.Dataset + "> , <" + VOIDVocabulary.Dataset + "> . ";
					
 		    for (String s : dataset.getTypeUris()) {
			   insert  += "<" + datasetUri + "> a <" + s + "> . ";
		    }
 		   
 		    boolean bidirectional =  false;
 		    ResourceOption bro =  dataset.getOptionsByType(ResourceOptionType.BIDIRECTIONAL);
 		    if (bro != null && bro.getValue().toString().equalsIgnoreCase("true")) {
 		    	bidirectional = true;
 		    }
 		    
 		    ObjectId sourceId = (ObjectId)dataset.getLinkByType(ResourceOptionType.SOURCE).getValue();
 		    ObjectId targetId = (ObjectId)dataset.getLinkByType(ResourceOptionType.TARGET).getValue();
 		    
 		    Resource sourceUri = resourceVocabulary.getDatasetContentAsResource(datasetService.getContainer(null, sourceId).getObject());
 		    Resource targetUri = resourceVocabulary.getDatasetContentAsResource(datasetService.getContainer(null, targetId).getObject());

 		    insert  += "<" + datasetUri + "> <" + SEMAVocabulary.isAlignmentOf.toString() + "> [ <" + SEMAVocabulary.source + "> <" + sourceUri + "> ; <" + SEMAVocabulary.target + "> <" + targetUri + "> ] . ";
 		    
 		    insert += " } } WHERE { }";
 		    
	   		vc.executeSparqlUpdateStatement(insert);
 		   	
 		   	if (bidirectional) {
 		    	executeSaturate(currentUser, vc, dataset);

 				String sinsert = "insert { graph <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { ";
 		    	sinsert  += "<" + datasetUri  + "> <" + SEMAVocabulary.isAlignmentOf + "> [ <" + SEMAVocabulary.source + "> <" + targetUri + "> ; <" + SEMAVocabulary.target + "> <" + sourceUri + "> ] . ";
 		    	sinsert += " } } WHERE { }";
 		    	
 		   		vc.executeSparqlUpdateStatement(sinsert);
 		   	}
 		    
		} else if (header) {

//	 		System.out.println("TS PUBLISH 3");
	 		   
			String insert = 
			   "INSERT { GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " +
	               "<" + datasetUri + "> a <" + DCATVocabulary.Dataset + "> , <" + VOIDVocabulary.Dataset + "> . ";
			
 		    for (String s : dataset.getTypeUris()) {
			   insert  += "<" + datasetUri + "> a <" + s + "> . ";
		    }
 		    
 		    String label = "ASK FROM <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> " +  
 		            " { <" + datasetUri + "> <" + DCTVocabulary.title  + "> ?label }";
	    	
 		    try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(label, Syntax.syntaxARQ))) {
	   		
 		    	if (!qe.execAsk()) {
 				   insert += "<" + datasetUri + "> <" + DCTVocabulary.title + "> \"" + ResourceFactory.createPlainLiteral(dataset.getName()) + "\" . ";
 		    	}
	    	}

//  		   System.out.println("TS PUBLISH 4");
 		    
 		    if (dataset.getLinks() != null) {
	 		    for (ResourceOption ro : dataset.getLinks()) {
	 		    	insert  += "<" + datasetUri + "> <" + ResourceOptionType.getProperty(ro.getType()) + "> <" + ro.getValue() + "> . ";
	 		    }
 		    }
 		    
 		    if (endpoint != null) {
 		    	insert += "<" + datasetUri + "> <" + VOIDVocabulary.sparqlEndpoint + "> <" + endpoint + "> . ";
 		    }
	    	
//  		   System.out.println("TS PUBLISH 5");
  		   
//		    if (dataset.getTypeUri().contains(SEMAVocabulary.DataCatalog.toString()) && dataset.getDatasets() != null) {
 		    if (dataset.getDatasetType() == DatasetType.CATALOG && dataset.getDatasets() != null) {
		    	for (ObjectId memberId : dataset.getDatasets()) {
		    		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(memberId, new ObjectId(currentUser.getId()));
		    		
		    		if (doc.isPresent()) {
			    		insert  +=	"<" + datasetUri + "> <" + DCTVocabulary.hasPart + "> <" + resourceVocabulary.getDatasetContentAsResource(doc.get())  + "> . ";
		    		}
		    	}	
		    }
		    
//  		   System.out.println("TS PUBLISH 6");
// 		    	System.out.println("ENDPOINT" + endpoint);
	    	
 		    // check if thesaurus is SKOS
		    
		    if (dataset.getScope() == DatasetScope.VOCABULARY) {
 		    	String sparql =
 		            "ASK " + effectiveFromClause +  
 		            " { ?p a <" + SKOSVocabulary.Concept + "> }";
 		    	
//	 		    	System.out.println(sparql);
 		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ), client)) {
	
	 		    	if (qe.execAsk()) {
	 				   insert += "<" + datasetUri + "> a <" + SEMAVocabulary.SKOSThesaurus + "> . ";
	 				   isSKOS = true;
	 				}
 		    	}
 		    	
 		    	sparql =
 		    			"ASK " + effectiveFromClause +  
	 		            " { ?p a <http://www.w3.org/2002/07/owl#Ontology> }";
	 		    	
//	 		    	System.out.println(sparql);
	 		    try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ), client)) {
		
		 		    if (qe.execAsk()) {
		 			   insert += "<" + datasetUri + "> a <" + SEMAVocabulary.OWLOntology + "> . ";
		 			}
	 		    }			 		    	
 		    }	
 		    
            insert += " } } WHERE { }";
            
//  		   System.out.println("TS PUBLISH 7");
  		   
   	   		vc.executeSparqlUpdateStatement(insert);
		}


		
		// compute schema 
//		if ((dataset.getDatasetType() == DatasetType.DATASET && dataset.getScope() == DatasetScope.COLLECTION) ||
//			(dataset.getDatasetType() == DatasetType.DATASET && dataset.getScope() == DatasetScope.VOCABULARY) ||
//			(dataset.getDatasetType() == DatasetType.CATALOG)) {
//			
//			String insert = 
//					"insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " + 
////   						"<" + datasetUri + ">  <" + DCTVocabulary.identifier + "> \"" + id + "\" . " + 
//			        "<" + datasetUri + ">  <" + VOIDVocabulary.sparqlEndpoint + "> <" + resourceVocabulary.getContentSparqlEnpoint(dataset.getUuid()) + "> } } WHERE { }" ;
//    				
//			vc.executeSparqlUpdateStatement(insert);
//
//			String id = dataset.getIdentifier();
//  			if (id != null) {
//
//   				Resource distributionResource = resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql");
//   				Resource dataServiceResource = resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql");
//   				
//   				// DCAT	    				
//   				insert = 
//   						"insert { graph <" + resourceVocabulary.getContentGraphResource() + "> { " + 
//   						" <" + datasetUri + ">  <" + DCATVocabulary.distribution + "> <" + resourceVocabulary.getDistributionAsResource(dataset.getUuid() + "/sparql") + "> . " +
//                        " <" + distributionResource + "> a <" + DCATVocabulary.Distribution + "> . " + 	    								
//                        " <" + distributionResource + "> <" + DCATVocabulary.accessService + "> <" + resourceVocabulary.getDataServiceAsResource(dataset.getUuid() + "/sparql") + "> ." +
//                        " <" + distributionResource + "> <" + DCTVocabulary.conformsTo + "> <https://www.w3.org/TR/sparql11-protocol/> . " +
//                        " <" + distributionResource + "> <" + DCATVocabulary.accessURL + "> <" + resourceVocabulary.getContentSparqlEnpoint(id) + "> ." +
//                        " <" + distributionResource + "> <" + DCTVocabulary.title + "> \"SPARQL endpoint\" ." +
//                        " <" + dataServiceResource + "> a <" + DCATVocabulary.DataService + "> . " +
//                        " <" + dataServiceResource + "> <" + DCTVocabulary.conformsTo + "> <https://www.w3.org/TR/sparql11-protocol/> . " +
//                        " <" + dataServiceResource + "> <" + DCATVocabulary.endpointURL + "> <" + resourceVocabulary.getContentSparqlEnpoint(id) + "> ." +
//                        " <" + dataServiceResource + "> <" + DCATVocabulary.servesDataset + "> <" + datasetUri + "> ." +
//                        " } } WHERE { } ";
//
//   				vc.executeSparqlUpdateStatement(insert);
//	    	}	
//		}
		
	    // add languages for Vocabularies
//	    if (dataset.getTypeUri().contains(SEMAVocabulary.VocabularyCollection.toString())) {
		if (dataset.getScope() == DatasetScope.VOCABULARY) {
	    	if (endpoint == null) {
	    		// avoid join (join does not always work) : first get label property
	    		// if this fails also, only solution is to iterate over records
	    		String languages = legacyUris ?
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> {" +
			    		    "<" + datasetUri + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?n . " +
			    		    "?n <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " + 
			    		    "?n <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProperty } } " : 
			    		"SELECT DISTINCT ?labelProperty WHERE { " +
			    		  "GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> {" +
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
		    	if (label.length() == 0) { // try default
		    		label = "<" + RDFSVocabulary.label + ">|<" +  SKOSVocabulary.prefLabel + ">|<" + SKOSVocabulary.altLabel + ">";
		    	}
		    	
		    	if (label.length() != 0) {
			    	languages = "SELECT DISTINCT ?lang " + fromClause + " WHERE { ?p " + label + " ?r . BIND(LANG(?r) AS ?lang) } ";
			    	
//			    	System.out.println(languages);
			    	
			    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(languages, Syntax.syntaxARQ))) {
			    		ResultSet rs = qe.execSelect();
			    		while (rs.hasNext()) {
			    			String lang = rs.next().get("lang").toString();
			    			if (lang.length() > 0) {
			    				String insert = "insert { graph <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " +
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
			    		  "GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> {" +
			    		    "<" + datasetUri + "> <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?n . " +
			    		    "?n <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . " + 
			    		    "?n <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProperty } } " : 
	    		"SELECT DISTINCT ?labelProperty WHERE { " +
	    		  "GRAPH <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> {" +
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
			    	
			    	if (label.length() == 0) { // try default
			    		label = "<" + RDFSVocabulary.label + ">|<" +  SKOSVocabulary.prefLabel + ">|<" + SKOSVocabulary.altLabel + ">";
			    	}
			    	
			    	if (label.length() != 0) {
				    	languages = 
					    		"SELECT DISTINCT ?lang " + effectiveFromClause + " WHERE { ?p " + label + " ?r . BIND(LANG(?r) AS ?lang) } ";
					    	
	//				    	System.out.println(languages);
	
				    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(languages, Syntax.syntaxARQ), client)) {
				    		ResultSet rs = qe.execSelect();
				    		while (rs.hasNext()) {
				    			String lang = rs.next().get("lang").toString();
				    			if (lang.length() > 0) {
									String insert = "insert { graph <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " +
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
		    		"SELECT DISTINCT ?scheme " + effectiveFromClause + " WHERE { ?p <" + SKOSVocabulary.inScheme + "> ?scheme }";
		    	
//		    	System.out.println(schemes);
	   		
		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint == null ? vc.getSparqlEndpoint() : endpoint, QueryFactory.create(schemes, Syntax.syntaxARQ), client)) {
		    		ResultSet rs = qe.execSelect();
		    		while (rs.hasNext()) {
		    			String scheme = rs.next().get("scheme").toString();
		    			
						String insert = "insert { graph <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " +
                                        "<" + datasetUri.toString() + "> <" + SEMAVocabulary.scheme + "> <" + scheme + "> . } } WHERE { }";
					    		
						vc.executeSparqlUpdateStatement(insert);
					}
		    	} catch (Exception ex) {
		    		ex.printStackTrace();
		    	}
		    }		    
	    }
	    
	    // update prefixes maps
	    
//	    if (dataset.getScope() == DatasetScope.VOCABULARY && dataset.getDatasetType() == DatasetType.DATASET) {	    	
//	    	((VocabulariesBean)context.getBean("vocabularies")).setMap(namesService.createVocabulariesMap(virtuosoConfigurations.values(), legacyUris));
//	    }
	    
//	    vm.removeMap(datasetUri);
//	    namesService.createDatasetsMap(vm, vc, datasetUri, legacyUris);
	    
//		System.out.println("TS PUBLISH 8");
		
	    try {
//			if (dataset.getTypeUri().contains(SEMAVocabulary.VocabularyCollection.toString()) || 
//					dataset.getTypeUri().contains(SEMAVocabulary.DataCollection.toString()) || dataset.getTypeUri().contains(SEMAVocabulary.DataCatalog.toString())) {
			if ((dataset.getDatasetType() == DatasetType.DATASET && dataset.getScope() == DatasetScope.COLLECTION) ||
					(dataset.getDatasetType() == DatasetType.DATASET && dataset.getScope() == DatasetScope.VOCABULARY) ||
					(dataset.getDatasetType() == DatasetType.CATALOG)) {
				
				logger.info("Computing schema for " + datasetUri);
				
				Model schema = schemaService.buildSchema(dataset, true);
				schema.clearNsPrefixMap();
				
				Writer sw = new StringWriter();
	
				RDFDataMgr.write(sw, schema, RDFFormat.TTL) ;
				
				String insert = 
						"insert { graph <" + dataset.getMetadataTripleStoreGraph(resourceVocabulary) + "> { " + sw.toString() + " } } WHERE { }" ;
				
				vc.executeSparqlUpdateStatement(insert);
			}
	    } catch (Exception ex) {
	    	ex.printStackTrace();
	    	return false;
	    }

	    return true;
    }


	private boolean executeSaturate(UserPrincipal currentUser, TripleStoreConfiguration vc, Dataset dataset) throws Exception {

		try (FileSystemRDFOutputHandler outhandler = folderService.createDatasetSaturateOutputHandler(currentUser, dataset, shardSize)) {

			Map<String, Object> params = new HashMap<>();

			params.put("iigraph", resourceVocabulary.getDatasetContentAsResource(dataset).toString());
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
		    				
					String targetGraph = resourceVocabulary.getDatasetContentAsResource(dataset).toString();
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

}
