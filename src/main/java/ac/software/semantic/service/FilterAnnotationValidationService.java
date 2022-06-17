package ac.software.semantic.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import ac.software.semantic.controller.ExecuteMonitor;
import ac.software.semantic.controller.SSEController;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.model.AnnotationEditFilter;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.ExecuteState;
import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.FilterValidationType;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FilterAnnotationValidationRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.monitor.FileSystemOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SOAVocabulary;

@Service
public class FilterAnnotationValidationService {

	Logger logger = LoggerFactory.getLogger(FilterAnnotationValidationService.class);

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;
			
	@Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    @Value("${annotation.manual.folder}")
    private String annotationsFolder;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;
	
	@Autowired
	private Environment env;

	@Autowired
	private DatasetRepository datasetRepository;
	
	@Autowired
	AnnotatorDocumentRepository annotatorDocumentRepository;

    @Autowired
	AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	FilterAnnotationValidationRepository favRepository;

	@Autowired
	AnnotationEditRepository annotationEditRepository;

	@Autowired
	VirtuosoJDBC virtuosoJDBC;
	
	@Autowired
	ResourceLoader resourceLoader;
	

	public FilterAnnotationValidation create(UserPrincipal currentUser, String aegId, String name, List<AnnotationEditFilter> filters) {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));
		if (!aegOpt.isPresent()) {
			return null;
		}
		
		AnnotationEditGroup aeg = aegOpt.get();

		List<String> annotatorUuids = annotatorDocumentRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList());
		
		FilterAnnotationValidation fav = new FilterAnnotationValidation();
		fav.setUserId(new ObjectId(currentUser.getId()));
		fav.setUuid(UUID.randomUUID().toString());
		fav.setAnnotationEditGroupId(aeg.getId());
		fav.setDatasetUuid(aeg.getDatasetUuid());
		fav.setOnProperty(aeg.getOnProperty());
		fav.setAsProperty(aeg.getAsProperty());
		fav.setAnnotatorDocumentUuid(annotatorUuids);
		
		fav.setName(name);
		fav.setFilters(filters);
			
		fav = favRepository.save(fav);
	
		return fav;
	}
	
	public FilterAnnotationValidation update(UserPrincipal currentUser, String id, String name, List<AnnotationEditFilter> filters) {

		Optional<FilterAnnotationValidation> favOpt = favRepository.findById(new ObjectId(id));
		if (!favOpt.isPresent()) {
			return null;
		}
		
		FilterAnnotationValidation fav = favOpt.get();

		fav.setName(name);
		fav.setFilters(filters);
			
		favRepository.save(fav);
	
		return fav;
	}
	

//	public boolean execute(UserPrincipal currentUser, String id, ApplicationEventPublisher applicationEventPublisher) throws Exception {
//
//		Optional<FilterAnnotationValidation> odoc = favRepository.findById(id);
//	    if (!odoc.isPresent()) {
//	    	logger.info("Filter Annotation Validation " + id + " not found");
//	    	return false;
//	    }
//    	
//	    FilterAnnotationValidation fav = odoc.get();
//	    
//	    Date executeStart = new Date(System.currentTimeMillis());
//	    
//    	ExecuteState es = fav.getExecuteState(fileSystemConfiguration.getId());
//    	
//    	String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + fav.getDatasetUuid() + "/";
//    	File datasetFolderFile = new File(datasetFolder);
//    	
//		if (!datasetFolderFile.exists()) {
//			datasetFolderFile.mkdir();
//		}
//
//		// Clearing old files
////		if (es.getExecuteState() == MappingState.EXECUTED) {
//			try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(datasetFolderFile.toPath(), fav.getUuid() + "_*")) {
//		        for (final Path path : directoryStream) {
//		            Files.delete(path);
//		        }
//		    } catch (final Exception e) { 
//		    	e.printStackTrace();
//		    }
////		}
//		
//		es.setExecuteState(MappingState.EXECUTING);
//		es.setExecuteStartedAt(executeStart);
//		es.setExecuteShards(0);
//		es.setCount(0);
//		
//		favRepository.save(fav);
//		
//		logger.info("Filter Annotation Validation " + id + " starting");
//		
//		try (FileSystemOutputHandler deleteHandler = new FileSystemOutputHandler(datasetFolder, fav.getUuid() + "_delete", shardSize);
////				Writer delete = new OutputStreamWriter(new FileOutputStream(new File(datasetFolder + fav.getUuid() + "_delete.trig"), false), StandardCharsets.UTF_8);
//				Writer deleteCatalog = new OutputStreamWriter(new FileOutputStream(new File(datasetFolder + fav.getUuid() + "_delete_catalog.trig"), false), StandardCharsets.UTF_8)				
//				) {
//			
//			Executor exec = new Executor(deleteHandler, safeExecute);
//			exec.keepSubjects(true);
//			
//			try (ExecuteMonitor em = new ExecuteMonitor("filter-validation", id, null, applicationEventPublisher)) {
//				exec.setMonitor(em);
//				
//				String d2rml = env.getProperty("validator.filter-delete.d2rml"); 
//				InputStream inputStream = resourceLoader.getResource("classpath:"+ d2rml).getInputStream();
//				D2RMLModel rmlMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
//
////				Dataset ds2 = DatasetFactory.create();
////				Model deleteModel2 = ds2.getDefaultModel();
//		
//				String onPropertyString = AnnotationEditGroup.onPropertyListAsString(fav.getOnProperty());
//				String annfilter = AnnotationEditGroup.annotatorFilter("annotation", fav.getAnnotatorDocumentUuid());
//
//				SSEController.send("filter-validation", applicationEventPublisher, this, new ExecuteNotificationObject(id, null,
//						ExecutionInfo.createStructure(rmlMapping), executeStart));
//
//				for (AnnotationEditFilter aef : fav.getFilters()) {
//
//					if (aef.getAction() == FilterValidationType.DELETE) {
//	
//						String expr = aef.getSelectExpression();
//		    	
//				    	String sparql = 
//				    			"SELECT * " +
//		     			        "WHERE { " + 
//		    			        "  GRAPH <" + fav.getAsProperty() + "> { " + 
//							    "    ?annotation a ?type . " + 
//		    			        "    VALUES ?type { <" + SEMAVocabulary.SpatialAnnotation + "> <" + SEMAVocabulary.TemporalAnnotation + "> <" + SEMAVocabulary.TermAnnotation + "> } . " + 
//		    			        "    ?annotation <" + OAVocabulary.hasBody + "> ?body . " + 
//		    			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
//		    		            "    OPTIONAL { ?annotation <" + DCVocabulary.created + "> ?created } . " +
//		    		            "    OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?score } . " +		    		            
//							    annfilter +
//		    			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
//		    			        "    ?target <" + SOAVocabulary.onValue + "> ?value . " +
//		    			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
//		    			        expr + 
//		    			        "    OPTIONAL { ?target <" + SOAVocabulary.start + "> ?start } . " +		    			        
//		    			        "    OPTIONAL { ?target <" + SOAVocabulary.end + "> ?end } . " +
//		    		            (annfilter.length() == 0 ? "    OPTIONAL { <" + ASVocabulary.generator + "> ?generator } . " : "") +
//		    		            "  } . " +	    			        
//		    			        "  GRAPH <" + SEMAVocabulary.getDataset(fav.getDatasetUuid()).toString() + "> { " +
//		    			        "    ?source " + onPropertyString + " ?value } " +
//		                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
//		                        "     ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " +		    			        
//		    			        "}";		
//				    	
//						Map<String, Object> params = new HashMap<>();
//						params.put("iirdfsource", virtuosoConfiguration.getSparqlEndpoint());
//						params.put("iisparql", sparql);
//						params.put("iiproperty", onPropertyString);
//						
//						exec.partialExecute(rmlMapping, params);
//		    		}
//					
//				}
//				exec.completeExecution();
//		
//				Set<Resource> subjects = exec.getSubjects();
////
//				String uuid = UUID.randomUUID().toString();
//				
//				deleteCatalog.write("<" + SEMAVocabulary.getAnnotationSet(uuid).toString() + ">\n");
//				deleteCatalog.write("        <http://purl.org/dc/terms/hasPart>\n");
//				deleteCatalog.write("                ");
//				int c = 0;
//				for (Resource r : subjects) {
//					if (c++ > 0) {
//						deleteCatalog.write(" , ");
//					}
//					deleteCatalog.write("<" + r.getURI() + ">");
//				}
//				deleteCatalog.write(" .");
//				
//
//				Date executeFinish = new Date(System.currentTimeMillis());
//					
//				es.setExecuteCompletedAt(executeFinish);
//				es.setExecuteState(MappingState.EXECUTED);
//				es.setExecuteShards(deleteHandler.getShards());
////				es.setCount(subjects.size());
//					
//				favRepository.save(fav);
//		
//				SSEController.send("filter-annotation", applicationEventPublisher, this, new NotificationObject("execute",
////						MappingState.EXECUTED.toString(), favId, null, executeStart, executeFinish, subjects.size()));
//						MappingState.EXECUTED.toString(), id, null, executeStart, executeFinish, 0));
//
//				logger.info("Filter validation annotation edits executed -- id: " + id + ", shards: " + 0);
//
////				try {
////					zipExecution(currentUser, adoc, outhandler.getShards());
////				} catch (Exception ex) {
////					ex.printStackTrace();
////					
////					logger.info("Zipping annotator execution failed -- id: " + id);
////				}
//				
//				return true;
//				
//			} catch (Exception ex) {
//				ex.printStackTrace();
//				
//				logger.info("Filter validation annotation edits failed -- id: " + id);
//				
////				exec.getMonitor().currentConfigurationFailed();
//
//				throw ex;
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//
//			es.setExecuteState(MappingState.EXECUTION_FAILED);
//
//			SSEController.send("filter-validation", applicationEventPublisher, this,
//					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), id, null, null, null));
//
//			favRepository.save(fav);
//
//			return false;
//		}
//	}

	public boolean executeNoDelete(UserPrincipal currentUser, String id, ApplicationEventPublisher applicationEventPublisher) throws Exception {

		Optional<FilterAnnotationValidation> odoc = favRepository.findById(id);
	    if (!odoc.isPresent()) {
	    	logger.info("Filter Annotation Validation " + id + " not found");
	    	return false;
	    }
    	
	    FilterAnnotationValidation fav = odoc.get();

	    Dataset ds = datasetRepository.findByUuid(fav.getDatasetUuid()).get();
	    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
	    
	    Date executeStart = new Date(System.currentTimeMillis());
	    
    	ExecuteState es = fav.getExecuteState(fileSystemConfiguration.getId());
    	
    	String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + fav.getDatasetUuid() + "/";
    	File datasetFolderFile = new File(datasetFolder);
    	
		if (!datasetFolderFile.exists()) {
			datasetFolderFile.mkdir();
		}

		// Clearing old files
//		if (es.getExecuteState() == MappingState.EXECUTED) {
			try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(datasetFolderFile.toPath(), fav.getUuid() + "_*")) {
		        for (final Path path : directoryStream) {
		            Files.delete(path);
		        }
		    } catch (final Exception e) { 
		    	e.printStackTrace();
		    }
//		}
		
		es.setExecuteState(MappingState.EXECUTING);
		es.setExecuteStartedAt(executeStart);
		es.setExecuteShards(0);
		es.setCount(0);
		
		favRepository.save(fav);
		
		logger.info("Filter Annotation Validation " + id + " starting");
		
		try (FileSystemOutputHandler deleteHandler = new FileSystemOutputHandler(datasetFolder, fav.getUuid(), shardSize)) {
			
			Executor exec = new Executor(deleteHandler, safeExecute);
			exec.keepSubjects(true);
			
			try (ExecuteMonitor em = new ExecuteMonitor("filter-validation", id, null, applicationEventPublisher)) {
				exec.setMonitor(em);
				
				String deleteD2rml = env.getProperty("validator.mark-delete.d2rml");
				D2RMLModel deleteMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ deleteD2rml).getInputStream()) {
					deleteMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
				}

				String replaceD2rml = env.getProperty("validator.filter-mark-replace.d2rml");
				D2RMLModel replaceMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ replaceD2rml).getInputStream()) {
					replaceMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
				}

				String onPropertyString = AnnotationEditGroup.onPropertyListAsString(fav.getOnProperty());
				String annfilter = AnnotationEditGroup.annotatorFilter("annotation", fav.getAnnotatorDocumentUuid());

				SSEController.send("filter-annotation-validation", applicationEventPublisher, this, new ExecuteNotificationObject(id, null,
						ExecutionInfo.createStructure(deleteMapping), executeStart));

				for (AnnotationEditFilter aef : fav.getFilters()) { // both delete and replace
					String expr = aef.getSelectExpression();
	    	
			    	String sparql = 
			    			"SELECT ?annotation " +
	     			        "WHERE { " + 
	    			        "  GRAPH <" + fav.getAsProperty() + "> { " + 
	    			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
						    annfilter +
	    			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
	    			        "    ?target <" + SOAVocabulary.onValue + "> ?value . " +
	    			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
	    			        expr + 
	    		            "  } . " +	    			        
	    			        "  GRAPH <" + SEMAVocabulary.getDataset(fav.getDatasetUuid()).toString() + "> { " +
	    			        "    ?source " + onPropertyString + " ?value } " +
	                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
	                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " +		    			        
	    			        "}";		
			    	
					Map<String, Object> params = new HashMap<>();
					params.put("iirdfsource", vc.getSparqlEndpoint());
					params.put("iisparql", sparql);
					params.put("validator", SEMAVocabulary.getAnnotationValidator(fav.getUuid()));
					
					exec.partialExecute(deleteMapping, params);
				}	
				
				for (AnnotationEditFilter aef : fav.getReplaceFilters()) { 
					if (aef.getAction() == FilterValidationType.REPLACE) {
						String expr = aef.getSelectExpression();
						
				    	String sparql = 
		    			"SELECT * " +
     			        "WHERE { " + 
    			        "  GRAPH <" + fav.getAsProperty() + "> { " + 
					    "    ?annotation a ?type . " + 
    			        "    VALUES ?type { <" + SEMAVocabulary.SpatialAnnotation + "> <" + SEMAVocabulary.TemporalAnnotation + "> <" + SEMAVocabulary.TermAnnotation + "> } . " + 
//    			        "    ?annotation <" + OAVocabulary.hasBody + "> ?body . " + 
    			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
    		            "    OPTIONAL { ?annotation <" + DCTVocabulary.created + "> ?created } . " +
    		            "    OPTIONAL { ?annotation <" + SOAVocabulary.score + "> ?score } . " +		    		            
					    annfilter +
    			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
    			        "    ?target <" + SOAVocabulary.onValue + "> ?value . " +
    			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
    			        expr + 
    			        "    OPTIONAL { ?target <" + SOAVocabulary.start + "> ?start } . " +		    			        
    			        "    OPTIONAL { ?target <" + SOAVocabulary.end + "> ?end } . " +
    		            (annfilter.length() == 0 ? "    OPTIONAL { <" + ASVocabulary.generator + "> ?generator } . " : "") +
    		            "  } . " +	    			        
    			        "  GRAPH <" + SEMAVocabulary.getDataset(fav.getDatasetUuid()).toString() + "> { " +
    			        "    ?source " + onPropertyString + " ?value } " +
                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " +		    			        
    			        "}";		
		    	
				    	Map<String, Object> params = new HashMap<>();
				    	params.put("iirdfsource", vc.getSparqlEndpoint());
				    	params.put("iisparql", sparql);
				    	params.put("iiproperty", onPropertyString);
						params.put("iiannotator", SEMAVocabulary.getAnnotationValidator(fav.getUuid()));
				    	params.put("validator", SEMAVocabulary.getAnnotationValidator(fav.getUuid()));
				    	params.put("newValue", aef.getNewValue());
				    	
				    	exec.partialExecute(replaceMapping, params);
					}
				}
				
				exec.completeExecution();
		
				Date executeFinish = new Date(System.currentTimeMillis());
					
				es.setExecuteCompletedAt(executeFinish);
				es.setExecuteState(MappingState.EXECUTED);
				es.setExecuteShards(deleteHandler.getShards());
				es.setCount(deleteHandler.getTotalItems());
					
				favRepository.save(fav);
		
				SSEController.send("filter-annotation-validation", applicationEventPublisher, this, new NotificationObject("execute",
						MappingState.EXECUTED.toString(), id, null, executeStart, executeFinish, deleteHandler.getTotalItems()));

				logger.info("Filter validation executed -- id: " + id + ", shards: " + 0);

//				try {
//					zipExecution(currentUser, adoc, outhandler.getShards());
//				} catch (Exception ex) {
//					ex.printStackTrace();
//					
//					logger.info("Zipping annotator execution failed -- id: " + id);
//				}
				
				return true;
				
			} catch (Exception ex) {
				ex.printStackTrace();
				
				logger.info("Filter validation failed -- id: " + id);
				
//				exec.getMonitor().currentConfigurationFailed();

				throw ex;
			}
		} catch (Exception ex) {
			ex.printStackTrace();

			es.setExecuteState(MappingState.EXECUTION_FAILED);

			SSEController.send("filter-validation", applicationEventPublisher, this,
					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), id, null, null, null));

			favRepository.save(fav);

			return false;
		}
	}

	public boolean republishNoDelete(UserPrincipal currentUser, String id) throws Exception {
		return unpublishNoDelete(currentUser, id) && publishNoDelete(currentUser, id);
	}
	
	public boolean publishNoDelete(UserPrincipal currentUser, String id) throws Exception {
		
		Optional<FilterAnnotationValidation> doc = favRepository.findById(new ObjectId(id));
	
		if (doc.isPresent()) {
			FilterAnnotationValidation fav = doc.get();
			
		    Dataset ds = datasetRepository.findByUuid(fav.getDatasetUuid()).get();
		    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
			
			PublishState ps = fav.getPublishState(vc.getDatabaseId());
		
			ps.setPublishState(DatasetState.PUBLISHING);
			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
		
			favRepository.save(fav);
			
			virtuosoJDBC.publish(currentUser, vc.getName(), fav);
	    	
			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			ps.setPublishState(DatasetState.PUBLISHED);
			
			favRepository.save(fav);
		}
		
		return true;
	}
	
	public boolean unpublishNoDelete(UserPrincipal currentUser, String id) throws Exception {
		
		Optional<FilterAnnotationValidation> doc = favRepository.findById(new ObjectId(id));
	
		if (doc.isPresent()) {
			FilterAnnotationValidation fav = doc.get();
		    Dataset ds = datasetRepository.findByUuid(fav.getDatasetUuid()).get();
		    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
			
			PublishState ps = fav.getPublishState(vc.getDatabaseId());
		
			ps.setPublishState(DatasetState.UNPUBLISHING);
			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
			
			favRepository.save(fav);
			
			virtuosoJDBC.unpublish(currentUser, vc.getName(), fav);
	    	
			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			ps.setPublishState(DatasetState.UNPUBLISHED);
			
			favRepository.save(fav);
		}
		
		return true;
	}
	
//	public boolean unpublish(UserPrincipal currentUser, String aegId) throws Exception {
//		
//		Optional<AnnotationEditGroup> doc = aegRepository.findById(new ObjectId(aegId));
//	
//		if (doc.isPresent()) {
//			AnnotationEditGroup adoc = doc.get();
//			
//			PublishState ps = adoc.getPublishState(virtuosoConfiguration.getDatabaseId());
//		
//			ps.setPublishState(DatasetState.UNPUBLISHING);
//			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			
//			aegRepository.save(adoc);
//			
//			List<AnnotationEdit> adds = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndEditTypeAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), AnnotationEditType.ADD, adoc.getUserId());
//		
//			virtuosoJDBC.unpublish(currentUser, adoc, adds);
//	    	
//			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//			ps.setPublishState(DatasetState.UNPUBLISHED);
//			
//			aegRepository.save(adoc);
//		}
//		
//		System.out.println("UNPUBLICATION COMPLETED");
//		
//		return true;
//	}
	
	public Optional<String> getLastExecution(UserPrincipal currentUser, String favId) throws Exception {
		Optional<FilterAnnotationValidation> entry = favRepository.findById(new ObjectId(favId));
		
		if (!entry.isPresent()) {
			return Optional.empty();
		}
			
		FilterAnnotationValidation fav = entry.get();

		String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + fav.getDatasetUuid() + "/";
		
		StringBuffer result = new StringBuffer();
			
//		result.append(">> ADD    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//		result.append(new String(Files.readAllBytes(Paths.get(datasetFolder + fav.getUuid().toString() + "_add.trig"))));
//		result.append("\n");
//		result.append(">> DELETE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//		result.append(new String(Files.readAllBytes(Paths.get(datasetFolder + fav.getUuid().toString() + "_delete.trig"))));
		
		result.append(new String(Files.readAllBytes(Paths.get(datasetFolder + fav.getUuid().toString() + ".trig"))));

		return Optional.of(result.toString());
	}


}
