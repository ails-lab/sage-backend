package ac.software.semantic.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import ac.software.semantic.controller.APIPagedAnnotationValidationController;
import ac.software.semantic.controller.ExecuteMonitor;
import ac.software.semantic.controller.SSEController;
import ac.software.semantic.controller.APIPagedAnnotationValidationController.PageRequestMode;
import ac.software.semantic.payload.*;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
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

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditFilter;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditType;
import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.ExecuteState;
import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.FilterValidationType;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.PagedAnnotationValidatationDataResponse;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepositoryPage;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.d2rml.monitor.FileSystemOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SOAVocabulary;

import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.ANNOTATED_ONLY;
import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.UNANNOTATED_ONLY;

@Service
public class PagedAnnotationValidationService {

	Logger logger = LoggerFactory.getLogger(PagedAnnotationValidationService.class);

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
	AnnotatorDocumentRepository annotatorDocumentRepository;

    @Autowired
	AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	PagedAnnotationValidationRepository pavRepository;

	@Autowired
	PagedAnnotationValidationRepositoryPage pavpRepository;
	
	@Autowired
	AnnotationEditRepository annotationEditRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	PagedAnnotationValidationPageLocksService locksService;

	@Autowired
	VirtuosoJDBC virtuosoJDBC;
	
	@Autowired
	ResourceLoader resourceLoader;
	
	public boolean createPagedAnnotationValidation(UserPrincipal currentUser, String aegId) {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));
		if (!aegOpt.isPresent()) {
			return false;
		}		

		// temporary: do not create more that one pavs for an aeg;
		List<PagedAnnotationValidation> pavList = pavRepository.findByAnnotationEditGroupId(new ObjectId(aegId));
		if (pavList.size() > 0) {
			return false;
		}		

		PagedAnnotationValidation pav = null;
		
		try {
			AnnotationEditGroup aeg = aegOpt.get();
			
			ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(aeg.getDatasetUuid()).get();
			VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			List<String> annotatorUuids = annotatorDocumentRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList());
			String annfilter = AnnotationEditGroup.annotatorFilter("v", annotatorUuids);

			pav = new PagedAnnotationValidation();
			pav.setUserId(new ObjectId(currentUser.getId()));
			pav.setUuid(UUID.randomUUID().toString());
			pav.setAnnotationEditGroupId(aeg.getId());
			pav.setDatasetUuid(aeg.getDatasetUuid());
			pav.setOnProperty(aeg.getOnProperty());
			pav.setAsProperty(aeg.getAsProperty());
			pav.setAnnotatorDocumentUuid(annotatorUuids);
			pav.setComplete(false);
	
			String datasetUri = SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString();
			String spath = aeg.getOnPropertyAsString();
	
			logger.info("Starting paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ".");
			
			String annotatedCountSparql = 
					"SELECT (count(DISTINCT ?value) AS ?count)" + 
			        "WHERE { " + 
					"  GRAPH <" + datasetUri + "> { " + 
			        "    ?s " + spath + " ?value }  " + 
					"  GRAPH <" + aeg.getAsProperty() + "> { " + 
			        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
					"     <" + OAVocabulary.hasTarget + "> ?r . " + 
				    annfilter + 
			        "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
			        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } " +
	                "  FILTER (isLiteral(?value)) " + 
	                " } ";
	                
			int annotatedValueCount = 0;
			
	//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));
	
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
					QueryFactory.create(annotatedCountSparql, Syntax.syntaxSPARQL_11))) {
	
				ResultSet rs = qe.execSelect();
	
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					annotatedValueCount = sol.get("count").asLiteral().getInt();
				}
			}
	
			int annotatedPages = annotatedValueCount / pageSize + (annotatedValueCount % pageSize > 0 ? 1 : 0);
	
			String nonAnnotatedCountSparql = 
					"SELECT (count(DISTINCT ?value) AS ?count)" + 
			        "WHERE { " + 
					"  GRAPH <" + datasetUri + "> { " + 
			        "    ?s " + spath + " ?value }  " + 
					"  FILTER NOT EXISTS {GRAPH <" + aeg.getAsProperty() + "> { " + 
			        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
					"     <" + OAVocabulary.hasTarget + "> ?r . " + 
				    annfilter + 			        
			        "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
			        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } " +
			        "  FILTER (isLiteral(?value)) " +
			        "}";
	
			int nonAnnotatedValueCount = 0;
			
	//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));
	
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
					QueryFactory.create(nonAnnotatedCountSparql, Syntax.syntaxSPARQL_11))) {
	
				ResultSet rs = qe.execSelect();
	
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					nonAnnotatedValueCount = sol.get("count").asLiteral().getInt();
				}
			}
	
			int nonAnnotatedPages = nonAnnotatedValueCount / pageSize + (nonAnnotatedValueCount % pageSize > 0 ? 1 : 0);
	
			String annotationsCountSparql = 
					"SELECT (count(?v) AS ?count)" + 
			        "WHERE { " + 
					"  GRAPH <" + datasetUri + "> { " + 
			        "    ?s " + spath + " ?value }  " + 
					"  GRAPH <" + aeg.getAsProperty() + "> { " + 
			        "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
					"     <" + OAVocabulary.hasTarget + "> ?r . " + 
				    annfilter + 			        
			        "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
			        "     <" + SOAVocabulary.onValue + "> ?value ; " + 
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } " + 
			        "  FILTER (isLiteral(?value)) " +
			        "} ";
			
			int annotationsCount = 0;
			
	//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));
	
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
					QueryFactory.create(annotationsCountSparql, Syntax.syntaxSPARQL_11))) {
	
				ResultSet rs = qe.execSelect();
	
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					annotationsCount = sol.get("count").asLiteral().getInt();
				}
			}
			
			logger.info("Paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ": valueCount=" + annotatedValueCount + "/" + nonAnnotatedValueCount + " pages=" + annotatedPages + "/" + nonAnnotatedPages);
	
			pav.setPageSize(pageSize);
			pav.setAnnotationsCount(annotationsCount);
			pav.setAnnotatedPagesCount(annotatedPages);
			pav.setNonAnnotatedPagesCount(nonAnnotatedPages);
			
			pav = pavRepository.save(pav);
	
			logger.info("Finished paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ".");
	
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			
			if (pav != null) {
				pavRepository.deleteById(pav.getId());
			}
			
			return false;
		}
	}
	
	public List<ValueCount> getValuesForPage(VirtuosoConfiguration vc, String datasetUri, String onPropertyString, String asProperty, List<String> annotatorUuids, AnnotationValidationRequest mode, int page) {
		String annfilter = AnnotationEditGroup.annotatorFilter("v", annotatorUuids);
		
		String graph = 
			"GRAPH <" + asProperty + "> { " +
            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		    "     <" + OAVocabulary.hasTarget + "> ?r . " + 
            annfilter +
            "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
		    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
            "     <" + OAVocabulary.hasSource + "> ?s . " + 
		    " }";
		
		if (mode == AnnotationValidationRequest.ALL) {
//			graph +=  " OPTIONAL { " + graph + " }  "; 
		} else if (mode == ANNOTATED_ONLY) {
			graph = " FILTER EXISTS { " + graph + " }  ";
		} else if (mode == UNANNOTATED_ONLY) {
			graph = " FILTER NOT EXISTS { " + graph + " }  "; 
		}
		
		// should also filter out URI values here but this would spoil pagination due to previous bug.
		String sparql = 
            "SELECT ?value ?valueCount WHERE { " +
			"  SELECT ?value (count(*) AS ?valueCount)" +
	        "  WHERE { " + 
		    "    GRAPH <" + datasetUri + "> { " + 
	        "      ?s " + onPropertyString + " ?value } " + 
		         graph +
		    "    FILTER (isLiteral(?value)) " +		         
		    "  } " +
			"  GROUP BY ?value " + 
			"  ORDER BY desc(?valueCount) ?value } " + 
 		    "LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1);


    	List<ValueCount> values = new ArrayList<>();
//	    System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
    		
    		ResultSet rs = qe.execSelect();
    		
    		while (rs.hasNext()) {
    			QuerySolution qs = rs.next();
    			RDFNode value = qs.get("value");
    			int count = qs.get("valueCount").asLiteral().getInt(); //valueCount is the number a value appears (not of annotations on value)
    			
    			values.add(new ValueCount(value, count));
    		}
    	}
    	
    	return values;
		
	}



	/*
		A function to update the Page object and set isAssigned to a boolean value, after we have locked it.
		Overloaded.
	 */
	public UpdateLockedPageResponse updateLockedPageIsAssigned(int page, String pavId, AnnotationValidationRequest mode, boolean isAssigned) {
		Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findByPagedAnnotationValidationIdAndModeAndPage(new ObjectId(pavId), mode, page);
		if (!pavpOpt.isPresent()) {
			System.out.println("not present");
			return new UpdateLockedPageResponse(false, null);
		}
		PagedAnnotationValidationPage pavp = pavpOpt.get();
		pavp.setAssigned(isAssigned);
		try {
			pavpRepository.save(pavp);
		}
		catch(Exception e) {
			e.printStackTrace();
			return new UpdateLockedPageResponse(true, null);
		}
		return new UpdateLockedPageResponse(false, pavp);
	}

	public boolean updateLockedPageIsAssigned(PagedAnnotationValidationPage pavp, boolean isAssigned) {
		pavp.setAssigned(isAssigned);
		try {
			pavpRepository.save(pavp);
		}
		catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public ObjectId lockPage(UserPrincipal currentUser, String pavId, int page, AnnotationValidationRequest mode) {
		ObjectId locked = locksService.obtainLock(currentUser.getId(), pavId, page, mode);
		if(locked != null) {
			return locked;
		}
		else {
			return null;
		}
	}

	public boolean unlockPage(UserPrincipal currentUser, String pavId, int page, AnnotationValidationRequest mode) {
		boolean unlocked = locksService.removeLock(currentUser.getId(), pavId, page, mode);
		if(unlocked) {
			return true;
		}
		else {
			return false;
		}
	}
	/*
		This function serves the UNANNOTADED_ONLY_SERIAL and ANNOTATED_ONLY_SERIAL PageRequestMode.
	 */

	public PagedAnnotationValidatationDataResponse getCurrent(UserPrincipal currentUser, VirtuosoConfiguration vc, String pavId, int currentPage, AnnotationValidationRequest mode, PageRequestMode pgMode) {
		// try to lock current page to give it back to user
		PagedAnnotationValidatationDataResponse res;
		UpdateLockedPageResponse updateRes;
		ObjectId lockId;
		lockId = locksService.obtainLock(currentUser.getId(), pavId, currentPage, mode);
		if (lockId != null) {
			updateRes = updateLockedPageIsAssigned(currentPage, pavId, mode, true);
			if (updateRes.isError()) {
				return new PagedAnnotationValidatationDataResponse("INTERNAL_ERROR");
			}
			try {
				res = view(currentUser, vc, pavId, mode, currentPage, false);
			}
			catch(Exception e) {
				return new PagedAnnotationValidatationDataResponse("NO_PAGE_FOUND");
			}
			res.setLockId(lockId.toString());
			res.setErrorMessage("redirect");
			res.setFilter(pgMode.toString());
			return res;
		}
		else {
			// if locking current page fails, just throw an error...
			return new PagedAnnotationValidatationDataResponse("NO_PAGE_FOUND");
		}
	}

	public PagedAnnotationValidatationDataResponse determinePageSerial(UserPrincipal currentUser, String pavId, int currentPage, PageRequestMode mode, APIPagedAnnotationValidationController.NavigatePageMode navigation) {
			Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
			if (!pavOpt.isPresent()) {
				return new PagedAnnotationValidatationDataResponse();
			}

			if(!locksService.checkForLockAndRemove(currentUser)) {
				return new PagedAnnotationValidatationDataResponse("Error on lock deletion. Try again.");
			}

			PagedAnnotationValidation pav = pavOpt.get();
			ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
			VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());


			//number of total pages that exist - must not bypass that!
			int unannotated_pages = pav.getNonAnnotatedPagesCount();
			int annotated_pages = pav.getAnnotatedPagesCount();
//			System.out.println(unannotated_pages+" "+annotated_pages);

			PagedAnnotationValidatationDataResponse res = null;
			Optional<PagedAnnotationValidationPage> pavpOpt;
			PagedAnnotationValidationPage pavp;
			UpdateLockedPageResponse updateRes;
			ObjectId lockId;

			//UNANNOTATED_ONLY modes
			if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SERIAL)) {
				/* check for the range of current_page, page_count.
				   If we get a lock, then check if the page exists to update isAssigned, else create it
				*/
				if (navigation.equals(APIPagedAnnotationValidationController.NavigatePageMode.RIGHT)) {
					for (int i = currentPage + 1; i <= unannotated_pages; i++) {
						lockId = lockPage(currentUser, pavId, i, UNANNOTATED_ONLY);
						if (lockId != null) {
							updateLockedPageIsAssigned(i, pavId, UNANNOTATED_ONLY, true);
							res = view(currentUser, vc, pavId, UNANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							break;
						}
					}
				}
				else {
					for (int i = currentPage - 1; i > 0; i--) {
						lockId = lockPage(currentUser, pavId, i, UNANNOTATED_ONLY);
						if (lockId != null) {
							updateLockedPageIsAssigned(i, pavId, UNANNOTATED_ONLY, true);
							res = view(currentUser, vc, pavId, UNANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							break;
						}
					}
				}
			}
			// ANNOTATED_ONLY modes
			else {
				if (navigation.equals(APIPagedAnnotationValidationController.NavigatePageMode.RIGHT)) {
					for (int i = currentPage + 1; i <= annotated_pages; i++) {
						lockId = lockPage(currentUser, pavId, i, ANNOTATED_ONLY);
						if (lockId != null) {
							updateRes = updateLockedPageIsAssigned(i, pavId, ANNOTATED_ONLY, true);
							if (updateRes.isError()) {
								return new PagedAnnotationValidatationDataResponse("Error on server..");
							}

							//This checks for NOT_VALIDATED MODE
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED)) {
								if (updateRes.getPage() == null || updateRes.getPage().getValidatedCount() == 0) {
									res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i,false);
									res.setLockId(lockId.toString());
									res.setFilter("ANNOTATED_ONLY_NOT_VALIDATED");
									break;
								}
								else {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
							}

							// NOT_COMPLETE mode
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)) {
								// If null then we have a new page, we go to next iteration.
								if (updateRes.getPage() == null) {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
								else {
									if (updateRes.getPage().getValidatedCount() > 0 && updateRes.getPage().getUnvalidatedCount() > 0 )  {
										res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i,false);
										res.setLockId(lockId.toString());
										res.setFilter("ANNOTATED_ONLY_NOT_COMPLETE");
										break;
									}
									else {
										unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
										continue;
									}
								}
							}
							res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							res.setFilter("ANNOTATED_ONLY_SERIAL");
							break;
						}
					}
				}
				else {
					for (int i = currentPage - 1; i > 0; i--) {
						lockId = lockPage(currentUser, pavId, i, ANNOTATED_ONLY);
						if (lockId != null) {
							updateRes = updateLockedPageIsAssigned(i, pavId, ANNOTATED_ONLY, true);
							if (updateRes.isError()) {
								return new PagedAnnotationValidatationDataResponse("Error on server..");
							}

							//This checks for NOT_VALIDATED MODE
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_VALIDATED)) {
								if (updateRes.getPage() == null || updateRes.getPage().getValidatedCount() == 0) {
									res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
									res.setLockId(lockId.toString());
									res.setFilter("ANNOTATED_ONLY_NOT_VALIDATED");
									break;
								}
								else {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
							}

							// NOT_COMPLETE mode
							if (mode.equals(PageRequestMode.ANNOTATED_ONLY_NOT_COMPLETE)) {
								// If null then we have a new page, we go to next iteration.
								if (updateRes.getPage() == null) {
									unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
									continue;
								}
								else {
									if (updateRes.getPage().getValidatedCount() > 0 && updateRes.getPage().getUnvalidatedCount() > 0 )  {
										res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
										res.setLockId(lockId.toString());
										res.setFilter("ANNOTATED_ONLY_NOT_COMPLETE");
										break;
									}
									else {
										unlockPage(currentUser, pavId, i, ANNOTATED_ONLY);
										continue;
									}
								}
							}
							res = view(currentUser, vc, pavId, ANNOTATED_ONLY, i, false);
							res.setLockId(lockId.toString());
							res.setFilter("ANNOTATED_ONLY_SERIAL");
							break;
						}
					}
				}
			}
			// If we found a page, return it
			if (res != null) {
				return res;
			}
			// Else, try to lock again our "current page"
			else {
				// try to lock current page to give it back to user
				AnnotationValidationRequest req;
				if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SERIAL)) {
					req = UNANNOTATED_ONLY;
				}
				else {
					req = ANNOTATED_ONLY;
				}
				res = getCurrent(currentUser, vc, pavId, currentPage, req, mode);
				return res;
			}
		}

	/*
		This function serves the UNANNOTATED_ONLY_SPECIFIC_PAGE and ANNOTATED_ONLY_SPECIFIC_PAGE PageRequestModes.
	 */
	public PagedAnnotationValidatationDataResponse getSpecificPage(UserPrincipal currentUser, String pavId, int requestedPage, PageRequestMode mode, int currentPage) {
		PagedAnnotationValidatationDataResponse res;
		ObjectId lockId;
		UpdateLockedPageResponse updateRes;

		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
		if (!pavOpt.isPresent()) {
			return new PagedAnnotationValidatationDataResponse("PagedAnnotationValidation id is not present in database.");
		}
		PagedAnnotationValidation pav = pavOpt.get();

		ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
		VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		if(!locksService.checkForLockAndRemove(currentUser)) {
			return new PagedAnnotationValidatationDataResponse("Error on lock deletion. Try again.");
		}

		// check if requested page exists or not
		if ((mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE) && (requestedPage > pav.getNonAnnotatedPagesCount()))
			|| (mode.equals(PageRequestMode.ANNOTATED_ONLY_SPECIFIC_PAGE) && (requestedPage > pav.getAnnotatedPagesCount()))) {
			if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE)) {
				res = getCurrent(currentUser, vc, pavId, currentPage, UNANNOTATED_ONLY, mode);
			}
			else {
				res = getCurrent(currentUser, vc, pavId, currentPage, ANNOTATED_ONLY, mode);
			}
			return res;
		}

		if (mode.equals(PageRequestMode.UNANNOTATED_ONLY_SPECIFIC_PAGE)) {
			lockId = locksService.obtainLock(currentUser.getId(), pavId, requestedPage, UNANNOTATED_ONLY);

			if (lockId != null) {
				// If the page exists, mark it as assigned.
				updateRes = updateLockedPageIsAssigned(requestedPage, pavId, UNANNOTATED_ONLY, true);
				if (updateRes.isError()) {
					return new PagedAnnotationValidatationDataResponse("Error on server..");
				}
				res = view(currentUser, vc, pavId, UNANNOTATED_ONLY, requestedPage, false);
				res.setLockId(lockId.toString());
				return res;
			}
			else {
				// try to lock current page to give it back to user
				res = getCurrent(currentUser, vc, pavId, currentPage, UNANNOTATED_ONLY, mode);
				return res;
			}
		}
		// ANNOTATED_ONLY_SPECIFIC_PAGE request
		else {
			lockId = locksService.obtainLock(currentUser.getId(), pavId, requestedPage, ANNOTATED_ONLY);
			if (lockId != null) {
				updateRes = updateLockedPageIsAssigned(requestedPage, pavId, ANNOTATED_ONLY, true);
				if (updateRes.isError()) {
					return new PagedAnnotationValidatationDataResponse("Error on server..");
				}
				res = view(currentUser, vc, pavId, ANNOTATED_ONLY, requestedPage, false);
				res.setLockId(lockId.toString());
				return res;
			}
			else {
				res = getCurrent(currentUser, vc, pavId, currentPage, ANNOTATED_ONLY, mode);
				return res;
			}
		}
	}


	public void determinePage(UserPrincipal currentUser, String pavId, PageRequestMode mode, int currentPage) {
	}

	public ProgressResponse getProgress(UserPrincipal currentUser, String pavId) {
		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
		int totalAnnotations = 0;
		int totalValidations = 0;
		int totalAdded = 0;
		int totalAccepted = 0;
		int totalRejected = 0;
		int totalNeutral = 0;

		PagedAnnotationValidation pav;
		if (pavOpt.isPresent()) {
			pav = pavOpt.get();
			totalAnnotations = pav.getAnnotationsCount();
		}
		else {
			return null;
		}
		List<PagedAnnotationValidationPage> pages = pavpRepository.findByPagedAnnotationValidationIdAndMode(new ObjectId(pavId), ANNOTATED_ONLY);
		for(PagedAnnotationValidationPage page: pages) {
			totalValidations += page.getValidatedCount();
			totalAdded += page.getAddedCount();
			totalAccepted += page.getAcceptedCount();
			totalRejected += page.getRejectedCount();
			totalNeutral += page.getNeutralCount();
		}

		pages = pavpRepository.findByPagedAnnotationValidationIdAndMode(new ObjectId(pavId), UNANNOTATED_ONLY);
		for(PagedAnnotationValidationPage page: pages) {
			totalAdded += page.getAddedCount();
		}

		ProgressResponse res = new ProgressResponse();
		res.setTotalAnnotations(totalAnnotations);
		res.setTotalValidations(totalValidations);
		res.setTotalAdded(totalAdded);

		res.setTotalAccepted(totalAccepted);
		res.setTotalRejected(totalRejected);
		res.setTotalNeutral(totalNeutral);

		return res;
	}

	public PagedAnnotationValidatationDataResponse view(UserPrincipal currentUser, VirtuosoConfiguration vc, String pavId, AnnotationValidationRequest mode, int page, boolean ignoreAdded) {
		
		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
		if (!pavOpt.isPresent()) {
			return new PagedAnnotationValidatationDataResponse();
		}		
		
		PagedAnnotationValidation pav = pavOpt.get();

		 
		String datasetUri = SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString();
    	String onPropertyString = pav.getOnPropertyAsString();
		String annfilter = AnnotationEditGroup.annotatorFilter("v", pav.getAnnotatorDocumentUuid());

    	List<ValueCount> values = getValuesForPage(vc, datasetUri, onPropertyString, pav.getAsProperty(), pav.getAnnotatorDocumentUuid(), mode, page);
    	
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

    	StringBuffer sb = new StringBuffer();
    	for (ValueCount vct : values) {
			AnnotationEditValue aev = null;
    		
    		if (vct.getValue().isLiteral()) {
				Literal l = vct.getValue().asLiteral();
				String lf = l.getLexicalForm();
				
				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
				sb.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString());
	    		sb.append(" ");
				
				aev = new AnnotationEditValue(vct.getValue().asLiteral());
			} else {
				//ignore URI values. They should not be returned by getValuesForPage 
				
//				sb.append("<" + vc.getValue().toString() + ">");
//	    		sb.append(" ");

//				aev = new AnnotationEditValue(vc.getValue().asResource());
			}
    		
    		if (aev != null) {
				ValueAnnotation va = new ValueAnnotation();
				va.setOnValue(aev);
				va.setCount(vct.getCount()); // the number of appearances of the value
				
				res.put(aev, va);
    		}
    	}
    	
    	String valueString = sb.toString();
    	
		String sparql = null;
		
		String graph = 
			"GRAPH <" + pav.getAsProperty() + "> { " + 
		    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
	        "     <" + OAVocabulary.hasTarget + "> ?r . " + 
		    annfilter +
		    "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
		    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		    "     <" + OAVocabulary.hasSource + "> ?s . " + 
		    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
		    " { ?v <" + OAVocabulary.hasBody + "> [ " + 
		    " a <" + OWLTime.DateTimeInterval + "> ; " + 
		    " <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
		    " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " + 
		    " OPTIONAL { ?r <" + SOAVocabulary.start + "> ?start }  " + 
		    " OPTIONAL { ?r <" + SOAVocabulary.end + "> ?end } } ";
		    		
		if (mode == ANNOTATED_ONLY) {
			sparql = 
					"SELECT distinct ?value ?t ?ie ?start ?end  (count(*) AS ?count)" + 
		            "WHERE { " + 
					"  GRAPH <" + datasetUri + "> { " + 
		            "    ?s " + onPropertyString + " ?value }  " + 
                       graph +  
                    "  VALUES ?value { " + valueString  + " } " +                       
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end " +
					"ORDER BY DESC(?count) ?value ?start ?end";
		} else if (mode == UNANNOTATED_ONLY) {
			sparql = 
					"SELECT distinct ?value (count(*) AS ?count) " + 
			        "WHERE { " + 
		            "  GRAPH <" + datasetUri + "> { " + 
					"    ?s " + onPropertyString + " ?value }  " + 
		            "  FILTER NOT EXISTS { " + 
					"    GRAPH <" + pav.getAsProperty() + "> { " + 
					"      ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "         <" + OAVocabulary.hasTarget + "> ?r . " + 
		            annfilter +
					"      ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
					"         <" + SOAVocabulary.onValue + "> ?value ; " + 
					"         <" + OAVocabulary.hasSource + "> ?s  } } " +
					"  VALUES ?value { " + valueString  + " } " +
					"} " +
					"GROUP BY ?value  " +
			        "ORDER BY DESC(?count) ?value ";
		}    	
		
//    	System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
		int totalAnnotationsCount = 0;
		if (valueString.length() > 0) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
			
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					RDFNode value = sol.get("value");
					
					String ann = sol.get("t") != null ? sol.get("t").toString() : null;
					String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;

					int start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : -1;
					int end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : -1;
					
					int count = sol.get("count").asLiteral().getInt();
					
					AnnotationEditValue aev = null;
					if (value.isResource()) {
						aev = new AnnotationEditValue(value.asResource());
					} else if (value.isLiteral()) {
						aev = new AnnotationEditValue(value.asLiteral());
					}
					
					totalAnnotationsCount += count;
					
					ValueAnnotation va = res.get(aev);
					if (va != null && ann != null) {
						ValueAnnotationDetail vad = new ValueAnnotationDetail();
						vad.setValue(ann);
						vad.setValue2(ie);
						vad.setStart(start);
						vad.setEnd(end);
						vad.setCount(count); // the number of appearances of the annotation 
						                     // it is different than the number of appearances of the value if multiple annotations exist on the same value
						
						va.getDetails().add(vad);
					}
				}

			}
		}
		
		ObjectId uid = new ObjectId(currentUser.getId());
		
		int validatedCount = 0;
		int addedCount = 0;
		int acceptedCount = 0;
		int rejectedCount = 0;
		int neutralCount = 0;
				
		for (Map.Entry<AnnotationEditValue, ValueAnnotation> entry : res.entrySet()) {
			AnnotationEditValue aev = entry.getKey();
			ValueAnnotation nva = entry.getValue();
			
			Set<ObjectId> set = new HashSet<>();
			
			for (ValueAnnotationDetail vad : nva.getDetails()) {
				Optional<AnnotationEdit> editOpt = null;
				if (aev.getIri() != null) {
					editOpt = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAnnotationValueAndStartAndEnd(pav.getAnnotationEditGroupId(), aev.getIri(), vad.getValue(), vad.getStart(), vad.getEnd());
				} else {
					editOpt = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAnnotationValueAndStartAndEnd(pav.getAnnotationEditGroupId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype(), vad.getValue(), vad.getStart(), vad.getEnd());
				}
				
				// no added annotation should appear here
				if (editOpt.isPresent()) {
					AnnotationEdit edit = editOpt.get();

					vad.setId(edit.getId().toString());
					
					if (edit.getAcceptedByUserId().size() > 0 || edit.getRejectedByUserId().size() > 0) {
						validatedCount += vad.getCount();
						
						if (edit.getAcceptedByUserId().size() > edit.getRejectedByUserId().size()) {
							acceptedCount += vad.getCount();
						}  else if (edit.getAcceptedByUserId().size() < edit.getRejectedByUserId().size()) {
							rejectedCount += vad.getCount();
						} else {
							neutralCount += vad.getCount();
						}
						
					}
					
					set.add(edit.getId());
					
					if (edit.getAcceptedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.ACCEPT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
					} else if (edit.getRejectedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.REJECT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
					} else if (edit.getAddedByUserId().contains(uid)) { // should not allow addition of existing annotation
//						vad.setState(AnnotationEditType.ADD);
//						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
//						vad.setOthersRejected(edit.getRejectedByUserId().size());
					} else {
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
					}
				}
			}

			if (!ignoreAdded) {
				List<AnnotationEdit> edits = null;
				if (aev.getIri() != null) {
					edits = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAdded(pav.getAnnotationEditGroupId(), aev.getIri());
				} else {
					edits = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAdded(pav.getAnnotationEditGroupId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype());
				}

				for (AnnotationEdit edit : edits) {
					if (set.contains(edit.getId())) {
						continue;
					}
					
					addedCount += nva.getCount();

					ValueAnnotationDetail vad = new ValueAnnotationDetail();
					vad.setValue(edit.getAnnotationValue());
					vad.setStart(edit.getStart());
					vad.setEnd(edit.getEnd());
					vad.setCount(nva.getCount());
					
					vad.setId(edit.getId().toString());

					if (edit.getAddedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.ADD);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
					} else if (edit.getAcceptedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.ACCEPT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
					} else if (edit.getRejectedByUserId().contains(uid)) {
						vad.setState(AnnotationEditType.REJECT);
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
					} else {
						vad.setOthersAccepted(edit.getAcceptedByUserId().size());
						vad.setOthersRejected(edit.getRejectedByUserId().size());
					}

					nva.getDetails().add(vad);
				}
			}
		}		
		
		// get page
		Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findByPagedAnnotationValidationIdAndModeAndPage(new ObjectId(pavId), mode, page);
		PagedAnnotationValidationPage pavp = null;
		if (!pavpOpt.isPresent()) {
			pavp = new PagedAnnotationValidationPage();
			pavp.setPagedAnnotationValidationId(new ObjectId(pavId));
			pavp.setAnnotationEditGroupId(pav.getAnnotationEditGroupId());
			pavp.setMode(mode);
			pavp.setPage(page);
			pavp.setAnnotationsCount(totalAnnotationsCount);
			pavp.setValidatedCount(validatedCount);
			pavp.setUnvalidatedCount(totalAnnotationsCount - validatedCount);
			pavp.setAddedCount(addedCount);
			pavp.setAcceptedCount(acceptedCount);
			pavp.setRejectedCount(rejectedCount);
			pavp.setNeutralCount(neutralCount);			
			pavp.setAssigned(true);
			
			pavpRepository.save(pavp);
		} else {
			pavp = pavpOpt.get();
		}
		
		PagedAnnotationValidatationDataResponse pr = new PagedAnnotationValidatationDataResponse();
		pr.setId(pav.getId().toString());
		pr.setData(new ArrayList<>(res.values()));
		pr.setCurrentPage(page);
		pr.setMode(mode);
		pr.setPagedAnnotationValidationId(pavp.getPagedAnnotationValidationId().toString());
		pr.setPagedAnnotationValidationPageId(pavp.getId().toString());
		
		if (mode == ANNOTATED_ONLY) {
			pr.setTotalPages(pav.getAnnotatedPagesCount());
		} else {
			pr.setTotalPages(pav.getNonAnnotatedPagesCount());
		}
		
		return pr;
    } 
	
	public boolean endValidation(String pavId) {
		Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
		if (!pavOpt.isPresent()) {
			return false;
		}
		PagedAnnotationValidation pav = pavOpt.get();
		pav.setComplete(true);
		pavRepository.save(pav);
		return true;
	}

	public List<DatasetProgressResponse> getDatasetProgress(UserPrincipal currentUser, String uuid) {
		List<DatasetProgressResponse> res = new ArrayList<>();
		DatasetProgressResponse dataRes;
		ProgressResponse progRes;

		List<PagedAnnotationValidation> datasetValidations = pavRepository.findByDatasetUuid(uuid);
		for (PagedAnnotationValidation val : datasetValidations) {
			dataRes = new DatasetProgressResponse();
			dataRes.setValidationId(val.getId().toString());
			dataRes.setPropertyName(val.getOnPropertyAsString());
			dataRes.setAsProperty(val.getAsProperty());

			progRes = getProgress(currentUser, val.getId().toString());

			//round the result
			try {
				BigDecimal bd = BigDecimal.valueOf((1.0 * progRes.getTotalValidations() / progRes.getTotalAnnotations()) * 100);
				bd = bd.setScale(2, RoundingMode.HALF_UP);
				dataRes.setProgress(bd.doubleValue());
			}
			catch(Exception e) {
				dataRes.setProgress(0);
			}
			dataRes.setTotalAdded(progRes.getTotalAdded());
			dataRes.setTotalAnnotations(progRes.getTotalAnnotations());
			dataRes.setTotalValidations(progRes.getTotalValidations());
			dataRes.setTotalAccepted(progRes.getTotalAccepted());
			dataRes.setTotalRejected(progRes.getTotalRejected());
			dataRes.setTotalNeutral(progRes.getTotalNeutral());
			res.add(dataRes);
		}

		return res;
	}
	

	//does not work needs updating with OutputHandler
//	public boolean execute(UserPrincipal currentUser, String pavId, ApplicationEventPublisher applicationEventPublisher) throws Exception {
//
//		Optional<PagedAnnotationValidation> odoc = pavRepository.findById(pavId);
//	    if (!odoc.isPresent()) {
//	    	return false;
//	    }
//    	
//	    PagedAnnotationValidation pav = odoc.get();
//	    
//	    Date executeStart = new Date(System.currentTimeMillis());
//	    
//    	ExecuteState es = pav.getExecuteState(fileSystemConfiguration.getId());
//    	
//    	String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + pav.getDatasetUuid() + "/";
//    	
//		// Clearing old files
//		if (es.getExecuteState() == MappingState.EXECUTED) {
//			for (int i = 0; i < es.getExecuteShards(); i++) {
//				(new File(datasetFolder + pav.getUuid() + "_add" + (i == 0 ? "" : "_#" + i) + ".trig")).delete();
//			}
//			new File(datasetFolder + pav.getUuid() + "_add_catalog.trig").delete();
//			new File(datasetFolder + pav.getUuid() + "_delete.trig").delete();
//			new File(datasetFolder + pav.getUuid() + "_delete_catalog.trig").delete();
//		}
//		
//		es.setExecuteState(MappingState.EXECUTING);
//		es.setExecuteStartedAt(executeStart);
//		es.setExecuteShards(0);
//		es.setCount(0);
//		
//		pavRepository.save(pav);
//
//		if (!new File(datasetFolder).exists()) {
//			new File(datasetFolder).mkdir();
//		}
//		
//		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(
//				datasetFolder, pav.getUuid() + "_add",
//				shardSize);
//				Writer delete = new OutputStreamWriter(new FileOutputStream(new File(datasetFolder + pav.getUuid() + "_delete.trig"), false), StandardCharsets.UTF_8);
//				Writer deleteCatalog = new OutputStreamWriter(new FileOutputStream(new File(datasetFolder + pav.getUuid() + "_delete_catalog.trig"), false), StandardCharsets.UTF_8)				
//				) {
//			
//			Executor exec = new Executor(outhandler, safeExecute);
//			exec.keepSubjects(true);
//			
//			try (ExecuteMonitor em = new ExecuteMonitor("annotation-edit", pavId, null, applicationEventPublisher)) {
//				exec.setMonitor(em);
//				
//				String d2rml = env.getProperty("validator.paged-add.d2rml"); 
//				InputStream inputStream = resourceLoader.getResource("classpath:"+ d2rml).getInputStream();
//				D2RMLModel rmlMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
//
//				Dataset ds2 = DatasetFactory.create();
//				Model deleteModel2 = ds2.getDefaultModel();
//		
//				String onPropertyString = AnnotationEditGroup.onPropertyListAsString(pav.getOnProperty());
//				String annfilter = AnnotationEditGroup.annotatorFilter("v", pav.getAnnotatorDocumentUuid());
//
//				for (AnnotationEdit edit :  annotationEditRepository.findByPagedAnnotationValidationId(new ObjectId(pavId))) {
//					
////					System.out.println(edit.getEditType() + " " + edit.getAnnotationValue());
//					
//					if (edit.getEditType() == AnnotationEditType.ADD) {
//						
//						Map<String, Object> params = new HashMap<>();
//						params.put("iigraph", SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString());
//						params.put("iiproperty", onPropertyString);
//						params.put("iivalue", edit.getOnValue().toString());
//						params.put("iiannotation", edit.getAnnotationValue());
//						params.put("iirdfsource", virtuosoConfiguration.getSparqlEndpoint());
//						params.put("iiconfidence", "1");
//						params.put("iiannotator", SEMAVocabulary.getAnnotationValidator(pav.getUuid()));
//
////						System.out.println(edit.getOnValue().toString());
//						exec.partialExecute(rmlMapping, params);
//						
//					} else if (edit.getEditType() == AnnotationEditType.REJECT) {
//	
////						System.out.println(edit.getOnValue().toString());
//				    	String sparql = 
//				    			"CONSTRUCT { " + 
//					            "  ?annId ?p1 ?o1 ." + 
//					            "  ?o1 ?p2 ?o2 .  " +  
//		     			        "} WHERE { " + 
//		    			        "  GRAPH <" + pav.getAsProperty() + "> { " + 
//							    "   ?v a <" + OAVocabulary.Annotation + "> ?annId . " + 
//		    			        "   ?annId <" + OAVocabulary.hasTarget + "> ?target . " + 
//							    annfilter +
//		    			        "   ?target  <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
//		    			        "            <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " ; " +
//		    			        "            <" + OAVocabulary.hasSource + "> ?s  . " +
//		    		            "   ?annId ?p1 ?o1 . " +
//		    		            "   OPTIONAL { ?o1 ?p2 ?o2 } } . " +	    			        
//		    			        " GRAPH <" + SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString() + "> { " +
//		    			        "  ?s " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
//		                        " GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
//		                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annId . } " +		    			        
//		    			        "}";
//		    	
//				    	Writer sw = new StringWriter();
//				    	
////				    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
//				    	
//				    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//					    	Model model = qe.execConstruct();
//					    	model.setNsPrefixes(new HashMap<>());
//					    	
//							RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_FLAT) ;
//				    	}
//				    	
//						Dataset ds = DatasetFactory.create();
//						Model deleteModel = ds.getDefaultModel();
//	
//						deleteModel.read(new StringReader(sw.toString()), null, "JSON-LD");
//	//	
//						RDFDataMgr.write(delete, deleteModel, RDFFormat.TRIG);
//						delete.write("\n");
//						
//						String sparql2 = 
//				    			"CONSTRUCT { " + 
//					            "  ?adocid <http://purl.org/dc/terms/hasPart> ?annId . " +
//		     			        "} WHERE { " + 
//		    			        "  GRAPH <" + pav.getAsProperty() + "> { " + 
//							    "   ?v a <" + OAVocabulary.Annotation + "> ?annId . " + 
//		    			        "   ?annId <" + OAVocabulary.hasTarget + "> [ " + 
//							    annfilter +
//		    			        "     <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
//		    			        "     <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " ; " +
//		    			        "     <" + OAVocabulary.hasSource + "> ?s ] . " +
//		    		            "  } . " +	    			        
//		    			        " GRAPH <" + SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString() + "> { " +
//		    			        "  ?s " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
//		                        " GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
//		                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annId . } " +		    			        
//		    			        "}";				    					
//	
//				    	Writer sw2 = new StringWriter();
//	
//				    	try (QueryExecution qe2 = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(), QueryFactory.create(sparql2, Syntax.syntaxARQ))) {
//					    	Model model2 = qe2.execConstruct();
//					    	model2.setNsPrefixes(new HashMap<>());
//					    	
////					    	System.out.println(model2);
//					    	
//							RDFDataMgr.write(sw2, model2, RDFFormat.JSONLD_EXPAND_FLAT) ;
//				    	}
//				    	
//						deleteModel2.read(new StringReader(sw2.toString()), null, "JSON-LD");
//		    		}
//					
//				}
//				exec.completeExecution();
//					
//				RDFDataMgr.write(deleteCatalog, deleteModel2, RDFFormat.TRIG);
//	//			deleteCatalog.write("\n");
//		
//				String asetId = UUID.randomUUID().toString();
//		    	
//				Set<Resource> subjects = exec.getSubjects();
//				
//	        	try (Writer sw = new OutputStreamWriter(new FileOutputStream(new File(datasetFolder + pav.getUuid() + "_add_catalog.trig"), false), StandardCharsets.UTF_8)) {
//	//	        		sw.write("<" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + ">\n");
//	        		sw.write("<" + SEMAVocabulary.getAnnotationSet(asetId).toString() + ">\n");
//	        		sw.write("        <http://purl.org/dc/terms/hasPart>\n" );
//	        		sw.write("                " );
//	        		int c = 0;
//	        		for (Resource r : subjects) {
//	        			if (c++ > 0) {
//	        				sw.write(" , ");
//	        			}
//	        			sw.write("<" + r.getURI() + ">");
//	        		}
//	        		sw.write(" .");
//	    		}
//	        	
//				Date executeFinish = new Date(System.currentTimeMillis());
//					
//				es.setExecuteCompletedAt(executeFinish);
//				es.setExecuteState(MappingState.EXECUTED);
//				es.setExecuteShards(outhandler.getShards());
////				es.setCount(outhandler.getTotalItems());
//				es.setCount(subjects.size());
//					
//				pavRepository.save(pav);
//		
//				SSEController.send("edits", applicationEventPublisher, this, new NotificationObject("execute",
//						MappingState.EXECUTED.toString(), pavId, null, executeStart, executeFinish, subjects.size()));
//
//				logger.info("Annotation edits executed -- id: " + pavId + ", shards: " + outhandler.getShards());
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
//				logger.info("Annotation edits failed -- id: " + pavId);
//				
//				exec.getMonitor().currentConfigurationFailed();
//
//				throw ex;
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//
//			es.setExecuteState(MappingState.EXECUTION_FAILED);
//
//			SSEController.send("edits", applicationEventPublisher, this,
//					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), pavId, null, null, null));
//
//			pavRepository.save(pav);
//
//			return false;
//		}
//	}

	public boolean executeNoDelete(UserPrincipal currentUser, String id, ApplicationEventPublisher applicationEventPublisher) throws Exception {

		Optional<PagedAnnotationValidation> odoc = pavRepository.findById(id);
	    if (!odoc.isPresent()) {
	    	logger.info("Paged Annotation Validation " + id + " not found");
	    	return false;
	    }
    	
	    PagedAnnotationValidation pav = odoc.get();
		ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
		VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

	    
	    Date executeStart = new Date(System.currentTimeMillis());
	    
    	ExecuteState es = pav.getExecuteState(fileSystemConfiguration.getId());
    	
    	String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + pav.getDatasetUuid() + "/";
    	File datasetFolderFile = new File(datasetFolder);
    	
		if (!datasetFolderFile.exists()) {
			datasetFolderFile.mkdir();
		}

		// Clearing old files
//		if (es.getExecuteState() == MappingState.EXECUTED) {
			try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(datasetFolderFile.toPath(), pav.getUuid() + "_*")) {
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
		
		pavRepository.save(pav);
		
		logger.info("Paged Annotation Validation " + id + " starting");
		
		try (FileSystemOutputHandler deleteHandler = new FileSystemOutputHandler(datasetFolder, pav.getUuid(), shardSize);
//				Writer delete = new OutputStreamWriter(new FileOutputStream(new File(datasetFolder + fav.getUuid() + "_delete.trig"), false), StandardCharsets.UTF_8);
//				Writer replaceCatalog = new OutputStreamWriter(new FileOutputStream(new File(datasetFolder + fav.getUuid() + "_replace_catalog.trig"), false), StandardCharsets.UTF_8)				
				) {
			
			Executor exec = new Executor(deleteHandler, safeExecute);
			exec.keepSubjects(true);
			
			try (ExecuteMonitor em = new ExecuteMonitor("paged-validation", id, null, applicationEventPublisher)) {
				exec.setMonitor(em);
				
				String addD2rml = env.getProperty("validator.paged-mark-add.d2rml");
				D2RMLModel addMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ addD2rml).getInputStream()) {
					addMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
				}

				String deleteD2rml = env.getProperty("validator.mark-delete.d2rml");
				D2RMLModel deleteMapping = null;
				try (InputStream inputStream = resourceLoader.getResource("classpath:"+ deleteD2rml).getInputStream()) {
					deleteMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
				}

				String onPropertyString = AnnotationEditGroup.onPropertyListAsString(pav.getOnProperty());
				String annfilter = AnnotationEditGroup.annotatorFilter("annotation", pav.getAnnotatorDocumentUuid());

				SSEController.send("paged-annotation-validation", applicationEventPublisher, this, new ExecuteNotificationObject(id, null,
						ExecutionInfo.createStructure(deleteMapping), executeStart));

				for (AnnotationEdit edit :  annotationEditRepository.findByPagedAnnotationValidationId(pav.getId())) {
				
					if (edit.getAddedByUserId().size() > 0 && edit.getAcceptedByUserId().size() + 1 >= edit.getRejectedByUserId().size()) {
						
						Map<String, Object> params = new HashMap<>();
						params.put("iigraph", SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString());
						params.put("iiproperty", onPropertyString);
						params.put("iivalue", edit.getOnValue().toString());
						params.put("iiannotation", edit.getAnnotationValue());
						params.put("iirdfsource", vc.getSparqlEndpoint());
						params.put("iiconfidence", "1");
						params.put("iiannotator", SEMAVocabulary.getAnnotationValidator(pav.getUuid()));
						params.put("validator", SEMAVocabulary.getAnnotationValidator(pav.getUuid()));
	
//						System.out.println(edit.getOnValue().toString());
						
						exec.partialExecute(addMapping, params);
					
					} else if (edit.getAddedByUserId().size() == 0 && edit.getAcceptedByUserId().size() < edit.getRejectedByUserId().size()) {

				    	String sparql = 
				    			"SELECT ?annotation " +
		     			        "WHERE { " + 
		    			        "  GRAPH <" + pav.getAsProperty() + "> { " + 
		    			        "    ?annotation <" + OAVocabulary.hasBody + "> <" + edit.getAnnotationValue() + "> . " +
		    			        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " +
							    annfilter +
		    			        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
		    			        "    ?target <" + SOAVocabulary.onValue + "> " + edit.getOnValue().toString() + " . " +
		    			        "    ?target <" + OAVocabulary.hasSource + "> ?source . " +
		    			        (edit.getStart() != -1 ? " ?target <" + SOAVocabulary.start + "> " + edit.getStart() + " . " : " FILTER NOT EXISTS { ?target <" + SOAVocabulary.start + "> " + edit.getStart() + " } . ") +
		    			        (edit.getEnd() != -1 ? " ?target <" + SOAVocabulary.end + "> " + edit.getEnd() + " . " : " FILTER NOT EXISTS { ?target <" + SOAVocabulary.end + "> " + edit.getEnd() + " } . ") +
		    		            "  } . " +	    			        
		    			        "  GRAPH <" + SEMAVocabulary.getDataset(pav.getDatasetUuid()).toString() + "> { " +
		    			        "    ?source " + onPropertyString + " " + edit.getOnValue().toString() + " } " +
		                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
		                        "    ?adocid <http://purl.org/dc/terms/hasPart> ?annotation . } " +		    			        
		    			        "}";
	
						Map<String, Object> params = new HashMap<>();
						params.put("iirdfsource", vc.getSparqlEndpoint());
						params.put("iisparql", sparql);
						params.put("validator", SEMAVocabulary.getAnnotationValidator(pav.getUuid()));
						
						exec.partialExecute(deleteMapping, params);
					}
				}	
				
				exec.completeExecution();
		
				Date executeFinish = new Date(System.currentTimeMillis());
					
				es.setExecuteCompletedAt(executeFinish);
				es.setExecuteState(MappingState.EXECUTED);
				es.setExecuteShards(deleteHandler.getShards());
				es.setCount(deleteHandler.getTotalItems());
					
				pavRepository.save(pav);
		
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

			pavRepository.save(pav);

			return false;
		}
	}	
	
//	public boolean publish(UserPrincipal currentUser, String pavId) throws Exception {
//		
//		Optional<PagedAnnotationValidation> odoc = pavRepository.findById(pavId);
//	    if (!odoc.isPresent()) {
//	    	return false;
//	    }
//		
//	    PagedAnnotationValidation pav = odoc.get();
//
//		PublishState ps = pav.getPublishState(virtuosoConfiguration.getDatabaseId());
//		
//		ps.setPublishState(DatasetState.PUBLISHING);
//		ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			
//		pavRepository.save(pav);
//			
////		List<AnnotationEdit> deletes = annotationEditRepository.findByPagedAnnotationValidationId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), AnnotationEditType.REJECT, adoc.getUserId());
//		List<AnnotationEdit> deletes = annotationEditRepository.findByPagedAnnotationValidationId();
//		
//		virtuosoJDBC.publish(currentUser, pav, deletes);
//	    	
//		ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//		ps.setPublishState(DatasetState.PUBLISHED);
//			
//		pavRepository.save(pav);
//		
//		logger.info("Paged annotation validation " + pavId + " publication completed.");
//		
//		return true;
//	}
	
//	public boolean unpublish(UserPrincipal currentUser, String pavId) throws Exception {
//		
//		Optional<PagedAnnotationValidation> odoc = pavRepository.findById(pavId);
//	    if (!odoc.isPresent()) {
//	    	return false;
//	    }
//    	
//	    PagedAnnotationValidation pav = odoc.get();
//	    
//		PublishState ps = pav.getPublishState(virtuosoConfiguration.getDatabaseId());
//	
//		ps.setPublishState(DatasetState.UNPUBLISHING);
//		ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
//		
//		pavRepository.save(pav);
//		
//		List<AnnotationEdit> adds = annotationEditRepository.findByPagedAnnotationValidationId(pav.getId()); // which criteria to add
//	
//		virtuosoJDBC.unpublish(currentUser, pav, adds);
//    	
//		ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//		ps.setPublishState(DatasetState.UNPUBLISHED);
//		
//		pavRepository.save(pav);
//		
//		logger.info("Paged annotation validation " + pavId + " unpublication completed.");
//		
//		return true;
//	}
	
	public boolean republishNoDelete(UserPrincipal currentUser, String id) throws Exception {
		return unpublishNoDelete(currentUser, id) && publishNoDelete(currentUser, id);
	}

	
	public boolean publishNoDelete(UserPrincipal currentUser, String id) throws Exception {
		
		Optional<PagedAnnotationValidation> doc = pavRepository.findById(new ObjectId(id));
	
		if (doc.isPresent()) {
			PagedAnnotationValidation pav = doc.get();
			ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
			VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			PublishState ps = pav.getPublishState(vc.getDatabaseId());
		
			ps.setPublishState(DatasetState.PUBLISHING);
			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
		
			pavRepository.save(pav);
			
			virtuosoJDBC.publish(currentUser, vc.getName(), pav);
	    	
			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			ps.setPublishState(DatasetState.PUBLISHED);
			
			pavRepository.save(pav);
		}
		
		return true;
	}

	public boolean unpublishNoDelete(UserPrincipal currentUser, String id) throws Exception {
		
		Optional<PagedAnnotationValidation> doc = pavRepository.findById(new ObjectId(id));
	
		if (doc.isPresent()) {
			PagedAnnotationValidation pav = doc.get();
			ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
			VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		
			PublishState ps = pav.getPublishState(vc.getDatabaseId());
		
			ps.setPublishState(DatasetState.UNPUBLISHING);
			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
			
			pavRepository.save(pav);
			
			virtuosoJDBC.unpublish(currentUser, vc.getName(), pav);
	    	
			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			ps.setPublishState(DatasetState.UNPUBLISHED);
			
			pavRepository.save(pav);
		}
		
		return true;
	}
	
//	public Optional<String> getLastExecution(UserPrincipal currentUser, String pavId) throws Exception {
//		Optional<PagedAnnotationValidation> entry = pavRepository.findById(new ObjectId(pavId));
//		
//		if (entry.isPresent()) {
//			return Optional.empty();
//		}
//			
//		PagedAnnotationValidation pav = entry.get();
//
//		String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + pav.getDatasetUuid() + "/";
//		
//		StringBuffer result = new StringBuffer();
//			
//		result.append(">> ADD    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//		result.append(new String(Files.readAllBytes(Paths.get(datasetFolder + pav.getUuid().toString() + "_add.trig"))));
//		result.append("\n");
//		result.append(">> DELETE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//		result.append(new String(Files.readAllBytes(Paths.get(datasetFolder + pav.getUuid().toString() + "_delete.trig"))));
//
//		return Optional.of(result.toString());
//	}


	public Optional<String> getLastExecution(UserPrincipal currentUser, String favId) throws Exception {
		Optional<PagedAnnotationValidation> entry = pavRepository.findById(new ObjectId(favId));
		
		if (!entry.isPresent()) {
			return Optional.empty();
		}
			
		PagedAnnotationValidation pav = entry.get();

		String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + pav.getDatasetUuid() + "/";
		
		StringBuffer result = new StringBuffer();
			
		result.append(new String(Files.readAllBytes(Paths.get(datasetFolder + pav.getUuid().toString() + ".trig"))));

		return Optional.of(result.toString());
	}
}
