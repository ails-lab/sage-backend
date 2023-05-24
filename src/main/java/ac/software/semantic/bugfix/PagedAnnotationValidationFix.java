package ac.software.semantic.bugfix;

import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.ANNOTATED_ONLY;
import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.UNANNOTATED_ONLY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationPageRepository;
import ac.software.semantic.service.AnnotationEditGroupService;
import ac.software.semantic.service.PagedAnnotationValidationService;
import ac.software.semantic.service.SchemaService;
import ac.software.semantic.service.ValueCount;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class PagedAnnotationValidationFix {

    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;
			
	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private LegacyVocabulary legacyVocabulary;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	AnnotatorDocumentRepository annotatorDocumentRepository;

    @Autowired
	AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	PagedAnnotationValidationRepository pavRepository;

	@Autowired
	PagedAnnotationValidationPageRepository pavpRepository;
	
	@Autowired
	AnnotationEditRepository annotationEditRepository;

	@Autowired
	PagedAnnotationValidationService pavService;

	@Autowired
	AnnotationEditGroupService aegService;
	
	@Autowired
	DatasetRepository datasetRepository;
	
	public void applyFix() {
		// should by executed once . Modifies mondgo
//		updatePagedAnnotationValidations(); 
		
		List<PagedAnnotationValidation[]> pavs = findProblematicPagedAnnotationValidations();
		
		List<PagedAnnotationValidationPage> newPavs = new ArrayList<>(); 

		for (int i = 0; i < pavs.size(); i++) {

			PagedAnnotationValidation oldPav = pavs.get(i)[0];
			PagedAnnotationValidation newPav = pavs.get(i)[1];

			Dataset dataset = datasetRepository.findByUuid(oldPav.getDatasetUuid()).get();
			System.out.println("UPDATING " + dataset.getName() + " / " + PathElement.onPathStringListAsSPARQLString(oldPav.getOnProperty()) + " " + oldPav.getAsProperty());

			oldPav.setAnnotatedPagesCount(newPav.getAnnotatedPagesCount());
			oldPav.setAnnotationsCount(newPav.getAnnotationsCount());
			oldPav.setNonAnnotatedPagesCount(newPav.getNonAnnotatedPagesCount());
			
			//update paged annotation validation in Mongo
//			pavRepository.save(oldPav);
			
			//delete related paged annotation validation pages
//			pavpRepository.deleteByPagedAnnotationValidationId(oldPav.getId());

			List<AnnotationEdit> edits = annotationEditRepository.findByAnnotationEditGroupId(oldPav.getAnnotationEditGroupId());

			// take all existing edits
			Set<String> editIds = edits.stream().map(edit -> edit.getId().toString()).collect(Collectors.toSet());
			
			System.out.println("EXISTING EDITS: " + editIds.size());
					
			if (editIds.size() > 0) {
				for (int j = 1; j <= newPav.getAnnotatedPagesCount(); j++ ) {
//					System.out.println("ATTEMPTING ANN PAGE: " + j + " : " + editIds.size());
					if (editIds.isEmpty()) {
						break;
					}
					
					PagedAnnotationValidationPage pavp = viewTMP(oldPav, AnnotationValidationRequest.ANNOTATED_ONLY, j, editIds);
					if (pavp != null) {
						System.out.println("CREATED PAGE: " + pavp.getMode() + " " + pavp.getPage() + " " + pavp.getAnnotationsCount() + " " + pavp.getValidatedCount() + " " + pavp.getUnvalidatedCount() + " " + pavp.getAddedCount() + " / " + editIds.size());
						newPavs.add(pavp);
					}
				}
				
				for (int j = 1; j <= newPav.getNonAnnotatedPagesCount(); j++ ) {
//					System.out.println("ATTEMPTING NOT ANN PAGE: " + j + " : " + editIds.size());				
					if (editIds.isEmpty()) {
						break;
					}
					
					PagedAnnotationValidationPage pavp = viewTMP(oldPav, AnnotationValidationRequest.UNANNOTATED_ONLY, j, editIds);
					if (pavp != null) {
						System.out.println("CREATED PAGE: " + pavp.getMode() + " PAGE " + pavp.getPage() + " : " + pavp.getAnnotationsCount() + " " + pavp.getValidatedCount() + " " + pavp.getUnvalidatedCount() + " " + pavp.getAddedCount() + " / " + editIds.size());
						newPavs.add(pavp);
					}
					
				}
				
				if (editIds.size() > 0) {
					System.out.println("MISSED " + editIds);
				}
			}			
		}

		// save all pages
//		for (PagedAnnotationValidationPage page : newPavs)  {
//			pavpRepository.save(page); 
//		}

	}
	
	// Add annotation document uuids to paged annotation validations in Mongo
	public void updatePagedAnnotationValidations() {
		for (PagedAnnotationValidation pav : pavRepository.findAll()) {
			
			List<String> adocs = annotatorDocumentRepository.findByAnnotatorEditGroupId(pav.getAnnotationEditGroupId())
					.stream().map(adoc -> adoc.getUuid())
					.collect(Collectors.toList());
			
			pav.setAnnotatorDocumentUuid(adocs);
			
			pavRepository.save(pav);
		}
	}
	
	// Find paged annotation validations that need fixing
	public List<PagedAnnotationValidation[]> findProblematicPagedAnnotationValidations() {
		List<PagedAnnotationValidation[]> pavList = new ArrayList<>();		
		
		for (PagedAnnotationValidation pav : pavRepository.findAll()) {
			Dataset dataset = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
			
			ObjectId aegId = pav.getAnnotationEditGroupId();
			
			boolean change = false;
			PagedAnnotationValidation pav2 = createPagedAnnotationValidationTMP(pav.getUserId(), aegId.toString(), false);
			if (pav.getAnnotatedPagesCount() != pav2.getAnnotatedPagesCount() ||
				pav.getAnnotationsCount() != pav2.getAnnotationsCount() || 
				pav.getNonAnnotatedPagesCount() != pav2.getNonAnnotatedPagesCount()) {
				change = true;
			}
			
			pav2 = createPagedAnnotationValidationTMP(pav.getUserId(), aegId.toString(), true);
			if (pav.getAnnotatedPagesCount() != pav2.getAnnotatedPagesCount() ||
					pav.getAnnotationsCount() != pav2.getAnnotationsCount() || 
					pav.getNonAnnotatedPagesCount() != pav2.getNonAnnotatedPagesCount()) {
					change = true;
			}
			
			if (change) {
				System.out.println("NEEDS FIX: " + dataset.getName() + " : " + pav.getOnProperty() + " " + pav.getAsProperty() + " : " + pav.getAnnotatedPagesCount() + "/" + pav2.getAnnotatedPagesCount() + " " + pav.getNonAnnotatedPagesCount() + "/" + pav2.getNonAnnotatedPagesCount() + " " + pav.getAnnotationsCount() +"/" + pav2.getAnnotationsCount());

				pavList.add(new PagedAnnotationValidation[] { pav, pav2 });
			}
		}
		
		return pavList ;
	}
	
	public PagedAnnotationValidation createPagedAnnotationValidationTMP(ObjectId userId, String aegId, boolean withAnns) {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));

		PagedAnnotationValidation pav = null;
		
		try {
			AnnotationEditGroup aeg = aegOpt.get();
			ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(aeg.getDatasetUuid()).get();
			TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			List<String> annotatorUuids = annotatorDocumentRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList());
			String annfilter = aegService.annotatorFilter("v", annotatorUuids);
			
			if (!withAnns) {
				annfilter = "";
			}

			pav = new PagedAnnotationValidation();
			pav.setUserId(userId);
			pav.setAnnotationEditGroupId(aeg.getId());
			pav.setDatasetUuid(aeg.getDatasetUuid());
			pav.setDatabaseId(database.getId());
			pav.setOnProperty(aeg.getOnProperty());
			pav.setAsProperty(aeg.getAsProperty());
			pav.setAnnotatorDocumentUuid(annotatorUuids);
			pav.setComplete(false);
	
			String datasetUri = resourceVocabulary.getDatasetAsResource(aeg.getDatasetUuid()).toString();
			String spath = PathElement.onPathStringListAsSPARQLString(aeg.getOnProperty());
	
//			logger.info("Starting paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ".");
			
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
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } ";
	
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
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } }";
	
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
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } ";
	
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
			
//			logger.info("Paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ": valueCount=" + annotatedValueCount + "/" + nonAnnotatedValueCount + " pages=" + annotatedPages + "/" + nonAnnotatedPages);
	
			pav.setPageSize(pageSize);
			pav.setAnnotationsCount(annotationsCount);
			pav.setAnnotatedPagesCount(annotatedPages);
			pav.setNonAnnotatedPagesCount(nonAnnotatedPages);
			
			return pav;
		} catch (Exception ex) {
			ex.printStackTrace();
			
			return null;
		}
	}	
	
	public PagedAnnotationValidationPage viewTMP(PagedAnnotationValidation pav, AnnotationValidationRequest mode, int page, Set<String> editIds) {
		String datasetUri = resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString();
		DatasetCatalog dcg = schemaService.asCatalog(pav.getDatasetUuid());
		String fromClause = schemaService.buildFromClause(dcg);

		String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());
		String annfilter = aegService.annotatorFilter("v", pav.getAnnotatorDocumentUuid());

		ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
		TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		List<ValueCount> values = pavService.getValuesForPage(vc, datasetUri, pav.getMode(), onPropertyString, pav.getAsProperty(), pav.getAnnotatorDocumentUuid(), mode, page, fromClause);
		

    	Map<RDFNode, Integer> countMap = new HashMap<>();
    	
    	StringBuffer sb = new StringBuffer();
    	for (ValueCount vcc : values) {
			if (vcc.getValue().isLiteral()) {
				Literal l = vcc.getValue().asLiteral();
				String lf = l.getLexicalForm();
				
				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
				sb.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString());
			} else {
				sb.append("<" + vc.toString() + ">");
			}
    		
    		sb.append(" ");
    		
    		countMap.put(vcc.getValue(), vcc.getCount());
    	}
    	
    	String valueString = sb.toString();
    	
//    	System.out.println(valueString);
    	
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
		    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start }  " + 
		    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } } ";
		    		
		if (mode == ANNOTATED_ONLY) {
			sparql = 
					"SELECT distinct ?value ?t ?ie ?start ?end  (count(*) AS ?count)" +
					fromClause + 
					"FROM NAMED <" + pav.getAsProperty() + "> " +
		            "WHERE { " + 
		            "  ?s " + onPropertyString + " ?value  " + 
                       graph +  
                    "  VALUES ?value { " + valueString  + " } " +                       
		            "} " + 
		            "GROUP BY ?t ?ie ?value ?start ?end " +
					"ORDER BY DESC(?count) ?value ?start ?end";
		} else if (mode == UNANNOTATED_ONLY) {
			sparql = 
					"SELECT distinct ?value (count(*) AS ?count) " +
					fromClause + 
					"FROM NAMED <" + pav.getAsProperty() + "> " +
			        "WHERE { " + 
					"  ?s " + onPropertyString + " ?value " + 
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
    	
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

		int totalCount = 0;
		if (valueString.length() > 0) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
			
				ResultSet rs = qe.execSelect();
				
				AnnotationEditValue prev = null;
				ValueAnnotation va = null;
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					RDFNode value = sol.get("value");
					
					String ann = sol.get("t") != null ? sol.get("t").toString() : null;
					Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
					Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
	//				int count = sol.get("count").asLiteral().getInt();
	//				
	//				if (ann != null) {
	//					totalCount += count;
	//				}
	//				
					String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
	
					AnnotationEditValue aev = null;
					if (value.isResource()) {
						aev = new AnnotationEditValue(value.asResource());
					} else if (value.isLiteral()) {
						aev = new AnnotationEditValue(value.asLiteral());
					}
	//				aev.setCount(countMap.get(value));
					totalCount += countMap.get(value); // totalCount will be not correct if multiple identical annotations exist for the same value 
					                                   // or if not all identical values have the same annotations.
					                                   // the assumption is that the same values have all the same annotations appearing once.
					
					if (!aev.equals(prev)) {
						if (prev != null) {
							res.put(prev, va);
						}
	
						prev = aev;
						
						va = new ValueAnnotation();
						va.setOnValue(aev);
						va.setCount(countMap.get(value));
							
						if (ann != null) {
							ValueAnnotationDetail vad  = new ValueAnnotationDetail();
							vad.setValue(ann);
							vad.setValue2(ie);
							vad.setStart(start);
							vad.setEnd(end);
							
							va.getDetails().add(vad);
						}
					} else {
						ValueAnnotationDetail vad  = new ValueAnnotationDetail();
						vad.setValue(ann);
						vad.setValue2(ie);
						vad.setStart(start);
						vad.setEnd(end);
						
						va.getDetails().add(vad);
					}
					
				}
				if (prev != null) {
					res.put(prev, va);
				}
			}
		}
		
//		ObjectId uid = new ObjectId(currentUser.getId());
				
		int validatedCount = 0;
		int addedCount = 0;
		
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
				
				if (editOpt.isPresent()) {
					AnnotationEdit edit = editOpt.get();
					
					if (edit.getAcceptedByUserId().size() > 0 || edit.getRejectedByUserId().size() > 0) {
						validatedCount += nva.getCount();
					}
					
					set.add(edit.getId());
					editIds.remove(edit.getId().toString());
				}
			}
			
			List<AnnotationEdit> edits = null;
			if (aev.getIri() != null) {
				edits = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAdded(pav.getAnnotationEditGroupId(), aev.getIri());
			} else {
				edits = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAdded(pav.getAnnotationEditGroupId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype());
			}

//			System.out.println(">>>1 "+ pav.getId() + " " + aev.getLexicalForm() + " " + aev.getLanguage() + " " + aev.getDatatype());
			for (AnnotationEdit edit : edits) {
				
//				System.out.println(">>>1 "+ edit.getId());

				if (set.contains(edit.getId())) {
					continue;
				}
				
				addedCount += nva.getCount();
				
				editIds.remove(edit.getId().toString());
			}
		}	
		
		if (validatedCount > 0 || addedCount > 0) {
			PagedAnnotationValidationPage pavp = new PagedAnnotationValidationPage();
			pavp.setPagedAnnotationValidationId(pav.getId());
			pavp.setAnnotationEditGroupId(pav.getAnnotationEditGroupId());
			pavp.setMode(mode);
			pavp.setPage(page);
			pavp.setAnnotationsCount(totalCount);
			pavp.setValidatedCount(validatedCount);
			pavp.setUnvalidatedCount(totalCount - validatedCount);
			pavp.setAddedCount(addedCount);
			pavp.setAssigned(false);
			
			return pavp;
		} else {
			return null;
		}
		
		
	}
}
