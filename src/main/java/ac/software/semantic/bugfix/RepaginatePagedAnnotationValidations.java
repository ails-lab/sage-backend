package ac.software.semantic.bugfix;

import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.ANNOTATED_ONLY;
import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.UNANNOTATED_ONLY;

import java.util.ArrayList;
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
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import ac.software.semantic.vocs.LegacyVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;

@Service
public class RepaginatePagedAnnotationValidations {

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
	
	// call this to repaginate all paged annotation validations
	public void repaginatePagedAnnotationValidations() {

		//delete all pages
		pavpRepository.deleteAll();
		
		List<PagedAnnotationValidation> pavs = pavRepository.findAll();

		for (PagedAnnotationValidation oldPav : pavs) {
			Dataset dataset = datasetRepository.findByUuid(oldPav.getDatasetUuid()).get();
			
			ObjectId aegId = oldPav.getAnnotationEditGroupId();
			
			// recreate paged annotation validation
			PagedAnnotationValidation newPav = createPagedAnnotationValidationTMP(oldPav.getUserId(), aegId.toString());

			System.out.println();
			System.out.println("UPDATING : " + dataset.getName() + " : " + oldPav.getOnProperty() + " " + oldPav.getAsProperty() + " : " + oldPav.getAnnotatedPagesCount() + "/" + newPav.getAnnotatedPagesCount() + " " + oldPav.getNonAnnotatedPagesCount() + "/" + newPav.getNonAnnotatedPagesCount() + " " + oldPav.getAnnotationsCount() +"/" + newPav.getAnnotationsCount());

			oldPav.setAnnotatedPagesCount(newPav.getAnnotatedPagesCount());
			oldPav.setAnnotationsCount(newPav.getAnnotationsCount());
			oldPav.setNonAnnotatedPagesCount(newPav.getNonAnnotatedPagesCount());
//			
//			//update paged annotation validation in Mongo
			pavRepository.save(oldPav);
//			
			// create paged form edits
			List<AnnotationEdit> edits = annotationEditRepository.findByPagedAnnotationValidationId(oldPav.getId());
			if (edits.size() == 0) {
				continue;
			}

			Set<String> editIds = edits.stream().map(edit -> edit.getId().toString()).collect(Collectors.toSet());
			
			System.out.println("EXISTING EDITS: " + edits.size());
					
			if (editIds.size() > 0) {
				for (int j = 1; j <= Math.max(newPav.getAnnotatedPagesCount(),newPav.getNonAnnotatedPagesCount()); j++ ) {
				
					if (editIds.isEmpty()) {
						break;
					}

					if (j <= newPav.getAnnotatedPagesCount()) {
						int before = editIds.size();
						PagedAnnotationValidationPage pavp = view(oldPav, AnnotationValidationRequest.ANNOTATED_ONLY, j, editIds);
						if (before > editIds.size()) {
							System.out.println("CREATED PAGE: " + pavp.getMode() + " PAGE " + pavp.getPage() + " : " + pavp.getAnnotationsCount() + " " + pavp.getValidatedCount() + " " + pavp.getUnvalidatedCount() + " " + pavp.getAddedCount() + " / " + editIds.size());
							pavpRepository.save(pavp);
						}
					}

					if (editIds.isEmpty()) {
						break;
					}

					if (j <= newPav.getNonAnnotatedPagesCount()) {
						int before = editIds.size();
						PagedAnnotationValidationPage pavp = view(oldPav, AnnotationValidationRequest.UNANNOTATED_ONLY, j, editIds);
						if (before > editIds.size()) {
							System.out.println("CREATED PAGE: " + pavp.getMode() + " PAGE " + pavp.getPage() + " : " + pavp.getAnnotationsCount() + " " + pavp.getValidatedCount() + " " + pavp.getUnvalidatedCount() + " " + pavp.getAddedCount() + " / " + editIds.size());
							pavpRepository.save(pavp);
						}
					}					
				}

				if (editIds.size() > 0) {
					System.out.println("MISSED " + editIds);
				}
			}			
		}

	}
	
	public PagedAnnotationValidation createPagedAnnotationValidationTMP(ObjectId userId, String aegId) {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));

		PagedAnnotationValidation pav = null;
		
		try {
			AnnotationEditGroup aeg = aegOpt.get();
			ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(aeg.getDatasetUuid()).get();
			TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			List<String> annotatorUuids = annotatorDocumentRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(adoc -> adoc.getUuid()).collect(Collectors.toList());
			String annfilter = aegService.annotatorFilter("v", annotatorUuids);
			
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
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } " +
			        "  FILTER (isLiteral(?value)) " + 
			        " } ";
	
			int annotatedValueCount = 0;
			
//			System.out.println(QueryFactory.create(annotatedCountSparql, Syntax.syntaxSPARQL_11));
	
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
					"  FILTER NOT EXISTS { GRAPH <" + aeg.getAsProperty() + "> { " + 
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
	
	public class ValueCount {
		private RDFNode value;
		private int count;
		
		public ValueCount(RDFNode value, int count) {
			this.value = value;
			this.count = count;
		}
		
		public RDFNode getValue() {
			return value;
		}
		
		public int getCount() {
			return count;
		}
		
	}
	
	public List<ValueCount> getValuesForPage(TripleStoreConfiguration vc, String datasetUri, String onPropertyString, String asProperty, List<String> annotatorUuids, AnnotationValidationRequest mode, int page) {
		String annfilter = aegService.annotatorFilter("v", annotatorUuids);
		
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

	public PagedAnnotationValidationPage view(PagedAnnotationValidation pav, AnnotationValidationRequest mode, int page, Set<String> editIds) {
		
		String datasetUri = resourceVocabulary.getDatasetAsResource(pav.getDatasetUuid()).toString();
    	String onPropertyString = PathElement.onPathStringListAsSPARQLString(pav.getOnProperty());
		String annfilter = aegService.annotatorFilter("v", pav.getAnnotatorDocumentUuid());

		ac.software.semantic.model.Dataset ds = datasetRepository.findByUuid(pav.getDatasetUuid()).get();
		TripleStoreConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

    	List<ValueCount> values = getValuesForPage(vc, datasetUri, onPropertyString, pav.getAsProperty(), pav.getAnnotatorDocumentUuid(), mode, page);
    	
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

    	StringBuffer sb = new StringBuffer();
    	for (ValueCount vcc : values) {
			AnnotationEditValue aev = null;
    		
    		if (vcc.getValue().isLiteral()) {
				Literal l = vcc.getValue().asLiteral();
				String lf = l.getLexicalForm();
				
				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
				sb.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString());
	    		sb.append(" ");
				
				aev = new AnnotationEditValue(vcc.getValue().asLiteral());
			}
    		
    		if (aev != null) {
				ValueAnnotation va = new ValueAnnotation();
				va.setOnValue(aev);
				va.setCount(vcc.getCount()); // the number of appearances of the value
				
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
		    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.start) + "> ?start }  " + 
		    " OPTIONAL { ?r <" + legacyVocabulary.fixLegacy(OAVocabulary.end) + "> ?end } } ";
		    		
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

					Integer start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : null;
					Integer end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : null;
					
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
		
		PagedAnnotationValidationPage pavp = new PagedAnnotationValidationPage();
		pavp.setPagedAnnotationValidationId(pav.getId());
		pavp.setAnnotationEditGroupId(pav.getAnnotationEditGroupId());
		pavp.setMode(mode);
		pavp.setPage(page);
		pavp.setAnnotationsCount(totalAnnotationsCount);
		
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
				
				if (editOpt.isPresent()) {
					AnnotationEdit edit = editOpt.get();
					
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
					editIds.remove(edit.getId().toString());
				}

			}

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

				editIds.remove(edit.getId().toString());
			}
		}		

		
		pavp.setValidatedCount(validatedCount);
		pavp.setUnvalidatedCount(totalAnnotationsCount - validatedCount);
		pavp.setAddedCount(addedCount);
		pavp.setAcceptedCount(acceptedCount);
		pavp.setRejectedCount(rejectedCount);
		pavp.setNeutralCount(neutralCount);
		pavp.setAssigned(false);

		return pavp;
	}
	
}
