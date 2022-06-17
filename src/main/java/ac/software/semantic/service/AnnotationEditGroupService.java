package ac.software.semantic.service;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import ac.software.semantic.model.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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
import org.elasticsearch.search.suggest.term.TermSuggestion.Score;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.controller.ExecuteMonitor;
import ac.software.semantic.controller.SSEController;
import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.payload.AnnotationEditGroupResponse;
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
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.d2rml.monitor.FileSystemOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SOAVocabulary;
import io.jsonwebtoken.lang.Collections;

@Service
public class AnnotationEditGroupService {

	Logger logger = LoggerFactory.getLogger(AnnotationEditGroupService.class);

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;
			
	@Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private AnnotationEditRepository annotationEditRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	AnnotatorDocumentRepository annotatorRepository;
	
	@Autowired
	AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	PagedAnnotationValidationRepository pavRepository;

	@Autowired
	FilterAnnotationValidationRepository favRepository;

	@Autowired
	PagedAnnotationValidationRepositoryPage pavpRepository;
	
	public List<AnnotationEditGroupResponse> getAnnotationEditGroups(UserPrincipal currentUser, String datasetUri) {

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		List<AnnotationEditGroup> docs = aegRepository.findByDatasetUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		List<AnnotationEditGroupResponse> response = docs.stream()
				.map(doc -> modelMapper.annotationEditGroup2AnnotationEditGroupResponse(virtuosoConfigurations.values(), doc, pavRepository.findByAnnotationEditGroupId(doc.getId()), favRepository.findByAnnotationEditGroupId(doc.getId())))
				.collect(Collectors.toList());

		return response;
	}

	public List<AnnotationEditGroupResponse> getAnnotationEditGroups(String datasetUri) {

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		List<AnnotationEditGroup> docs = aegRepository.findByDatasetUuid(datasetUuid);

		List<AnnotationEditGroupResponse> response = docs.stream()
				.map(doc -> modelMapper.annotationEditGroup2AnnotationEditGroupResponse(virtuosoConfigurations.values(), doc, pavRepository.findByAnnotationEditGroupId(doc.getId()), favRepository.findByAnnotationEditGroupId(doc.getId())))
				.collect(Collectors.toList());

		return response;
	}

	
	public ByteArrayResource downloadAnnotationValues(UserPrincipal currentUser, String id, String mode) throws Exception {

		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(id));
		if (!aegOpt.isPresent()) {
			return null;
		}		
		
		AnnotationEditGroup aeg = aegOpt.get();
		
		Optional<ac.software.semantic.model.Dataset> dopt = datasetRepository.findByUuid(aeg.getDatasetUuid());
		VirtuosoConfiguration vc = dopt.get().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		List<String> generatorIds = new ArrayList<>();
		generatorIds.addAll(annotatorRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(doc -> "<" + SEMAVocabulary.getAnnotator(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
		generatorIds.addAll(pavRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> "<" + SEMAVocabulary.getAnnotationValidator(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));
		generatorIds.addAll(favRepository.findByAnnotationEditGroupId(aeg.getId()).stream().map(doc -> "<" + SEMAVocabulary.getAnnotationValidator(doc.getUuid()).toString() + "> ").collect(Collectors.toList()));

		String annfilter = AnnotationEditGroup.generatorFilter("annotation", generatorIds);
		
		String valFilter = "";
		if (mode.equalsIgnoreCase("NON_DELETED")) {
			valFilter = " FILTER NOT EXISTS { ?annotation <" + SOAVocabulary.hasValidation + "> [ <" + SOAVocabulary.action + "> <" + SOAVocabulary.Delete + "> ] }. ";
		} else {
			valFilter = " FILTER NOT EXISTS { ?annotation <" + SOAVocabulary.hasValidation + "> [ <" + SOAVocabulary.action + "> <" + SOAVocabulary.Add + "> ] }. ";
		}
	
    	String onPropertyString = aeg.getOnPropertyAsString();

//		String sparql = "SELECT ?source ?body ?body2 ?score WHERE { " + 
//    	String sparql = "SELECT ?source ?body (max(?score) as ?mscore) WHERE { " +
    	String sparql = "SELECT ?source ?body ?score ?value WHERE { " +
		                "  GRAPH <" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + "> { " + 
				        "    ?source " + onPropertyString + " ?value }  " +
				        "  GRAPH <" + aeg.getAsProperty() + "> { " + 
				        "    ?annotation a <" + OAVocabulary.Annotation + ">  . " + 
				        "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
				             annfilter + 
		                     valFilter +
		                "    ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) . " +
//				        "    { ?annotation <" + OAVocabulary.hasBody + "> ?body . FILTER (!isBlank(?body)) } UNION " + 
//				        "    { ?annotation <" + OAVocabulary.hasBody + "> [ " + 
//				        "         a <" + OWLTime.DateTimeInterval + "> ; " + 
//				        "         <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?body ; " + 
//				        "         <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?body2 ]  }  " +
				        "    OPTIONAL {?annotation <" + SOAVocabulary.score + "> ?score . } " +
				        "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
				        "    ?target <" + SOAVocabulary.onValue + "> ?value . " + 
				        "    ?target <" + OAVocabulary.hasSource + "> ?source . } } " +
//				        "GROUP BY ?source ?body ";
				        "";

//    	System.out.println(sparql);
    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
		
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
				Writer writer = new BufferedWriter(new OutputStreamWriter(bos));
				CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Item", "AnnotationIRI", "AnnotationLiteral", "Score", "SourceLiteralLexicalForm", "SourceLiteralLanguage", "SourceProperty"));
				ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			
			try (VirtuosoSelectIterator vs = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {
				while (vs.hasNext()) {
					QuerySolution sol = vs.next();
					Resource sid = sol.get("source").asResource();
					RDFNode node1 = sol.get("body");
//					RDFNode node2 = sol.get("body2");
					RDFNode score = sol.get("score");
					RDFNode value = sol.get("value");

					List<Object> line = new ArrayList<>();
					line.add(sid);
					
					if (node1.isResource()) {
						line.add(node1.asResource());
						line.add(null);
					} else if (node1.isLiteral()) {
						line.add(null);
						line.add(NodeFactory.createLiteralByValue(Utils.escapeJsonTab(node1.asLiteral().getLexicalForm()), node1.asLiteral().getLanguage(), node1.asLiteral().getDatatype()));
					}
					
					if (score != null) {
						line.add(score.asLiteral().getDouble());
					} else {
						line.add(null);
					}
					
					if (value != null) {
						line.add(value.asLiteral().getLexicalForm());
						line.add(value.asLiteral().getLanguage());
						line.add(onPropertyString);
					} else {
						line.add(null);
						line.add(null);
						line.add(null);
					}
					

//					if (node2 != null) {
//						if (node2.isResource()) {
//							line.add(node2.asResource());
//							line.add(null);
//						} else if (node2.isLiteral()) { 
//							line.add(null);
//							line.add(NodeFactory.createLiteralByValue(Utils.escapeJsonTab(node2.asLiteral().getLexicalForm()), node2.asLiteral().getLanguage(), node2.asLiteral().getDatatype()));
//						}
//					}
					
					csvPrinter.printRecord(line);
				}
				
			}

			csvPrinter.flush();
			bos.flush();
			
			try (ZipOutputStream zos = new ZipOutputStream(baos)) {
				ZipEntry entry = new ZipEntry(aeg.getUuid() + ".csv");

				zos.putNextEntry(entry);
				zos.write(bos.toByteArray());
				zos.closeEntry();

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}

			return new ByteArrayResource(baos.toByteArray());
		}
	
	}
	
	private VirtuosoConfiguration getPublishVirtuosoConfiguration(String datasetUuid) {
	
		Optional<ac.software.semantic.model.Dataset> dataset = datasetRepository.findByUuid(datasetUuid);
		for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site    	
			if (dataset.get().checkPublishState(vc.getId()) != null) {
				return vc;
			}
		}
		
		return null;
	}
	

	public Collection<ValueAnnotation> view(UserPrincipal currentUser, String aegId, AnnotationValidationRequest mode, int page, String annotators) {
		
		Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(new ObjectId(aegId));
		if (!aegOpt.isPresent()) {
			return new HashSet<>();
		}		
		
		AnnotationEditGroup aeg = aegOpt.get();

		String datasetUri = SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString();
    	String spath = aeg.getOnPropertyAsString();
    	
//		String annfilter = "";
//		if (annotators != null && annotators.length() > 0) {
//			for (String uuid : annotators.split(",")) {
//				if (uuid.length() == 36) { // quick fix for compatibility for old annotations that do not have annotator
//											// uuid;
//					annfilter += "<" + SEMAVocabulary.getAnnotator(uuid).toString() + "> ";
//				}
//			}
//
//			if (annfilter.length() > 0) {
//				annfilter = "?v <https://www.w3.org/ns/activitystreams#generator> ?annotator . VALUES ?annotator { "
//						+ annfilter + " } . ";
//			}
//		}
		String annfilter = AnnotationEditGroup.annotatorFilter("v", annotatorRepository.findByAnnotatorEditGroupId(aeg.getId()).stream().map(doc -> doc.getUuid()).collect(Collectors.toList()));

//    	System.out.println("SCHEMA");
//    	System.out.println(uri);
//    	System.out.println(asUri);
//    	System.out.println(path);

		String sparql = null;
		if (mode == AnnotationValidationRequest.ALL) {
			sparql = 
					"SELECT distinct ?value ?t ?ie ?start ?end  (count(*) AS ?count)" + 
			        "WHERE { " + "  GRAPH <" + datasetUri + "> { " + 
				    "    ?s " + spath + " ?value }  " + 
			        "  OPTIONAL { GRAPH <" + aeg.getAsProperty() + "> { " + 
				    "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
			        "     <" + OAVocabulary.hasTarget + "> ?r . " + 
				    annfilter + 
				    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
				    " { ?v <" + OAVocabulary.hasBody + "> [ " + 
				    " a <" + OWLTime.DateTimeInterval + "> ; " + 
				    " <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
				    " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " + 
				    "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
				    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
				    "     <" + OAVocabulary.hasSource + "> ?s . " + 
				    " OPTIONAL { ?r <" + SOAVocabulary.start + "> ?start } . " + 
				    " OPTIONAL { ?r <" + SOAVocabulary.end + "> ?end } . " + " } } }" + 
				    "GROUP BY ?t ?ie ?value ?start ?end " +
					// "ORDER BY DESC(?count) ?value ?start ?end LIMIT 50 OFFSET " + 50*(page - 1);
					"ORDER BY ?value ?start ?end LIMIT 50 OFFSET " + 50 * (page - 1);
		} else if (mode == AnnotationValidationRequest.ANNOTATED_ONLY) {
			sparql = 
					"SELECT distinct ?value ?t ?ie ?start ?end  (count(*) AS ?count)" + 
		            "WHERE { " + 
					"  GRAPH <" + datasetUri + "> { " + 
		            "    ?s " + spath + " ?value }  " + 
					"  GRAPH <" + aeg.getAsProperty() + "> { " + 
		            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
					"     <" + OAVocabulary.hasTarget + "> ?r . " + 
		            annfilter + 
		            " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
		            " { ?v <" + OAVocabulary.hasBody + "> [ " + 
		            " a <" + OWLTime.DateTimeInterval + "> ; " + 
		            " <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
		            " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " + 
		            "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
		            "     <" + SOAVocabulary.onValue + "> ?value ; " + 
		            "     <" + OAVocabulary.hasSource + "> ?s . " + 
		            " OPTIONAL { ?r <" + SOAVocabulary.start + "> ?start } . " + 
		            " OPTIONAL { ?r <" + SOAVocabulary.end + "> ?end } . " + "   } } " + 
		            "GROUP BY ?t ?ie ?value ?start ?end " +
					// "ORDER BY DESC(?count) ?value ?start ?end LIMIT 50 OFFSET " + 50*(page - 1);
					"ORDER BY ?value ?start ?end LIMIT 50 OFFSET " + 50 * (page - 1);
		} else if (mode == AnnotationValidationRequest.UNANNOTATED_ONLY) {
			sparql = 
					"SELECT distinct ?value (count(*) AS ?count) " + "WHERE { " + 
		            "  GRAPH <" + datasetUri + "> { " + 
					"    ?s " + spath + " ?value }  " + 
		            " FILTER NOT EXISTS { GRAPH <" + aeg.getAsProperty() + "> { " + 
					"  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		            "     <" + OAVocabulary.hasTarget + "> ?r . " + 
					annfilter + 
					"  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
					"     <" + SOAVocabulary.onValue + "> ?value ; " + 
					"     <" + OAVocabulary.hasSource + "> ?s  } } }" + 
					"GROUP BY ?value  " + "ORDER BY ?value LIMIT 50 OFFSET " + 50 * (page - 1);
		}    	
//    	System.out.println(sparql);
    	
    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

		//grouping does not work well with paging!!! annotations of some group may be split in different pages
		//it should be fixed somehow;
		//also same blank node annotation are repeated
		
		VirtuosoConfiguration vc = getPublishVirtuosoConfiguration(aeg.getDatasetUuid());
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
		
			ResultSet rs = qe.execSelect();
			
			AnnotationEditValue prev = null;
			ValueAnnotation va = null;
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				
				RDFNode value = sol.get("value");
				
				String ann = sol.get("t") != null ? sol.get("t").toString() : null;
				int start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : -1;
				int end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : -1;
				int count = sol.get("count").asLiteral().getInt();
//				int count = 0;
				
				String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;

				AnnotationEditValue aev = null;
				if (value.isResource()) {
					aev = new AnnotationEditValue(value.asResource());
				} else if (value.isLiteral()) {
					aev = new AnnotationEditValue(value.asLiteral());
				}
				
				if (!aev.equals(prev)) {
					if (prev != null) {
						res.put(prev, va);
					}

					prev = aev;
					
					va = new ValueAnnotation();
					va.setOnValue(aev);
						
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
		
		for (Map.Entry<AnnotationEditValue, ValueAnnotation> entry : res.entrySet()) {
			AnnotationEditValue aev = entry.getKey();
			ValueAnnotation nva = entry.getValue();
			
			List<AnnotationEdit> edits = null;
			if (aev.getIri() != null) {
				edits = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserIdAndIriValue(aeg.getDatasetUuid(), aeg.getOnProperty(), aeg.getAsProperty(), new ObjectId(currentUser.getId()), aev.getIri());
			} else {
				edits = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserIdAndLiteralValue(aeg.getDatasetUuid(), aeg.getOnProperty(), aeg.getAsProperty(), new ObjectId(currentUser.getId()), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype());
			}
			
			for (AnnotationEdit edit : edits) {

				if (edit.getEditType() == AnnotationEditType.REJECT) {
					for (ValueAnnotationDetail vad : nva.getDetails()) {
						if (vad.getValue().equals(edit.getAnnotationValue())) {
//							vad.setId(edit.getId().toString());
							vad.setState(AnnotationEditType.REJECT);
//							break;
						}
					}
				} else if (edit.getEditType() == AnnotationEditType.ACCEPT) {
					for (ValueAnnotationDetail vad : nva.getDetails()) {
						if (vad.getValue().equals(edit.getAnnotationValue())) {
//							vad.setId(edit.getId().toString());
							vad.setState(AnnotationEditType.ACCEPT);
//							break;
						}
					}
				} else if (edit.getEditType() == AnnotationEditType.ADD) {
//					ValueAnnotationDetail vad = new ValueAnnotationDetail(edit.getAnnotationValue(), edit.getAnnotationValue(), -1, -1);
					ValueAnnotationDetail vad  = new ValueAnnotationDetail();
					vad.setValue(edit.getAnnotationValue());
					vad.setStart(edit.getStart());
					vad.setEnd(edit.getEnd());
					
//					vad.setId(edit.getId().toString());
					vad.setState(AnnotationEditType.ADD);
					
					nva.getDetails().add(vad);
				}
			}
		}
		
		return res.values();
    } 
	
	public boolean createPagedAnnotationValidation(UserPrincipal currentUser, String virtuoso, String aegId) {

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
	
			pav = new PagedAnnotationValidation();
			pav.setUserId(new ObjectId(currentUser.getId()));
			pav.setAnnotationEditGroupId(aeg.getId());
	
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
			        "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
			        "     <" + SOAVocabulary.onValue + "> ?value ; " +
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } ";

			int annotatedValueCount = 0;

	//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(),
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
			        "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
			        "     <" + SOAVocabulary.onValue + "> ?value ; " +
			        "     <" + OAVocabulary.hasSource + "> ?s . " + "  } } }";

			int nonAnnotatedValueCount = 0;

	//		System.out.println(QueryFactory.create(countSparql, Syntax.syntaxSPARQL_11));

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(),
					QueryFactory.create(nonAnnotatedCountSparql, Syntax.syntaxSPARQL_11))) {

				ResultSet rs = qe.execSelect();

				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					nonAnnotatedValueCount = sol.get("count").asLiteral().getInt();
				}
			}

			int nonAnnotatedPages = nonAnnotatedValueCount / pageSize + (nonAnnotatedValueCount % pageSize > 0 ? 1 : 0);

			logger.info("Paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ": valueCount=" + annotatedValueCount + "/" + nonAnnotatedValueCount + " pages=" + annotatedPages + "/" + nonAnnotatedPages);

			pav.setPageSize(pageSize);
			pav.setAnnotatedPagesCount(annotatedPages);
			pav.setNonAnnotatedPagesCount(nonAnnotatedPages);

			pav = pavRepository.save(pav);

			// temporary: do not populate pages
			boolean createPages = false;

			if (createPages) {
				int totalCount = 0;

				for (int i = 1; i <= nonAnnotatedPages; i++) {
					String subsparql =
							"SELECT ?value (count(*) AS ?valueCount)" +
				            "WHERE { " +
						    "  GRAPH <" + datasetUri + "> { " +
				            "    ?s " + spath + " ?value }  " +
						    "  GRAPH <" + aeg.getAsProperty() + "> { " +
				            "  ?v a <" + OAVocabulary.Annotation + "> ; " +
						    "     <" + OAVocabulary.hasTarget + "> ?r . " +
				            "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
						    "     <" + SOAVocabulary.onValue + "> ?value ; " +
				            "     <" + OAVocabulary.hasSource + "> ?s . " + " } }  " +
						    "GROUP BY ?value " +
				            "ORDER BY desc(?valueCount) ?value " +
						    "LIMIT " + pageSize + " OFFSET " + pageSize * (i - 1);

			    	StringBuffer values = new StringBuffer();
		//	    	System.out.println(QueryFactory.create(subsparql, Syntax.syntaxSPARQL_11));

			    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(subsparql, Syntax.syntaxSPARQL_11))) {

			    		ResultSet rs = qe.execSelect();

			    		while (rs.hasNext()) {
			    			QuerySolution qs = rs.next();
			    			RDFNode value = qs.get("value");
			    			if (value.isLiteral()) {
			    				Literal l = value.asLiteral();
			    				String lf = l.getLexicalForm();

			    				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
		//
			    				values.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString() + " ");
			    			} else {
			    				values.append("<" + value.toString() + "> ");
			    			}
			    		}
			    	}

		//			SPLIT INTO TWO QUERIES : MUCH FASTER !!!!

					String sparql =
							"SELECT ?value ?t ?ie  " +
					        "WHERE { " + "  GRAPH <" + datasetUri + "> { " +
							"    ?s " + spath + " ?value }  " +
					        "   GRAPH <" + aeg.getAsProperty() + "> { " +
							"  ?v a <" + OAVocabulary.Annotation + "> ; " +
					        "     <" + OAVocabulary.hasTarget + "> ?r . " +
							" { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " +
					        " { ?v <" + OAVocabulary.hasBody + "> [ " + " a <" + OWLTime.DateTimeInterval + "> ; " +
							" <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " +
					        " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " +
							"  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " +
					        "     <" + SOAVocabulary.onValue + "> ?value ; " +
							"     <" + OAVocabulary.hasSource + "> ?s . " + " } " +
		//			        " { " + subsparql  + " }  }";
		                    " VALUES ?value { " + values.toString()  + " }  }";

		//			System.out.println(sparql);
		//			System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));

					int localCount = 0;
					try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfigurations.get(virtuoso).getSparqlEndpoint(),
							QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
						ResultSet rs = qe.execSelect();

						while (rs.hasNext()) {
							rs.next();
							localCount++;
						}
					}

					totalCount += localCount;

					logger.info("Paged annotation validation " + aeg.getDatasetUuid() + "/" + aeg.getAsProperty() + "/" + aeg.getOnProperty() + ": page=" + i + "/" + annotatedPages + "count=" + localCount + "/" + totalCount);

					PagedAnnotationValidationPage pavp = new PagedAnnotationValidationPage();
					pavp.setPagedAnnotationValidationId(pav.getId());
//					pavp.setAnnotations(localCount);
					
					pavpRepository.save(pavp);
				}
	//			System.out.println(count);
			}
			
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
	
//	//does not work needs updating with OutputHandler
//	public boolean execute(UserPrincipal currentUser, String aegId, ApplicationEventPublisher applicationEventPublisher) throws Exception {
//
//		Optional<AnnotationEditGroup> odoc = aegRepository.findById(aegId);
//	    if (!odoc.isPresent()) {
//	    	return false;
//	    }
//    	
//	    AnnotationEditGroup aeg = odoc.get();
//
//	    Date executeStart = new Date(System.currentTimeMillis());
//	    
//    	ExecuteState es = aeg.getExecuteState(fileSystemConfiguration.getId());
//    	
//		// Clearing old files
//		if (es.getExecuteState() == MappingState.EXECUTED) {
//			for (int i = 0; i < es.getExecuteShards(); i++) {
//				(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_add"
//						+ (i == 0 ? "" : "_#" + i) + ".trig")).delete();
//			}
//			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_add_catalog.trig").delete();
//			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete.trig").delete();
//			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete_catalog.trig").delete();
//		}
//		
//		es.setExecuteState(MappingState.EXECUTING);
//		es.setExecuteStartedAt(executeStart);
//		es.setExecuteShards(0);
//		es.setCount(0);
//		
//		aegRepository.save(aeg);
//
//		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(
//				fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder, aeg.getId().toString() + "_add",
//				shardSize);
//				Writer delete = new OutputStreamWriter(new FileOutputStream(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete.trig"), false), StandardCharsets.UTF_8);
//				Writer deleteCatalog = new OutputStreamWriter(new FileOutputStream(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_delete_catalog.trig"), false), StandardCharsets.UTF_8)				
//				) {
//			
//			Executor exec = new Executor(outhandler, safeExecute);
//			exec.keepSubjects(true);
//			
//			try (ExecuteMonitor em = new ExecuteMonitor("annotation-edit", aegId, null, applicationEventPublisher)) {
//				exec.setMonitor(em);
//				
//				String d2rml = env.getProperty("annotator.manual.d2rml"); 
//				InputStream inputStream = resourceLoader.getResource("classpath:"+ d2rml).getInputStream();
//				D2RMLModel rmlMapping = D2RMLModel.readFromString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
//
//				Dataset ds2 = DatasetFactory.create();
//				Model deleteModel2 = ds2.getDefaultModel();
//		
//				for (AnnotationEdit edit :  annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(aeg.getDatasetUuid(), aeg.getOnProperty(), aeg.getAsProperty(), new ObjectId(currentUser.getId()))) {
//					String prop = "";
//					for (int i = aeg.getOnProperty().size() - 1; i >= 0; i--) {
//						if (i < aeg.getOnProperty().size() - 1) {
//							prop += "/";
//						}
//						prop += "<" + aeg.getOnProperty().get(i) + ">";
//					}
//					
////					System.out.println(edit.getEditType() + " " + edit.getAnnotationValue());
//					
//					if (edit.getEditType() == AnnotationEditType.ADD) {
//						
//						Map<String, Object> params = new HashMap<>();
//						params.put("iigraph", SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString());
//						params.put("iiproperty", prop);
//						params.put("iivalue", edit.getOnValue().toString());
//						params.put("iiannotation", edit.getAnnotationValue());
//						params.put("iirdfsource", virtuosoConfiguration.getSparqlEndpoint());
////						params.put("iiannotator", SEMAVocabulary.getAnnotatorEditGroup(aeg.getUuid()));
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
//		    			        "  GRAPH <" + aeg.getAsProperty() + "> { " + 
//		    			        "   ?annId <http://www.w3.org/ns/oa#hasTarget> [ " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onProperty> \"" + prop + "\" ; " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onValue> " + edit.getOnValue().toString() + " ; " +
//		    			        "     <http://www.w3.org/ns/oa#hasSource> ?s ] . " +
//		    		            "   ?annId ?p1 ?o1 . " +
//		    		            "   OPTIONAL { ?o1 ?p2 ?o2 } } . " +	    			        
//		    			        " GRAPH <" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + "> { " +
//		    			        "  ?s a ?type } " +
//		                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
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
//		    			        "  GRAPH <" + aeg.getAsProperty() + "> { " + 
//		    			        "   ?annId <http://www.w3.org/ns/oa#hasTarget> [ " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onProperty> \"" + prop + "\" ; " + 
//		    			        "     <http://sw.islab.ntua.gr/annotation/onValue> " + edit.getOnValue().toString()  + " ; " +
//		    			        "     <http://www.w3.org/ns/oa#hasSource> ?s ] . }" +
//		    			        " GRAPH <" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + "> { " +
//		    			        "  ?s a ?type } " +
//		                        "  GRAPH <" + SEMAVocabulary.annotationGraph.toString() + "> { " +
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
//	        	try (Writer sw = new OutputStreamWriter(new FileOutputStream(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + aeg.getId().toString() + "_add_catalog.trig"), false), StandardCharsets.UTF_8)) {
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
//				aegRepository.save(aeg);
//		
////				RDFJenaConnection conn = (RDFJenaConnection)ts.getConnection();
////		
////		    	conn.saveAsTRIG(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder, aeg.getId().toString() + "_add");
//		
//					
//		    	
//		        	
//		//	        	try (Writer sw = new OutputStreamWriter(new FileOutputStream(new File(annotationsFolder + aeg.getId().toString() + "_delete_catalog.trig"), false), StandardCharsets.UTF_8)) {
//		////	        		sw.write("<" + SEMAVocabulary.getDataset(aeg.getDatasetUuid()).toString() + ">\n");
//		//	        		sw.write("        <http://purl.org/dc/terms/hasPart>\n" );
//		//	        		sw.write("                " );
//		//	        		int c = 0;
//		//	        		for (Resource r : deleteSubjects) {
//		//	        			if (c++ > 0) {
//		//	        				sw.write(" , ");
//		//	        			}
//		//	        			sw.write("<" + r.getURI() + ">");
//		//	        		}
//		//	        		sw.write(" .");
//		//	    		}
//					      
//				SSEController.send("edits", applicationEventPublisher, this, new NotificationObject("execute",
//						MappingState.EXECUTED.toString(), aegId, null, executeStart, executeFinish, subjects.size()));
//
//				logger.info("Annotation edits executed -- id: " + aegId + ", shards: " + outhandler.getShards());
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
//				logger.info("Annotation edits failed -- id: " + aegId);
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
//					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), aegId, null, null, null));
//
//			aegRepository.save(aeg);
//
//			return false;
//		}
//	}
//	
//	public boolean publish(UserPrincipal currentUser, String id) throws Exception {
//		
//		Optional<AnnotationEditGroup> doc = aegRepository.findById(new ObjectId(id));
//	
//		if (doc.isPresent()) {
//			AnnotationEditGroup adoc = doc.get();
//			
//			PublishState ps = adoc.getPublishState(virtuosoConfiguration.getDatabaseId());
//		
//			ps.setPublishState(DatasetState.PUBLISHING);
//			ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			
//			aegRepository.save(adoc);
//			
//			List<AnnotationEdit> deletes = annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndEditTypeAndUserId(adoc.getDatasetUuid(), adoc.getOnProperty(), adoc.getAsProperty(), AnnotationEditType.REJECT, adoc.getUserId());
//		
//			virtuosoJDBC.publish(currentUser, adoc, deletes);
//	    	
//			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//			ps.setPublishState(DatasetState.PUBLISHED);
//			
//			aegRepository.save(adoc);
//		}
//		
//		System.out.println("PUBLICATION COMPLETED");
//		
//		return true;
//	}
//	
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
//	
//	public Optional<String> getLastExecution(UserPrincipal currentUser, String aegId) throws Exception {
//		Optional<AnnotationEditGroup> entry = aegRepository.findById(new ObjectId(aegId));
//		
//		if (entry.isPresent()) {
//			AnnotationEditGroup doc = entry.get();
//      	
//			StringBuffer result = new StringBuffer();
//			
//			result.append(">> ADD    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//			result.append(new String(Files.readAllBytes(Paths.get(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + doc.getId().toString() + "_add.trig"))));
//			result.append("\n");
//			result.append(">> DELETE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
//			result.append(new String(Files.readAllBytes(Paths.get(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + doc.getId().toString() + "_delete.trig"))));
//
//			return Optional.of(result.toString());
//		} else {
//			return Optional.empty();
//		}
//	}
	
	
}
