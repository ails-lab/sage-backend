package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.AnnotationEditGroupSearch;
import ac.software.semantic.model.AnnotationEditGroupSearchField;
import ac.software.semantic.model.AnnotatorContext;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.Pagination;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.payload.response.ResultCount;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;

@Service
public class AnnotationUtils {

	@Autowired
	private SPARQLService sparqlService;

	@Autowired
	private SparqlQueryService sparqlQueryService;

	@Autowired
	private DatasetRepository datasetRepository;
	
	@Autowired
	private PrototypeDocumentRepository prototypeRepository;
	
	@Lazy
	@Autowired
	private DatasetService datasetService;
	
    @Autowired
    @Qualifier("annotators")
    private Map<String, DataService> annotators;
    
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;
	
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	private QueryExecution createQueryExecution(TripleStoreConfiguration vc, org.apache.jena.query.Dataset rdfDataset, String sparql) {
		QueryExecution qe;
		if (rdfDataset != null) {
			qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset);
		} else {
			qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
		}
		return qe;
	}
	
	public 	ValueResponseContainer<ValueAnnotation> countAnnotations(AnnotationContainerBase abase, List<IndexKeyMetadata> keyMetadata, TripleStoreConfiguration vc, org.apache.jena.query.Dataset rdfDataset, String inSelect, String annotatorFromClause, String annFilter, String inWhere, String filterString) {
		String sparql = sparqlQueryService.countAnnotations(inSelect, annotatorFromClause, annFilter, inWhere, filterString); 
		
		int annCount = 0;
		int sourceCount = 0;
//		int valueCount = 0;
		List<ResultCount> rc  = new ArrayList<>();
		
//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
		
		try (QueryExecution qe = createQueryExecution(vc, rdfDataset, sparql)) {
			ResultSet rs = qe.execSelect();
//			rdfDataset.execQuery(sparql);
			
			if (rs.hasNext()) {
//			if (rdfDataset.hasNext()) {
				QuerySolution sol = rs.next();
//				rdfDataset.next();
//				RDFAccessWrapper sol = rdfDataset;
				
				annCount = sol.get("annCount").asLiteral().getInt();
				sourceCount = sol.get("sourceCount").asLiteral().getInt();
				
				if (abase.getOnProperty() != null) {
					rc.add(new ResultCount("property", sol.get("valueCount").asLiteral().getInt()));
				} else {
					for (IndexKeyMetadata ikm : keyMetadata) {
						if (sol.get("value" + ikm.getIndex() + "Count") != null) {
							rc.add(new ResultCount(ikm.getName() , sol.get("value" + ikm.getIndex() + "Count").asLiteral().getInt()));
						}
					}
				}	
			}
		}
		
		ValueResponseContainer<ValueAnnotation> vrc = new ValueResponseContainer<>();
		vrc.setTotalCount(annCount);
		vrc.setDistinctSourceTotalCount(sourceCount);
		vrc.setDistinctValueTotalCount(rc);
		
		return vrc;
	}
	
	public RDFTermHandler createValueRDFTermHandler(AnnotationContainerBase abase, QuerySolution vsol, Map<String, IndexKeyMetadata> keyMap) {
		RDFTermHandler aev = null;
		
		if (abase.getOnProperty() != null) {
			RDFNode value = vsol.get("value");
//			RDFNode value = RDF4JRemoteSelectIterator.value2RDFNode(vsol.getBinding("value").getValue()) ;
			
			if (value.isResource()) {
				aev = new SingleRDFTerm(value.asResource());
			} else if (value.isLiteral()) {
				aev = new SingleRDFTerm(value.asLiteral());
			}
		} else {
			List<SingleRDFTerm> list = new ArrayList<>();
			for (Map.Entry<String,IndexKeyMetadata> entry : keyMap.entrySet()) {
				RDFNode value = vsol.get("value_" + entry.getKey());
//				RDFNode value = vsol.getBinding("value_" + entry.getKey()) != null ? RDF4JRemoteSelectIterator.value2RDFNode(vsol.getBinding("value_" + entry.getKey()).getValue()) : null ;
				
				if (value != null) {
					SingleRDFTerm st = null;
					if (value.isResource()) {
						st = new SingleRDFTerm(value.asResource());
					} else if (value.isLiteral()) {
						st = new SingleRDFTerm(value.asLiteral());
					}
					st.setName(entry.getValue().getName());
				
					list.add(st);
				}
			}
			
			aev = new MultiRDFTerm(list);
		}

		return aev;
	}
	
	public Pagination createPagination(TripleStoreConfiguration vc, org.apache.jena.query.Dataset rdfDataset, String sparql, int page, int pageSize) {
		Pagination pg = new Pagination();

		try (QueryExecution qe = createQueryExecution(vc, rdfDataset, sparql)) {
			ResultSet vrs = qe.execSelect();
			
			int count = 0;
			while (vrs.hasNext()) {	
				QuerySolution vsol = vrs.next();
				count = vsol.get("count").asLiteral().getInt();
			}
			
			pg.setTotalElements(count);
			pg.setCurrentPage(page);
			pg.setTotalPages((int)Math.ceil(count / (double)pageSize));
		}
		
		return pg;
	}
	
	public AnnotatorContext createAnnotationContext(AnnotatorDocument adoc, Dataset enclosingDataset) {
		AnnotatorContext ai = new AnnotatorContext();

		if (adoc.getAnnotator() != null) {
			DataService annotatorService = annotators.get(adoc.getAnnotator());
			ai.setName(annotatorService.getTitle());
		} else if (adoc.getAnnotatorId() != null) {
			ai.setName(prototypeRepository.findById(adoc.getAnnotatorId()).get().getName());
		}

//		if (adoc.getThesaurus() != null) {
		if (adoc.getThesaurusId() != null) {
//			Dataset dataset = datasetRepository.findByIdentifierAndDatabaseId(adoc.getThesaurus(), database.getId()).get();
			Dataset dataset = datasetRepository.findById(adoc.getThesaurusId()).get();
			ai.setId(dataset.getId());												
			ai.setVocabularyContainer(datasetService.createVocabularyContainer(dataset.getId()));
		} else {
			ai.setId(enclosingDataset.getId());												
			
			VocabularyContainer<ResourceContext> vcont = new VocabularyContainer<>();
			for (Vocabulary voc : vocc.getVocsById().values()) {
				vcont.add(voc);
			}
			
			ai.setVocabularyContainer(datasetService.createVocabularyContainer(enclosingDataset.getId(), vcont));
		}
		
		return ai;
	}
	
	public AnnotationUtilsContainer createAnnotationUtilsContainer(AnnotationContainerBase abase, AnnotationEditGroupSearch filter) {
		return new AnnotationUtilsContainer(abase, filter);
	}
	
	public class AnnotationUtilsContainer {
		private String inSelect;
		private String inWhere;
		private String filterString;

		private String annfilter;
		
		private List<IndexKeyMetadata> keyMetadata;
		
		private Map<String, Integer> fieldNameToIndexMap;
		private Map<String, IndexKeyMetadata> rNameToEntryMap;
		
		private String variablesString;
		private String valuesString;
		
		private List<String> variables;
		private List<String> hasValueClauses;
		private List<String> hasNoValueClauses;
		
		private String bodyClause;
		private List<String> bodyVariables;
		private String groupBodyVariables;

		private List<String> bodyProperties; 

		
		public AnnotationUtilsContainer(AnnotationContainerBase abase, AnnotationEditGroupSearch filter) {

			List<AnnotatorDocument> adocs = abase.getAnnotators();
			
			annfilter = sparqlQueryService.generatorFilter("v", adocs.stream().map(doc -> doc.asResource(resourceVocabulary).toString()).collect(Collectors.toList()));
			
			fieldNameToIndexMap = new HashMap<>();
			rNameToEntryMap = new LinkedHashMap<>();

			inSelect = "";
			inWhere = "";
			filterString = "";
			
			variablesString = "";
			valuesString = "";
			
			variables = new ArrayList<>();
			hasValueClauses = new ArrayList<>();
			hasNoValueClauses = new ArrayList<>();
			
			bodyVariables = new ArrayList<>();
			groupBodyVariables = "";
			bodyClause = "";
			
			if (abase.getOnProperty() != null) { 

				IndexKeyMetadata entry = new IndexKeyMetadata(0, "");
				rNameToEntryMap.put("r" + entry.getIndex(), entry);
				fieldNameToIndexMap.put(entry.getName(), entry.getIndex());

				if (filter != null) {
					for (AnnotationEditGroupSearchField filt : filter.getFields()) { // there should be one !
						filterString += " VALUES ?value" + " { " + filt.getValue() + " } "; 
					}
				}
				
				inSelect = " (COUNT(DISTINCT ?value) AS ?valueCount) " ;
				inWhere = " ?r <" + SOAVocabulary.onValue + "> ?value . ";
				
				String spath = PathElement.onPathStringListAsSPARQLString(abase.getOnProperty());
				variablesString = "?value ";
		        String bindString = " ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" . " + 
				             " ?r <" + SOAVocabulary.onValue + "> ?value . ";
		        valuesString = " ?c_0 " + spath + " ?value . FILTER (isLiteral(?value)) . ";
		        
		        variables.add("value");
		        hasValueClauses.add(bindString);
		        hasNoValueClauses.add(" FILTER NOT EXISTS { ?r <" + SOAVocabulary.onValue + "> ?value . } ");	
		        
			} else {
				keyMetadata = adocs.get(0).getStructure().getKeysMetadata();

				if (adocs.size() > 1) {
					for (AnnotatorDocument adoc : adocs) {
						List<IndexKeyMetadata> adocKeyMetadata = adoc.getStructure().getKeysMetadata();
						for (int i = 0; i < adocKeyMetadata.size(); i++) {
							if (adocKeyMetadata.get(i).getOptional() != null && adocKeyMetadata.get(i).getOptional() == true) {
								keyMetadata.get(i).setOptional(true);
							}
						}
					}
				}
				
				for (IndexKeyMetadata ikm : keyMetadata) {
					rNameToEntryMap.put("r" + ikm.getIndex(), ikm);
					fieldNameToIndexMap.put(ikm.getName(), ikm.getIndex());
				}
				
				Set<Integer> filteredFields = new HashSet<>();
				if (filter != null) {
					for (AnnotationEditGroupSearchField filt : filter.getFields()) {
						filterString += " VALUES ?value_r" + fieldNameToIndexMap.get(filt.getField()) + " { " + filt.getValue() + " } ";
						filteredFields.add(fieldNameToIndexMap.get(filt.getField()));
					}
				}
				
				for (IndexKeyMetadata ikm : keyMetadata) {
					inSelect += " (count(DISTINCT ?value_r" + ikm.getIndex() + ") AS ?value" + ikm.getIndex() + "Count) " ;
					if (ikm.getOptional() != null && ikm.getOptional() == true && !filteredFields.contains(ikm.getIndex())) {
						inWhere += " OPTIONAL { ?r <" +  SOAVocabulary.onBinding + "> ?vvv_r" + ikm.getIndex() + " . ?vvv_r" + ikm.getIndex() + " <" +  SOAVocabulary.value + "> ?value_r" + ikm.getIndex() + " . ?vvv_r" + ikm.getIndex() + " <" + SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" } . " ;
					} else {
						inWhere += " ?r <" +  SOAVocabulary.onBinding + "> ?vvv_r" + ikm.getIndex() + " . ?vvv_r" + ikm.getIndex() + " <" +  SOAVocabulary.value + "> ?value_r" + ikm.getIndex() + " . ?vvv_r" + ikm.getIndex() + " <" + SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" . " ;
					}
				}
				
				
				for (IndexKeyMetadata ikm : keyMetadata) {
					variables.add("value_r" + ikm.getIndex());
					hasValueClauses.add(" ?r <" + SOAVocabulary.onBinding + "> ?vvv_r" + ikm.getIndex() + " . ?vvv_r" + ikm.getIndex() + " <" + SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" . ?vvv_r" + ikm.getIndex() + " <" + SOAVocabulary.value + "> ?value_r" + ikm.getIndex() + " . ") ;
					hasNoValueClauses.add(" FILTER NOT EXISTS { ?r <" +  SOAVocabulary.onBinding + "> ?vvv_r" + ikm.getIndex() + " . ?vvv_r" + ikm.getIndex() + " <" +  SOAVocabulary.variable + "> \"r" + ikm.getIndex() + "\" } . ") ;
				}
				
				for (String s : variables) {
					variablesString += "?" + s + " ";
				}
				
				Map<Integer, IndexKeyMetadata> indexKeyMetadataMap = AnnotatorDocument.getKeyMetadataMap(keyMetadata);
				if (adocs.size() > 1) {
					String clause = null;
					Set<String> clauses = new HashSet<>();
					for (AnnotatorDocument adoc : adocs) {
						SPARQLStructure ss = sparqlService.toSPARQL(adoc.getStructure().getElement(), indexKeyMetadataMap, false);
						if (clause == null) {
							clause = " { " + ss.getWhereClause() + " } ";
							clauses.add(ss.getWhereClause());
						} else {
							if (clauses.add(ss.getWhereClause())) {
								clause += " UNION { " + ss.getWhereClause() + " } ";
							}
						}
					}
					valuesString = " " + clause;
				} else {
					SPARQLStructure ss = sparqlService.toSPARQL(adocs.get(0).getStructure().getElement(), indexKeyMetadataMap, false);
					valuesString = " " + ss.getWhereClause();
				}

				valuesString = valuesString.replaceAll("> \\?r([0-9]+) ", "> ?value_r$1 ");
				valuesString = valuesString.replaceAll(" VALUES \\?r([0-9]+) \\{ .*? \\} ", ""); // very hack.... temporary should not have VALUES on ?r...
				
				Set<String> bodyPropertiesSet = new TreeSet<>();
				for (int i = 0; i < adocs.size(); i++) {
					if (adocs.get(i).getBodyProperties() != null) {
						bodyPropertiesSet.addAll(adocs.get(i).getBodyProperties());
					}
				}
				bodyProperties = new ArrayList<>(bodyPropertiesSet);
				
				for (int i = 0; i < bodyProperties.size(); i++) {
					String s = bodyProperties.get(i);
					bodyClause += "OPTIONAL { ?v <" + SOAVocabulary.hasBodyStatement + "> [ <" + RDFSVocabulary.object + "> ?b" + i + "; <" + RDFSVocabulary.predicate + "> <" + s + "> ] } ";
					bodyVariables.add("gb" + i);
					groupBodyVariables += "(GROUP_CONCAT(DISTINCT ?b" + i + ";separator=\"||\") AS ?gb" + i + ") ";
				}
			}		
		}
		
		public String getInSelect() {
			return inSelect;
		}

		public void setInSelect(String inSelect) {
			this.inSelect = inSelect;
		}

		public String getInWhere() {
			return inWhere;
		}

		public void setInWhere(String inWhere) {
			this.inWhere = inWhere;
		}

		public String getFilterString() {
			return filterString;
		}

		public void setFilterString(String filterString) {
			this.filterString = filterString;
		}

		public List<IndexKeyMetadata> getKeyMetadata() {
			return keyMetadata;
		}

		public void setKeyMetadata(List<IndexKeyMetadata> keyMetadata) {
			this.keyMetadata = keyMetadata;
		}

		public Map<String, Integer> getFieldNameToIndexMap() {
			return fieldNameToIndexMap;
		}

		public void setFieldNameToIndexMap(Map<String, Integer> fieldNameToIndexMap) {
			this.fieldNameToIndexMap = fieldNameToIndexMap;
		}

		public Map<String, IndexKeyMetadata> getrNameToEntryMap() {
			return rNameToEntryMap;
		}

		public void setrNameToEntryMap(Map<String, IndexKeyMetadata> rNameToEntryMap) {
			this.rNameToEntryMap = rNameToEntryMap;
		}

		public String getAnnfilter() {
			return annfilter;
		}

		public void setAnnfilter(String annfilter) {
			this.annfilter = annfilter;
		}

		public String getVariablesString() {
			return variablesString;
		}

		public void setVariablesString(String variablesString) {
			this.variablesString = variablesString;
		}

		public String getValuesString() {
			return valuesString;
		}

		public void setValuesString(String valuesString) {
			this.valuesString = valuesString;
		}

		public List<String> getVariables() {
			return variables;
		}

		public void setVariables(List<String> variables) {
			this.variables = variables;
		}

		public List<String> getHasValueClauses() {
			return hasValueClauses;
		}

		public void setHasValueClauses(List<String> hasValueClauses) {
			this.hasValueClauses = hasValueClauses;
		}

		public List<String> getHasNoValueClauses() {
			return hasNoValueClauses;
		}

		public void setHasNoValueClauses(List<String> hasNoValueClauses) {
			this.hasNoValueClauses = hasNoValueClauses;
		}

		public String getBodyClause() {
			return bodyClause;
		}

		public void setBodyClause(String bodyClause) {
			this.bodyClause = bodyClause;
		}

		public String getGroupBodyVariables() {
			return groupBodyVariables;
		}

		public void setGroupBodyVariables(String groupBodyVariables) {
			this.groupBodyVariables = groupBodyVariables;
		}
		
		public List<String> getBodyVariables() {
			return bodyVariables;
		}

		public void setBodyVariables(List<String> bodyVariables) {
			this.bodyVariables = bodyVariables;
		}
		
		public List<String> getBodyProperties() {
			return bodyProperties;
		}

		public void setBodyProperties(List<String> bodyProperties) {
			this.bodyProperties = bodyProperties;
		}
	}

}
