package ac.software.semantic.service;

import java.util.Collection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.service.container.SideExecutableContainer;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.vocs.SOAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCATVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCMITVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;

@Service
public class SparqlQueryService {

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Value("${virtuoso.graphs.separate:#{true}}")
	private boolean separateGraphs;

	public String countAnnotationsL3(SideSpecificationDocument adoc) {
		
		String annotationGraph = adoc.getTripleStoreGraph(resourceVocabulary, separateGraphs);
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);

		String sparql =
            "SELECT (COUNT(?annotation) AS ?count) WHERE {" +
            "  GRAPH <" + annotationGraph + "> { " +
            "    ?annotation ?p1 ?o1 . ?o1 ?p2 ?o2 . ?o2 ?p3 ?o3 } " + 
            "  GRAPH <" + annotationSetGraph + "> { " +
            "    <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation . } " +
            " } ";
		
		return sparql;
	}

	public String deleteAnnotationsL3(SideSpecificationDocument adoc) {
		
		String annotationGraph = adoc.getTripleStoreGraph(resourceVocabulary, separateGraphs);
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);
	
		String sparql =
            "DELETE { GRAPH <" + annotationGraph + "> {" +
            " ?o2 ?p3 ?o3 . } } " +            		
    		"WHERE { " + 
            "  GRAPH <" + annotationGraph + "> {" +
            "    ?annotation ?p1 ?o1 . ?o1 ?p2 ?o2 . ?o2 ?p3 ?o3 } " + 
            "  GRAPH <" + annotationSetGraph + "> { " +
            "    <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation . } " +
            " } ";
		
		return sparql;
	}
	
	public String countAnnotationsL2(SideSpecificationDocument adoc) {
		
		String annotationGraph = adoc.getTripleStoreGraph(resourceVocabulary, separateGraphs);
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);

		String sparql =
            "SELECT (COUNT(?annotation) AS ?count) WHERE {" +
            "  GRAPH <" + annotationGraph + "> { " +
            "    ?annotation ?p1 ?o1 . ?o1 ?p2 ?o2 } " + 
            "  GRAPH <" + annotationSetGraph + "> { " +
            "    <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation . } " +
            " } ";
		
		return sparql;
	}

	public String deleteAnnotationsL2(SideSpecificationDocument adoc) {
		
		String annotationGraph = adoc.getTripleStoreGraph(resourceVocabulary, separateGraphs);
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);
	
		String sparql =
            "DELETE { GRAPH <" + annotationGraph + "> {" +
            " ?o1 ?p2 ?o2 . } } " +            		
    		"WHERE { " + 
            "  GRAPH <" + annotationGraph + "> {" +
            "    ?annotation ?p1 ?o1 . ?o1 ?p2 ?o2  } " + 
            "  GRAPH <" + annotationSetGraph + "> { " +
            "    <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation . } " +
            " } ";
		
		return sparql;
	}
	
	public String countAnnotationsL1(SideSpecificationDocument adoc) {
		
		String annotationGraph = adoc.getTripleStoreGraph(resourceVocabulary, separateGraphs);
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);

		String sparql =
            "SELECT (COUNT(?annotation) AS ?count) WHERE {" +
            "  GRAPH <" + annotationGraph + "> {" +
            "    ?annotation ?p1 ?o1 } " + 
            "  GRAPH <" + annotationSetGraph + "> { " +
            "        <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation . } " +
            " } ";
		
		return sparql;
	}
	
	public String deleteAnnotationsL1(SideSpecificationDocument adoc) {
		
		String annotationGraph = adoc.getTripleStoreGraph(resourceVocabulary, separateGraphs);
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);
	
		String sparql =
            "DELETE { GRAPH <" + annotationGraph + "> {" +
            " ?annotation ?p1 ?o1 } } " +            		
    		"WHERE { " + 
            "  GRAPH <" + annotationGraph + "> {" +
            "    ?annotation ?p1 ?o1 } " + 
            "  GRAPH <" + annotationSetGraph + "> { " +
            "        <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> <" + DCTVocabulary.hasPart + "> ?annotation . } " +
            " } ";
		
		return sparql;
	}
	
	public String countAnnotationSet(SideSpecificationDocument adoc) {
		
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);

		String sparql =
            "SELECT (COUNT(*) AS ?count) WHERE {" +
			" GRAPH <" + annotationSetGraph + "> { " +
					" <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> ?p ?q . } " + 
            " } ";
		
		return sparql;
	}
	
	public String deleteAnnotationSet(SideSpecificationDocument adoc) {
		
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, separateGraphs);
	
		String sparql =
			"DELETE WHERE { GRAPH <" + annotationSetGraph + "> { " +
			" <" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()) + "> ?p ?q . } }";
		
		return sparql;
	}
	
	public String countAnnotationValidationsL2(AnnotationValidation av) {
		
		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		String sparql =
				"SELECT (COUNT(?validation) AS ?count) WHERE {" + 
		        "  GRAPH <" + annotationGraph + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." +
			    "    ?validation ?p ?o . FILTER ( ?p != <" + ASVocabulary.generator + "> ) } " + 
		    	"} ";
		
		return sparql;
	}
	
	public String deleteAnnotationValidationsL2(AnnotationValidation av) {
		
		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		String sparql =
				"DELETE { " + 
			    "  GRAPH <" + annotationGraph + "> {" +
			    "    ?validation ?p ?o } " +
		        "} WHERE { " + 
		        "  GRAPH <" + annotationGraph + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." +
			    "    ?validation ?p ?o . FILTER ( ?p != <" + ASVocabulary.generator + "> ) } " + 
		    	"} ";
		
		return sparql;
	}
	
	public String countAnnotationValidationsL1(AnnotationValidation av) {
		
		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		String sparql =
				"SELECT (COUNT(?validation) AS ?count) WHERE {" + 
		        "  GRAPH <" + annotationGraph + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> }" + 
		    	"} ";
		
		return sparql;
	}
	
	public String deleteAnnotationValidationsL1(AnnotationValidation av) {
		
		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		String sparql =
				"DELETE { " + 
			    "  GRAPH <" + annotationGraph + "> {" +
			    "    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> } " +
		        "} WHERE { " + 
		        "  GRAPH <" + annotationGraph + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> }" + 
		    	"} ";
		
		return sparql;
	}
	
	public String countAnnotationValidationsL0(AnnotationValidation av) {
		
		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		String sparql =
				"SELECT (COUNT(?annotation) AS ?count) WHERE {" + 
		        "  GRAPH <" + annotationGraph + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    FILTER NOT EXISTS { ?validation ?p ?q } } " + 
		    	"} ";
		
		return sparql;
	}
	
	public String deleteAnnotationValidationsL0(AnnotationValidation av) {
		
		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		String sparql =
				"DELETE { " + 
			    "  GRAPH <" + annotationGraph + "> {" +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation } " +
		        "} WHERE { " + 
		        "  GRAPH <" + annotationGraph + "> { " + 
				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			    "    FILTER NOT EXISTS { ?validation ?p ?q } } " + 
		    	"} ";
		
		return sparql;
	}

	
//	public String countAnnotationValidations(AnnotationValidation av) {
//		
//		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary);
//		String annotationSetGraph = av.getTOCGraph(resourceVocabulary);
//
//		String sparql =
//				"SELECT (COUNT(?annotation) AS ?count) WHERE {" + 
//		        "  GRAPH <" + annotationGraph + "> { " + 
//				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//				"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//				"       ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." + // added annotations validator = generator
//				"    ?annotation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." +
//				"    ?annotation ?p1 ?o1 . OPTIONAL { ?o1 ?p2 ?o2 } } " +
//	            "  GRAPH <" + annotationSetGraph + "> { " +
//			    "    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " +
//		    	"} ";
//		
//		return sparql;
//	}
	
//	public String deleteManualAnnotationValidationsL1(AnnotationValidation av) {
//		
//		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary);
//		String annotationSetGraph = av.getTOCGraph(resourceVocabulary);
//
//		String sparql =
//				"DELETE { " + 
//			    "  GRAPH <" + annotationGraph + "> {" +
//			    "    ?annotation ?p1 ?o1 . ?o1 ?p2 ?p2 . } " +
//	            "  GRAPH <" + annotationSetGraph + "> { " +
//			    "    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " +
//		        "} WHERE { " + 
//		        "  GRAPH <" + annotationGraph + "> { " + 
//				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//				"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//				"    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." + // added annotations validator = generator
//				"    ?annotation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." +
//				"    ?annotation ?p1 ?o1 . OPTIONAL { ?o1 ?p2 ?o2 } } " +
//	            "  GRAPH <" + annotationSetGraph + "> { " +
//			    "    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " +
//		    	"} ";
//		
//		return sparql;
//	}
//
//	public String deleteManualAnnotationValidationsL1(AnnotationValidation av) {
//		
//		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary);
//		String annotationSetGraph = av.getAsProperty() != null ? resourceVocabulary.getAnnotationGraphResource().toString() : av.getTripleStoreGraph(resourceVocabulary);
//
//		String sparql =
//				"DELETE { " + 
//			    "  GRAPH <" + annotationGraph + "> {" +
//			    "    ?annotation ?p1 ?o1 . ?o1 ?p2 ?p2 . } " +
//	            "  GRAPH <" + annotationSetGraph + "> { " +
//			    "    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " +
//		        "} WHERE { " + 
//		        "  GRAPH <" + annotationGraph + "> { " + 
//				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//				"    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//				"    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> ." + // added annotations validator = generator
//				"    ?annotation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> ." +
//				"    ?annotation ?p1 ?o1 . OPTIONAL { ?o1 ?p2 ?o2 } } " +
//	            "  GRAPH <" + annotationSetGraph + "> { " +
//			    "    ?annotationset <" + DCTVocabulary.hasPart + "> ?annotation . } " +
//		    	"} ";
//		
//		return sparql;
//	}
//
//	public String countAnnotationValidations(AnnotationValidation av) {
//		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary);
//		String annotationSetGraph = av.getAsProperty() != null ? resourceVocabulary.getAnnotationGraphResource().toString() : av.getTripleStoreGraph(resourceVocabulary);
//
//		String sparql =
//				"SELECT (COUNT(?validation) AS ?count) WHERE {" + 
//		        "  GRAPH <" + annotationGraph + "> { " + 
//				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//			    "    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> . " +
//			    "    ?validation ?p ?o } " + 
//		    	"} ";
//		
//		return sparql;
//	}
//	
//	public String deleteAnnotationValidations(AnnotationValidation av) {
//		String annotationGraph = av.getTripleStoreGraph(resourceVocabulary);
//		String annotationSetGraph = av.getAsProperty() != null ? resourceVocabulary.getAnnotationGraphResource().toString() : av.getTripleStoreGraph(resourceVocabulary);
//
//		String sparql =
//				"DELETE { " + 
//			    "  GRAPH <" + annotationGraph + "> {" +
//		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//			    "    ?validation ?p ?o } " +
//		        "} WHERE { " + 
//		        "  GRAPH <" + annotationGraph + "> { " + 
//				"    ?annotation a <" + OAVocabulary.Annotation + "> . " +
//		        "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
//			    "    ?validation <" + ASVocabulary.generator + "> <" + resourceVocabulary.getAnnotationValidatorAsResource(av.getUuid()) + "> . " +
//			    "    ?validation ?p ?o } " + 
//		    	"} ";
//		
//		return sparql;
//	}
	

	
	public String declareAnnotationSet(SideSpecificationDocument adoc) {
		String annotationSetGraph = adoc.getTOCGraph(resourceVocabulary, false);
	
		String sparql; 
		if (adoc instanceof AnnotatorDocument && ((AnnotatorDocument)adoc).getAsProperty() != null) { // legacy
			sparql = 
			   "INSERT { GRAPH <" + annotationSetGraph + "> { " +
	                "<" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()).toString() + "> a <" + DCATVocabulary.Dataset + "> , <" + VOIDVocabulary.Dataset + "> . } } WHERE { }";
	
		} else {
			sparql = 
			   "INSERT { GRAPH <" + annotationSetGraph + "> { " +
	                "<" + resourceVocabulary.getAnnotationSetAsResource(adoc.getUuid()).toString() + "> a <" + DCMITVocabulary.Collection + "> ; <" + DCTVocabulary.creator + "> <" + adoc.asResource(resourceVocabulary) + "> . } } WHERE { }";
		}
		
		return sparql;
	}
	
	public <D extends SideSpecificationDocument, M extends ExecuteState, I extends EnclosingDocument> 
		String insertReferenedByInAnnotations(SideExecutableContainer<D,?,M,I> ec) {

		SideSpecificationDocument doc = ec.getObject();
		Dataset dataset = (Dataset)ec.getEnclosingObject();

		DatasetCatalog dcg = schemaService.asCatalog(dataset);
		String usingClause = schemaService.buildUsingClause(dcg);
		
		String annotationGraph = doc.getTripleStoreGraph(resourceVocabulary, separateGraphs);

		if (doc instanceof AnnotatorDocument) {
			AnnotatorDocument adoc = (AnnotatorDocument)doc;
			
			if (adoc.getOnProperty() == null) { // TODO for onClass !!!
				return null;
			}
			
			String onPropertyString = PathElement.onPathStringListAsSPARQLString(adoc.getOnProperty());
	
			String sparql = 
			   "INSERT { " +
	           "  GRAPH <" + annotationGraph + "> { ?target <" + DCTVocabulary.isReferencedBy + "> ?property } } " +
			   usingClause +
			   "USING NAMED <" + annotationGraph + "> " + 
			   "WHERE { " +
			   "  ?s ?property ?body ." + 
			   "  GRAPH <" + annotationGraph + "> { " +
			   "    ?v a <" + OAVocabulary.Annotation + "> . " +
			   "    ?v <" + OAVocabulary.hasTarget + "> ?target . " + 
			   "    ?target <" + OAVocabulary.hasSource + ">  ?s . " +
			   "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
			   "    ?v <" + OAVocabulary.hasBody + ">  ?body . FILTER ( !isBlank(?body) ) " +
			   "    ?v <" + ASVocabulary.generator + "> <" + adoc.asResource(resourceVocabulary) + "> . " +
			   "  } " +
			   "}";    				
		
			return sparql;
		}
		
		if (doc instanceof AnnotationValidation) {
			
			AnnotationValidation av = (AnnotationValidation)doc;
			
			if (av.getOnProperty() == null) { // TODO for onClass !!!
				return null;
			}
			
			String onPropertyString = PathElement.onPathStringListAsSPARQLString(av.getOnProperty());
	
			String sparql = 
			 		   "INSERT { GRAPH <" + annotationGraph + "> { ?target <" + DCTVocabulary.isReferencedBy + "> ?property } }" +
			 	       usingClause + 
			 	       "USING NAMED <" + annotationGraph + "> " +
			 	       "WHERE { " +
			  		   "  ?source ?property ?body . " + 
			 		   "  GRAPH <" + annotationGraph + "> { " +
			 		   "    ?annotation a <" + OAVocabulary.Annotation + "> . " +
			 		   "    ?annotation <" + OAVocabulary.hasTarget + "> ?target . " + 
			 		   "    ?target <" + OAVocabulary.hasSource + ">  ?source . " +
			 		   "    ?target <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " +
			 		   "    ?annotation <" + OAVocabulary.hasBody + ">  ?body . " +
					   "    ?annotation <" + SOAVocabulary.hasValidation + "> ?validation . " +
			 		   "    ?annotation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> . " + // added only annotations
					   "    ?validation <" + ASVocabulary.generator + "> <" + av.asResource(resourceVocabulary) + "> ." +     		   
			 		   "  } " +
			 		   "}";		
		
			return sparql;
		}
		
		return null;
		
	}
	
	public String countAnnotations(String inSelect, String annotatorFromClause, String annFilter, String inWhere, String filterString) {
		String sparql = 
				"SELECT (COUNT(DISTINCT ?v) AS ?annCount) (COUNT(DISTINCT ?source) AS ?sourceCount) " + inSelect +
				annotatorFromClause + 
		        "WHERE { " +  
		        	" ?v <" + RDFVocabulary.type + "> <" + OAVocabulary.Annotation + "> . " + 
		        	annFilter + 
		        	" ?v <" + OAVocabulary.hasTarget + "> ?r . " +
		        	" ?r <" + OAVocabulary.hasSource + "> ?source . " +
		        	inWhere +
		        	filterString +
                "}";
		
		return sparql;
	}
	
	public String generatorFilter(String var, List<String> generatorUris) {
		String filter = "";
		
		for (String uri : generatorUris) {
			filter += "<" + uri + "> ";
		}
	
		if (filter.length() > 0) {
			filter = "?" + var + " <" + ASVocabulary.generator + "> ?generator . VALUES ?generator { " + filter + " } . ";
		}
		
		return filter;
	}	
	
	public String annotatorFilter(String var, Collection<String> annotatorUuids) {
		String annfilter = "";
		
		for (String uuid : annotatorUuids) {
			annfilter += "<" + resourceVocabulary.getAnnotatorAsResource(uuid).toString() + "> ";
		}
	
		if (annfilter.length() > 0) {
			annfilter = "?" + var + " <" + ASVocabulary.generator + "> ?generator . VALUES ?generator { " + annfilter + " } . ";
		}
		
		return annfilter;
	}	

}
