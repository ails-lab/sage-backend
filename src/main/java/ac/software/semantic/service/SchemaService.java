package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ac.software.semantic.controller.PathElement;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.repository.DatasetRepository;
import edu.ntua.isci.ac.lod.vocabularies.DCAMVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;

@Service
public class SchemaService {

	@Autowired
	DatasetRepository datasetRepository;
	
    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;
    
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;
    
	private void explore(VirtuosoConfiguration vc, List<PathElement> path, String datasetUri, Model outModel, boolean ranges) {
		
//		System.out.println("EXPLORING " + path.size());
		String spath = "";
		String endVar = "";
		
		String modelPath = "BIND(<" + datasetUri + "> AS ?c0) . ";
		
		int pathLength = 0;
		int modelLength = 1;
		for (int i = 0; i < path.size(); i++) {
			PathElement pe = path.get(i);
			if (pe.getType().equals("class")) {
				spath += "?c" + pathLength + " a <" + pe.getUri() + "> . ";
				
				modelPath += "?c" + (modelLength-1) +  " <" + VOIDVocabulary.classPartition + "> ?c" + modelLength + " . ?c" + modelLength +  " <" + VOIDVocabulary.clazz + "> " + "<" + pe.getUri() + "> ." ;
				modelLength++;
			} else {
				spath += "?c" + pathLength + " <" + pe.getUri() + "> ?c" + (pathLength + 1) + " . ";
				endVar = "c" + (pathLength + 1);
			    pathLength++;

			    modelPath += "?c" + (modelLength-1) +  " <" + VOIDVocabulary.propertyPartition + "> ?c" + modelLength + " . ?c" + modelLength +  " <" + VOIDVocabulary.property + "> " + "<" + pe.getUri() + "> ." ;
			    modelLength++;
			}
		}
		
		modelPath = "SELECT ?c" + (modelLength - 1) + " WHERE { " + modelPath + " } ";
				
		Resource subject = null;
		try (QueryExecution lqe = QueryExecutionFactory.create(modelPath, outModel)) {
			ResultSet qrs = lqe.execSelect();
			while (qrs.hasNext()) {
				QuerySolution qqs = qrs.next();
				subject = qqs.get("?c" + (modelLength - 1)).asResource();
			}
		}
		
//		System.out.println("EXPLORING " + spath);
		if (pathLength == 0) {
			endVar = "c0";
////			spath = "?entry  ?prop ?z . FILTER NOT EXISTS { ?k1 ?k2 ?entry }" +
////					" OPTIONAL { " +
////		            "  ?entry  ?prop ?b  " + 
////		            "  FILTER isBlank(?b) } ";
////			spath = "?c0 ?prop ?z . ?c0 a ?type . FILTER isIRI(?c0) " +
//			spath = spath + "?" + endVar + " ?prop ?z . " + // " FILTER (isIRI(?c0))" + // FILTER is not necessary here and may causes virtuoso timeout
//					" OPTIONAL { " +
//		            "  ?" + endVar + " ?prop ?b  " + 
//		            "  FILTER isBlank(?b) " +
//		            "} ";
//		} else {
////			spath = "?entry ?p1 ?p2 . FILTER NOT EXISTS { ?k1 ?k2 ?entry } " +
////					"?entry " + spath + " ?z0 . ?z0 ?prop ?z . " +
//// 			        " OPTIONAL { " +
////                    "  ?z0  ?prop ?b  " + 
////                    "  FILTER isBlank(?b) } ";
////			spath = "?c0 ?p1 ?p2 . FILTER (isIRI(?c0)) " +
//			spath = //"FILTER (isIRI(?c0)) " + // is FILTER necessary here?
////					spath + " ?" + endVar + " ?prop ?z . ?" + endVar + " a ?type . " +
//                    spath + " ?" + endVar + " ?prop ?z . " +
// 			        " OPTIONAL { " +
//                    "  ?" + endVar + " ?prop ?b  " + 
//                    "  FILTER isBlank(?b) " +
//			        "} ";			
		}
		
		spath = spath + "?" + endVar + " ?prop ?z . ";
		

		Map<Resource, List<Resource>> rangeMap = new HashMap<>();
		
		if (ranges) {
			// Get properties ranges 
			String sparql = 
			    	"SELECT ?prop ?type " + 
			        "WHERE { " +
					"  GRAPH <" + datasetUri + "> { " +
			    	     spath +
	 				"    ?z a ?type  " +
	                " } } " +
			 		"GROUP BY ?prop ?type";
			
			Query query = QueryFactory.create(sparql);

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), query)) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					Resource prop = sol.getResource("prop");
					if (prop.getURI().equals(RDFVocabulary.type.toString())) {
						continue;
					}
	
					List<Resource> list = rangeMap.get(prop);
					if (list == null) {
						list = new ArrayList<>();
						rangeMap.put(prop, list);
					}
					list.add(sol.getResource("type"));
				}
			}
		}
		
		// Get properties 
		String sparql = 
		    	"SELECT ?prop (count(?z) AS ?countz) (count(?b) AS ?countb) " + 
		        "WHERE { " +
				"  GRAPH <" + datasetUri + "> { " +
		    	     spath +
 				"    OPTIONAL { " +
	            "      ?" + endVar + " ?prop ?b  " + 
	            "      FILTER isBlank(?b) } " +
                " } } " +
		 		"GROUP BY ?prop";

		
		Query query = QueryFactory.create(sparql);
//		System.out.println(query);
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), query)) {
			
			ResultSet rs = qe.execSelect();
			
			while (rs.hasNext()) {
				QuerySolution sol = rs.next();
				Resource prop = sol.getResource("prop");
				if (prop.toString().equals(RDFVocabulary.type.toString())) {
					continue;
				}

				Resource partition = outModel.createResource();
				outModel.add(subject, VOIDVocabulary.propertyPartition, partition);
				partition.addProperty(RDFVocabulary.type, VOIDVocabulary.Dataset);
				partition.addProperty(VOIDVocabulary.property, prop);
				partition.addProperty(VOIDVocabulary.triples, sol.get("countz"));
				
				List<Resource> list = rangeMap.get(prop);
				if (list != null) {
					for (Resource r : list) {
						partition.addProperty(DCAMVocabulary.rangeIncludes, r);
					}
				}
				
				
				List<PathElement> newPath = new ArrayList<>();
				newPath.addAll(path);
				newPath.add(new PathElement("property", prop.toString()));

				int blank = sol.get("countb").asLiteral().getInt();
				if (blank > 0) {
					explore(vc, newPath, datasetUri, outModel, ranges);
				}
				
			}
		}
	}

	private List<Resource> getTopClasses(VirtuosoConfiguration vc, String datasetUri, Model outModel) {
		String lsparql =
			"SELECT  ?clazz (count(?x) as ?count)" +
	        "WHERE { " +
	        "  GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
	        "    <" + datasetUri + ">  a  <http://www.w3.org/ns/dcat#Dataset> . " +
	    	"	 <" + datasetUri + ">  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/class" : SEMAVocabulary.clazz) + "> ?t . " +
	    	"    ?t  <" + (legacyUris ? "http://sw.islab.ntua.gr/apollonis/ms/uri" : SEMAVocabulary.uri) + "> ?clazz . } " +
            "  GRAPH <" + datasetUri + "> { " +
            "    ?x a ?clazz } " +
	    	"  } " +
	    	"GROUP BY ?clazz ";

	//		System.out.println(QueryFactory.create(lsparql));
		List<Resource> result = new ArrayList<>();

		Resource subject = outModel.createResource(datasetUri);
		outModel.add(subject, RDFVocabulary.type, VOIDVocabulary.Dataset);

		try (QueryExecution lqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(lsparql))) {
			ResultSet rs =  lqe.execSelect();
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
				Resource clazz = qs.get("clazz").asResource();

				result.add(clazz);
				
				Resource partition = outModel.createResource();
				outModel.add(subject, VOIDVocabulary.classPartition, partition);
				partition.addProperty(RDFVocabulary.type, VOIDVocabulary.Dataset);
				partition.addProperty(VOIDVocabulary.clazz, clazz);
				partition.addProperty(VOIDVocabulary.entities, qs.get("count").asLiteral());
			}
		}
		
		return result;
	}
	
    public Model buildSchema(String datasetUri, boolean ranges)  {

    	Model model = ModelFactory.createDefaultModel();
    	
    	String datasetUuid = SEMAVocabulary.getId(datasetUri);
		
    	Optional<Dataset> dopt = datasetRepository.findByUuid(datasetUuid);
    	if (!dopt.isPresent()) {
    		return model;
    	}
    	
		VirtuosoConfiguration vc = dopt.get().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		
		for (Resource node : getTopClasses(vc, datasetUri, model)) {

			ArrayList<PathElement> path = new ArrayList<>();
			path.add(new PathElement("class", node.getURI()));

			explore(vc, path, datasetUri, model, ranges);
		}
		
		model = Vocabulary.standarizeNamespaces(model);

		return model;
    }
    
    public Model readSchema(String datasetUri) {
    	
    	String datasetUuid = SEMAVocabulary.getId(datasetUri);
    	
    	Optional<Dataset> dopt = datasetRepository.findByUuid(datasetUuid);
    	if (!dopt.isPresent()) {
    		return null;
    	}
    	
		VirtuosoConfiguration vc = dopt.get().getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
		
		String query = 
				"CONSTRUCT { <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . ?c1 ?c11 ?c12 . } " + 
				"  WHERE  { GRAPH <" + SEMAVocabulary.contentGraph + "> {" +
				"    <" + datasetUri + "> <" + VOIDVocabulary.classPartition + "> ?cc . " + 
				"    <" + datasetUri + "> (<" + VOIDVocabulary.classPartition + ">|<" + VOIDVocabulary.propertyPartition + ">)+ ?c1 . ?c1 ?c11 ?c12 . } }";
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), query)) {
			Model model = qe.execConstruct();
			model = Vocabulary.standarizeNamespaces(model);
			return model;
		}

    }
}
