package ac.software.semantic.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.DatasetController.AnnotatorStatisticsCube;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.Distribution;
import ac.software.semantic.payload.stats.AnnotationsStatistics;
import ac.software.semantic.payload.stats.ComplexStatisticsValueCount;
import ac.software.semantic.payload.stats.DistributionStatisticsValueCount;
import ac.software.semantic.payload.stats.StatisticsValueCount;
import ac.software.semantic.vocs.SOAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;

@Service
public class AnnotationsStatisticsService {

	@Autowired
	@Qualifier("triplestore-configurations")
	private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
	private static Map<String, String> sparqlMap = new LinkedHashMap<>();
	static {
		sparqlMap.put("total", "");
		sparqlMap.put("fresh", "FILTER NOT EXISTS { ?r <" + DCTVocabulary.isReferencedBy + "> ?ref } . ");
		sparqlMap.put("accepted", "?v <" + SOAVocabulary.hasValidation + ">/<" + SOAVocabulary.action + "> <" + SOAVocabulary.Approve + "> . ");
		sparqlMap.put("rejected", "?v <" + SOAVocabulary.hasValidation + ">/<" + SOAVocabulary.action + "> <" + SOAVocabulary.Reject + "> . ");
	}
	
 	public AnnotationsStatistics annotatedItems(AnnotatorStatisticsCube asc, AnnotationsStatistics as, List<String> modes)  {
		
 		Map<String, StatisticsValueCount> map = new LinkedHashMap<>();
 		
 		for (String mode : modes) {
 			String filter = sparqlMap.get(mode);
 			if (filter == null) {
 				continue;
 			}
 			
			String annotatedItemsSparql = 
					"SELECT (COUNT(distinct ?s) AS ?count) " +
			        asc.fromClause + 
			        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
		            "WHERE { " + 
			        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
		    	    "  ?v a <" + OAVocabulary.Annotation + "> . " + 
		            "  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
		            "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
		               filter +
	 	    	       asc.allAnnotatorsFilter +
		            "} ";
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(annotatedItemsSparql, Syntax.syntaxSPARQL_11))) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
	
					int count = sol.get("count").asLiteral().getInt();
					
					StatisticsValueCount svc = map.get(null);
					if (svc == null) {
						svc = StatisticsValueCount.rdfPathCount(null);
						map.put(null, svc);
					}
					
					try {
						Method method = StatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), Integer.class);
						method.invoke(svc, count);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			String annotatedItemsPerFieldSparql = 
					"SELECT (COUNT(distinct ?s) AS ?count) ?property " +
			        asc.fromClause + 
			        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
		            "WHERE { " + 
			        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
		    	    "  ?v a <" + OAVocabulary.Annotation + "> . " + 
		            "  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
		            "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
		            "  ?r <" + SOAVocabulary.onProperty + "> ?property . " +
		               filter +
	 	    	       asc.allAnnotatorsFilter +
		            "} " +
	 	    	    "GROUP BY ?property";
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(annotatedItemsPerFieldSparql, Syntax.syntaxSPARQL_11))) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					int count = sol.get("count").asLiteral().getInt();
					String property = sol.get("property").toString().replaceAll("^a <", "<").replaceAll("> ; <", ">/<");
					
					StatisticsValueCount svc = map.get(property);
					if (svc == null) {
						svc = StatisticsValueCount.rdfPathCount(property);
						map.put(property, svc);
					}
					
					try {
						Method method = StatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), Integer.class);
						method.invoke(svc, count);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
 		}
 		
		for (StatisticsValueCount svc : map.values()) {
			as.addAnnotatedItems(svc);
		}

 		
		return as;
	}      	
 	
 	public AnnotationsStatistics annotations(AnnotatorStatisticsCube asc, AnnotationsStatistics as, List<String> modes)  {
		
 		Map<String, StatisticsValueCount> map = new LinkedHashMap<>();
 		
 		for (String mode : modes) {
 			String filter = sparqlMap.get(mode);
 			if (filter == null) {
 				continue;
 			}
		
 			String annotationsSparql = 
				"SELECT (COUNT(?v) AS ?count) " +
		        asc.fromClause + 
		        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
	            "WHERE { " + 
		        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
	    	    "  ?v a <" + OAVocabulary.Annotation + "> . " + 
	            "  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
	            "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
	               filter +
 	    	       asc.allAnnotatorsFilter +
	            "} ";
		
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(annotationsSparql, Syntax.syntaxSPARQL_11))) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
	
					int count = sol.get("count").asLiteral().getInt();
					
					StatisticsValueCount svc = map.get(null);
					if (svc == null) {
						svc = StatisticsValueCount.rdfPathCount(null);
						map.put(null, svc);
					}
					
					try {
						Method method = StatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), Integer.class);
						method.invoke(svc, count);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			String annotationsPerFieldSparql = 
					"SELECT (COUNT(?v) AS ?count) ?property " +
			        asc.fromClause + 
			        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
		            "WHERE { " + 
			        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
		    	    "  ?v a <" + OAVocabulary.Annotation + "> . " + 
		            "  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
		            "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
		            "  ?r <" + SOAVocabulary.onProperty + "> ?property . " +
		               filter + 
	 	    	       asc.allAnnotatorsFilter +
		            "} " +
	 	    	    "GROUP BY ?property";
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(annotationsPerFieldSparql, Syntax.syntaxSPARQL_11))) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					int count = sol.get("count").asLiteral().getInt();
					String property = sol.get("property").toString().replaceAll("^a <", "<").replaceAll("> ; <", ">/<");
					
					StatisticsValueCount svc = map.get(property);
					if (svc == null) {
						svc = StatisticsValueCount.rdfPathCount(property);
						map.put(property, svc);
					}
					
					try {
						Method method = StatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), Integer.class);
						method.invoke(svc, count);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
 			String distinctAnnotationsSparql = 
				"SELECT (COUNT(DISTINCT ?body) AS ?count) " +
		        asc.fromClause + 
		        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
	            "WHERE { " + 
		        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
	    	    "  ?v a <" + OAVocabulary.Annotation + "> . " +
	    	    "  ?v <" + OAVocabulary.hasBody + "> ?body . " +
	            "  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
	            "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
	               filter +
 	    	       asc.allAnnotatorsFilter +
	            "} ";
		
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(distinctAnnotationsSparql, Syntax.syntaxSPARQL_11))) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
	
					int count = sol.get("count").asLiteral().getInt();
					
					StatisticsValueCount svc = map.get(null);
					if (svc == null) {
						svc = StatisticsValueCount.rdfPathCount(null);
						map.put(null, svc);
					}
					
					try {
						Method method = StatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1) + "Distinct", Integer.class);
						method.invoke(svc, count);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			String distinctAnnotationsPerFieldSparql = 
					"SELECT (COUNT(DISTINCT ?body) AS ?count) ?property " +
			        asc.fromClause + 
			        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
		            "WHERE { " + 
			        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
		    	    "  ?v a <" + OAVocabulary.Annotation + "> . " + 
		    	    "  ?v <" + OAVocabulary.hasBody + "> ?body . " +
		            "  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
		            "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
		            "  ?r <" + SOAVocabulary.onProperty + "> ?property . " +
		               filter + 
	 	    	       asc.allAnnotatorsFilter +
		            "} " +
	 	    	    "GROUP BY ?property";
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(distinctAnnotationsPerFieldSparql, Syntax.syntaxSPARQL_11))) {
				
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					int count = sol.get("count").asLiteral().getInt();
					String property = sol.get("property").toString().replaceAll("^a <", "<").replaceAll("> ; <", ">/<");
					
					StatisticsValueCount svc = map.get(property);
					if (svc == null) {
						svc = StatisticsValueCount.rdfPathCount(property);
						map.put(property, svc);
					}
					
					try {
						Method method = StatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1) + "Distinct", Integer.class);
						method.invoke(svc, count);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

 		}
 		
		for (StatisticsValueCount svc : map.values()) {
			as.addAnnotations(svc);
		}
		
 		return as;
	}      	 	
 	
 	public AnnotationsStatistics mostFrequentAnnotations(int top, AnnotatorStatisticsCube asc, AnnotationsStatistics as, List<String> modes)  {
		
 		Map<String, ComplexStatisticsValueCount> map = new LinkedHashMap<>();
 		
 		for (String mode : modes) {
 			String filter = sparqlMap.get(mode);
 			if (filter == null) {
 				continue;
 			}

			String annotatedItemsSparql = 
					"SELECT ?body (COUNT(?body) AS ?count) " +
			        asc.fromClause + 
			        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
		            "WHERE { " + 
			        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
		    	    "  ?v a <" + OAVocabulary.Annotation + "> . " + 
		    	    "  ?v <" + OAVocabulary.hasBody + "> ?body . " +
		            "  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
		            "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
		               filter +
	 	    	       asc.allAnnotatorsFilter +
		            "} " +
	 	    	    "GROUP BY ?body " + 
		            "ORDER BY DESC(?count) LIMIT " + top;
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(annotatedItemsSparql, Syntax.syntaxSPARQL_11))) {
				
				ResultSet rs = qe.execSelect();
				
				List<StatisticsValueCount> list = new ArrayList<>();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
	
					int count = sol.get("count").asLiteral().getInt();
					String body = sol.get("body").toString();
					
					StatisticsValueCount svc = StatisticsValueCount.uriCount(body);
					svc.setCount(count);
					
					list.add(svc);
				}
				
				ComplexStatisticsValueCount csvc = map.get(null);
				if (csvc == null) {
					csvc = new ComplexStatisticsValueCount(null);
					map.put(null, csvc);
				}

				if (list.size() > 0 ) {
					try {
						Method method = ComplexStatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), List.class);
						method.invoke(csvc, list);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			
			for (Map.Entry<String,String> entry : asc.perFieldAnnotatorsFilter.entrySet()) {
				String annotatedItemsPerFieldSparql = 
						"SELECT ?body (COUNT(?body) AS ?count) " +
				        asc.fromClause + 
				        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
			            "WHERE { " + 
				        "  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
			    	    "  ?v a <" + OAVocabulary.Annotation + "> . " + 
			    	    "  ?v <" + OAVocabulary.hasBody + "> ?body . " +
			    	    "  ?v <" + OAVocabulary.hasTarget + "> ?r ." +
			    	    "  ?r <" + OAVocabulary.hasSource + "> ?s . " +
			    	    "  ?r  <" + SOAVocabulary.onProperty + "> \"" + entry.getKey() + "\" . " +
			    	       filter +
		 	    	       asc.allAnnotatorsFilter +
			            "} " +
		 	    	    "GROUP BY ?body " + 
			            "ORDER BY DESC(?count) LIMIT " + top;
				
				
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(annotatedItemsPerFieldSparql, Syntax.syntaxSPARQL_11))) {
					
					ResultSet rs = qe.execSelect();
					
					List<StatisticsValueCount> list = new ArrayList<>();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
	
						int count = sol.get("count").asLiteral().getInt();
						String body = sol.get("body").toString();
						
						
						StatisticsValueCount svc = StatisticsValueCount.uriCount(body);
						svc.setCount(count);
						
						list.add(svc);
						
					}
					
					String property = entry.getKey().replaceAll("^a <", "<").replaceAll("> ; <", ">/<");
	
					ComplexStatisticsValueCount csvc = map.get(property);
					if (csvc == null) {
						csvc = new ComplexStatisticsValueCount(property);
						map.put(property, csvc);
					}
					
					if (list.size() > 0 ) {
						try {
							Method method = ComplexStatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), List.class);
							method.invoke(csvc, list);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
 		}
 		
		for (ComplexStatisticsValueCount csvc : map.values()) {
			as.addMostFrequentAnnotations(csvc);
		}
 		
		return as;
	}
 	
 	
	public AnnotationsStatistics computeValidationDistribution(int accuracy, AnnotatorStatisticsCube asc, AnnotationsStatistics as, List<String> modes) {

//    	List<Map<String, Object>> result = new ArrayList<>();
//
 		Map<String, DistributionStatisticsValueCount> map = new LinkedHashMap<>();
 		
		List<double[]> ranges = new ArrayList<>();
		
		ranges.add(new double [] {0, 1});
		
		for (int i = 0; i < 100/accuracy; i++) {
			ranges.add(new double [] {i*accuracy/(double)100, (i + 1)*accuracy/(double)100 });
		}
		
 		for (String mode : modes) {
 			String filter = sparqlMap.get(mode);
 			if (filter == null) {
 				continue;
 			}    	
 			
 			{
    		List<Distribution> list = new ArrayList<>();
    		
	    	for (double[] range : ranges) {
		    	String sparql = 
		    			"SELECT (count(*) AS ?count) (avg(?score) AS ?avgScore) " +
		    	        asc.fromClause +
		    	        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
		    			"WHERE { " +
		    			"  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
//			    			"  ?s " + onPropertyString + " ?value }  " +
		    			"  ?v a <" + OAVocabulary.Annotation + ">  . " +
		    			"  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
		        		"  ?r <" + OAVocabulary.hasSource + "> ?s . " +
//			        		"  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" . " + 
//			        		"  ?r <" + SOAVocabulary.onValue + "> ?value . " +
		    			"  ?v <" + SOAVocabulary.score + "> ?score . " +
			    	       filter +
		 	    	       asc.allAnnotatorsFilter +
		 	    	    "  FILTER ( ?score " + (range[0] == 0 ? ">= ": "> ") + range[0] + " && ?score <= " + range[1] + ") " +
		        		"}  ";

		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {

					ResultSet results = qe.execSelect();
					
					while (results.hasNext()) {
						QuerySolution qs = results.next();
						
						Distribution d = new Distribution();
						d.setCount(qs.get("count").asLiteral().getInt());
						if (qs.get("avgScore") != null) {
							d.setAverageScore(qs.get("avgScore").asLiteral().getDouble());
						}
						d.setLowerBound(range[0]);
						d.setLowerBoundIncluded(range[0] == 0);
						d.setUpperBound(range[1]);
						
						list.add(d);
					}
		    	}
		    	
	    	}
	    	
			DistributionStatisticsValueCount csvc = map.get(null);
			if (csvc == null) {
				csvc = new DistributionStatisticsValueCount(null);
				map.put(null, csvc);
			}

			for (Distribution d : list) {
				if (d.getCount() != 0) {
					try {
						Method method = DistributionStatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), List.class);
						method.invoke(csvc, list);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					break;
				}
			}
 			}
			
			for (Map.Entry<String,String> entry : asc.perFieldAnnotatorsFilter.entrySet()) {
	    		
				List<Distribution> list = new ArrayList<>();
	    		
		    	for (double[] range : ranges) {
			    	String sparql = 
			    			"SELECT (count(*) AS ?count) (avg(?score) AS ?avgScore) " +
			    	        asc.fromClause +
			    	        "FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " +
			    			"WHERE { " +
			    			"  ?s a ?something . " + //ensure data item exists may not work correctly in virtuoso
			    			"  ?v a <" + OAVocabulary.Annotation + ">  . " +
			    			"  ?v <" + OAVocabulary.hasTarget + "> ?r . " +
			        		"  ?r <" + OAVocabulary.hasSource + "> ?s . " +
			        		"  ?r  <" + SOAVocabulary.onProperty + "> \"" + entry.getKey() + "\" . " +
			    			"  ?v <" + SOAVocabulary.score + "> ?score . " +
				    	       filter +
			 	    	       asc.allAnnotatorsFilter +
			        		"  FILTER ( ?score " + (range[0] == 0 ? ">= ": "> ") + range[0] + " && ?score <= " + range[1] + ") " +
			        		"}  ";
			    	
//			    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
			    	
			    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(asc.psv.getTripleStoreConfiguration().getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {

						ResultSet results = qe.execSelect();
						
						while (results.hasNext()) {
							QuerySolution qs = results.next();
							
							Distribution d = new Distribution();
							d.setCount(qs.get("count").asLiteral().getInt());
							if (qs.get("avgScore") != null) {
								d.setAverageScore(qs.get("avgScore").asLiteral().getDouble());
							}
							d.setLowerBound(range[0]);
							d.setLowerBoundIncluded(range[0] == 0);
							d.setUpperBound(range[1]);
							d.setUpperBoundIncluded(true);
							
							list.add(d);
						}
			    	}
			    	
		    	}
		    	
		    	String property = entry.getKey().replaceAll("^a <", "<").replaceAll("> ; <", ">/<");
				
		    	DistributionStatisticsValueCount csvc = map.get(property);
				
		    	if (csvc == null) {
					csvc = new DistributionStatisticsValueCount(property);
					map.put(property, csvc);
				}

				for (Distribution d : list) {
					if (d.getCount() != 0) {
						try {
							Method method = DistributionStatisticsValueCount.class.getMethod("set" + mode.substring(0,1).toUpperCase() + mode.substring(1), List.class);
							method.invoke(csvc, list);
						} catch (Exception e) {
							e.printStackTrace();
						}
						
						break;
					}
				}
			}

    	}
 		
		for (DistributionStatisticsValueCount csvc : map.values()) {
			as.addScoreDistribution(csvc);
		}
 
    	return as;
	}
}
