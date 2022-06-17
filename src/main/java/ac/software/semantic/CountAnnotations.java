package ac.software.semantic;

import java.util.ArrayList;
import java.util.HashMap;
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
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.User;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.UserRepository;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;

@Service
public class CountAnnotations  {

	@Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfiguration;

	
	@Autowired
	UserRepository userRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	AnnotatorDocumentRepository annotatorRepository;

	@Autowired
	PagedAnnotationValidationRepository pavRepository;

	@Autowired
	FilterAnnotationValidationRepository favRepository;

	public void statsPerProvider(String virtuoso) throws Exception {
		
		for (String s : new String[] { "europeanafashion", "europeanfilmgateway", "photoconsortium", "judaica", "euscreen", "pagode" }) {
			User user = userRepository.findByEmail(s).get();
			String v = "VALUES ?generator { ";
			for (Dataset ds : datasetRepository.findByUserId(user.getId())) {
				for (AnnotatorDocument ad : annotatorRepository.findByDatasetUuid(ds.getUuid())) {
					v += "<" + SEMAVocabulary.getAnnotator(ad.getUuid()) + "> ";
				}

				for (PagedAnnotationValidation ad : pavRepository.findByDatasetUuid(ds.getUuid())) {
					v += "<" + SEMAVocabulary.getAnnotationValidator(ad.getUuid()) + "> ";
				}

				for (FilterAnnotationValidation ad : favRepository.findByDatasetUuid(ds.getUuid())) {
					v += "<" + SEMAVocabulary.getAnnotationValidator(ad.getUuid()) + "> ";
				}

			}	
			v += " }";
			System.out.println(s);
//			System.out.println(v);
			
			String sparql1 = "SELECT (count(?annotation) AS ?count) " + 
					"FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> " + 
					"FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " + 
					"{ " + 
					"?annotation  a                  <http://www.w3.org/ns/oa#Annotation> . " + 
					"?annotation  <http://www.w3.org/ns/oa#hasTarget>/<http://www.w3.org/ns/oa#hasSource>  ?source . " +
			        "FILTER NOT EXISTS { ?annotation <http://sw.islab.ntua.gr/annotation/hasValidation>/<http://sw.islab.ntua.gr/annotation/action> <http://sw.islab.ntua.gr/annotation/Delete> } . " + 
					"?annotation <https://www.w3.org/ns/activitystreams#generator> ?generator." + 
					v +
					" }";
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(sparql1, Syntax.syntaxSPARQL_11))) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					System.out.println("Annotations count: " + sol.get("count").asLiteral().getInt());
				}

			}
			
			String sparql2 = "SELECT (count(DISTINCT ?source) AS ?count) " + 
					"FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> " + 
					"FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " + 
					"{ " + 
					"?annotation  a                  <http://www.w3.org/ns/oa#Annotation> . " + 
					"?annotation  <http://www.w3.org/ns/oa#hasTarget>/<http://www.w3.org/ns/oa#hasSource>  ?source . " +
			        "FILTER NOT EXISTS { ?annotation <http://sw.islab.ntua.gr/annotation/hasValidation>/<http://sw.islab.ntua.gr/annotation/action> <http://sw.islab.ntua.gr/annotation/Delete> } . " + 
					"?annotation <https://www.w3.org/ns/activitystreams#generator> ?generator." + 
					v +
					" }";
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(sparql2, Syntax.syntaxSPARQL_11))) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					System.out.println("Annotated items count: " + sol.get("count").asLiteral().getInt());
				}

			}

		}
	}
	
	public void statsPerProviderAndType(String virtuoso) throws Exception {
		
		for (String s : new String[] { "europeanafashion", "europeanfilmgateway", "photoconsortium", "judaica", "euscreen", "pagode" }) {
			Map<String, List<String>> map = new HashMap<>();
			
			User user = userRepository.findByEmail(s).get();
			for (Dataset ds : datasetRepository.findByUserId(user.getId())) {
				for (AnnotatorDocument ad : annotatorRepository.findByDatasetUuid(ds.getUuid())) {
					List<String> anns = map.get(ad.getAnnotator());
					if (anns == null) {
						anns = new ArrayList<>();
						map.put(ad.getAnnotator(), anns);
					}
					anns.add(ad.getUuid());
				}
			}

			for (Map.Entry<String, List<String>> entry : map.entrySet()) {
				String v = "VALUES ?generator { ";
				for (String uuid : entry.getValue()) {
					v += "<" + SEMAVocabulary.getAnnotator(uuid) + "> ";
				}
				v += " }";
				
				System.out.println(s + " " + entry.getKey());
	//			System.out.println(v);

				String sparql = "SELECT (count(?annotation) AS ?count) " + 
						"FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> " + 
						"FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " + 
						"{ " + 
						"?annotation  a                  <http://www.w3.org/ns/oa#Annotation> . " + 
						"?annotation  <http://www.w3.org/ns/oa#hasTarget>/<http://www.w3.org/ns/oa#hasSource>  ?source . " +
//				        "FILTER NOT EXISTS { ?annotation <http://sw.islab.ntua.gr/annotation/hasValidation>/<http://sw.islab.ntua.gr/annotation/action> <http://sw.islab.ntua.gr/annotation/Delete> } . " + 
						"?annotation <https://www.w3.org/ns/activitystreams#generator> ?generator." + 
						v +
						" }";
				
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
					ResultSet rs = qe.execSelect();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						
						System.out.println("Created annotations count: " + sol.get("count").asLiteral().getInt());
					}
	
				}
				
				sparql = "SELECT (count(?annotation) AS ?count) " + 
						"FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> " + 
						"FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " + 
						"{ " + 
						"?annotation  a                  <http://www.w3.org/ns/oa#Annotation> . " + 
						"?annotation  <http://www.w3.org/ns/oa#hasTarget>/<http://www.w3.org/ns/oa#hasSource>  ?source . " +
				        "?annotation <http://sw.islab.ntua.gr/annotation/hasValidation>/<http://sw.islab.ntua.gr/annotation/action> <http://sw.islab.ntua.gr/annotation/Delete>  . " + 
						"?annotation <https://www.w3.org/ns/activitystreams#generator> ?generator." + 
						v +
						" }";
				
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
					ResultSet rs = qe.execSelect();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						
						System.out.println("Deleted annotations count: " + sol.get("count").asLiteral().getInt());
					}
	
				}
				
//				String sparql2 = "SELECT (count(DISTINCT ?source) AS ?count) " + 
//						"FROM <http://sw.islab.ntua.gr/semaspace/ontology/term> " + 
//						"FROM <http://sw.islab.ntua.gr/semaspace/ontology/place> " + 
//						"{ " + 
//						"?annotation  a                  <http://www.w3.org/ns/oa#Annotation> . " + 
//						"?annotation  <http://www.w3.org/ns/oa#hasTarget>/<http://www.w3.org/ns/oa#hasSource>  ?source . " +
//				        "FILTER NOT EXISTS { ?annotation <http://sw.islab.ntua.gr/annotation/hasValidation>/<http://sw.islab.ntua.gr/annotation/action> <http://sw.islab.ntua.gr/annotation/Delete> } . " + 
//						"?annotation <https://www.w3.org/ns/activitystreams#generator> ?generator." + 
//						v +
//						" }";
//				
//				try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(), QueryFactory.create(sparql2, Syntax.syntaxSPARQL_11))) {
//					ResultSet rs = qe.execSelect();
//					
//					while (rs.hasNext()) {
//						QuerySolution sol = rs.next();
//						
//						System.out.println("Annotated items count: " + sol.get("count").asLiteral().getInt());
//					}
//	
//				}
			}
		}
	}

}
