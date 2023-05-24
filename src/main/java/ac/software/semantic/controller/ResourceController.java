package ac.software.semantic.controller;

import java.net.URI;
import java.net.URLEncoder;

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.LodViewConfiguration;
import ac.software.semantic.model.TripleStoreConfiguration;

@RestController
@RequestMapping("/resource")
public class ResourceController {

    @Value("${backend.server}")
    private String server;
    
    @Autowired
    @Qualifier("database")
    private Database database;
    
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
    
    @Autowired
    @Qualifier("lodview-configuration")
    private LodViewConfiguration lodViewConfiguration;
    
    @GetMapping(value = "/**")
    public ResponseEntity<?> getResource(HttpServletRequest request) {
        String resource = database.getResourcePrefix() + request.getRequestURI().substring(10);
    	
    	String sparql = "SELECT distinct ?g WHERE { GRAPH ?g { <" + resource + "> ?q ?r } } ";
    	
    	TripleStoreConfiguration cvc = null;
    	String graph = null;
    	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
			try (QueryExecution  qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql))) {
				ResultSet vs =  qe.execSelect();
				if (vs.hasNext()) {
					QuerySolution sol = vs.next();
	
					graph = sol.get("g").asResource().toString();
					cvc = vc;
					break;
				}
			}
		}
    	
    	if (cvc == null) {
    		return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
    	}
    	
    	String endpoint = server + "/api/content/" + database.getName() + "/" + cvc.getOrder() + "/" + cvc.getGraphIdentifier(graph) + "/sparql";
    	
    	try {
	    	URI externalUri = new URI(lodViewConfiguration.getBaseUrl() + "queryResource?iri=" + URLEncoder.encode(resource) + "&endpoint=" + URLEncoder.encode(endpoint));
	        HttpHeaders httpHeaders = new HttpHeaders();
	        httpHeaders.setLocation(externalUri);
	
	        return new ResponseEntity<>(httpHeaders, HttpStatus.SEE_OTHER);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    	}
    }

}
