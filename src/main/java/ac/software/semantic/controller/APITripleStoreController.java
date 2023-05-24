package ac.software.semantic.controller;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.TripleStoreResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Triple Stores API")
@RestController
@RequestMapping("/api/triple-stores")
public class APITripleStoreController {

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
    @Qualifier("database")
    private Database database;
	
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

	
    @GetMapping(value = "/get-all",
            produces = "application/json")
	public ResponseEntity<?> getTripleStores(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
		try {
			List<TripleStoreResponse> res = virtuosoConfigurations.values().stream().map(ts -> modelMapper.tripleStore2TripleStoreResponse(ts)).collect(Collectors.toList());
			
			return ResponseEntity.ok(res);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	 
	}          
    
    @GetMapping(value = "/get-info/{id}",
                produces = "application/json")
	public ResponseEntity<?> getTripleStoreInfo(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id)  {
		try {
			
			TripleStoreConfiguration vc = virtuosoConfigurations.getById(id);
			
			if (vc == null) {
				return new ResponseEntity<>(APIResponse.FailureResponse(), HttpStatus.NOT_FOUND);
			}
			
			TripleStoreResponse vcr = modelMapper.tripleStore2TripleStoreResponse(vc);
					
			String triplesCountSparqlQuery = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }";
			int triplesCount = 0;
			ResultSet rs;
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(triplesCountSparqlQuery, Syntax.syntaxSPARQL_11))) {
				rs = qe.execSelect();
	
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					triplesCount = sol.get("count").asLiteral().getInt();
				}
			}
			vcr.setTriplesCount(triplesCount);

			String graphCountSparqlQuery = "SELECT (COUNT(DISTINCT ?g) AS ?count) { GRAPH ?g { ?s ?p ?o } }";
			int graphCount = 0;
			try (QueryExecution qe2 = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(graphCountSparqlQuery, Syntax.syntaxSPARQL_11))) {
				ResultSet rs2 = qe2.execSelect();
	
				while (rs2.hasNext()) {
					QuerySolution sol2 = rs2.next();
					graphCount = sol2.get("count").asLiteral().getInt();
				}
			}
			vcr.setGraphCount(graphCount);
			
			return ResponseEntity.ok(vcr);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	 
	}      
    
}
