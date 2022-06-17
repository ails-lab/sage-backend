package ac.software.semantic.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import ac.software.semantic.config.VocabulariesBean;
import ac.software.semantic.config.VocabularyInfo;
import ac.software.semantic.model.VirtuosoConfiguration;
import edu.ntua.isci.ac.common.utils.Utils;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import io.swagger.v3.oas.annotations.tags.Tag;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

@Tag(name = "Content API")

@RestController
@RequestMapping("/api/content")
public class APIContentController {

    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfiguration;
    
	@Autowired
	@Qualifier("all-datasets")
	private VocabulariesBean vocs;

//	@Autowired
//	@Qualifier("all-prefixes")
//	private Set<URIDescriptor> prefixes;
	
//    @GetMapping(value = "/dataset/{identifier}")
//    public ResponseEntity<?> redirect(@PathVariable("identifier") String identifier, @RequestParam Map<String,String> input){
// 
//    	String sparql =  "SELECT ?graph FROM <" + SEMAVocabulary.contentGraph + "> WHERE { ?graph <" + DCVocabulary.identifier + "> \"" + identifier + "\" } ";
//		
//    	Resource graph = null;
//    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(), sparql)) {
//			ResultSet rs = qe.execSelect();
//			
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//				graph = sol.get("graph").asResource();
//				break;
//			}
//		}
//    	
//    	if (graph == null) {
//    		return ResponseEntity.notFound().build();
//    	}
//    	
//    	UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(virtuosoConfiguration.getSparqlEndpoint());
//    	
//    	builder.queryParam("default-graph-uri", graph.toString());
//    			
//    	for (Map.Entry<String,String> entry : input.entrySet()) {
//    		if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
//    			continue;
//    		}
//    		
//    		builder = builder.queryParam(entry.getKey(), entry.getValue());
//    	}
//    	
//    	UriComponents uri = builder.build();
//    	
////    	System.out.println(uri.toUri());
//    	
//    	RestTemplate restTemplate = new RestTemplate();
//    	
//    	return restTemplate.getForEntity(uri.toUri(), String.class);
//    	
//
//    }
    
	@Autowired
	@Qualifier("endpoints-cache")
	private Cache endpointsCache;
	
    @GetMapping(value = "/{identifier}/sparql")
    public ResponseEntity<?> getSparql(HttpServletRequest request, @PathVariable("identifier") String identifier) throws Exception {
//    	System.out.println("GET");
    	return sparql(request, identifier, true);
    }
    
    @PostMapping(value = "/{identifier}/sparql", consumes = "application/sparql-query")
    public ResponseEntity<?> spostSparql(HttpServletRequest request, @PathVariable("identifier") String identifier) throws Exception {
//    	System.out.println("POST-SPARQL");
    	return sparql(request, identifier, true); 
    }

    @PostMapping(value = "/{identifier}/sparql", consumes = "application/x-www-form-urlencoded")
    public ResponseEntity<?> xpostSparql(HttpServletRequest request, @PathVariable("identifier") String identifier) throws Exception {
//    	System.out.println("POST-FORM");    	
    	return sparql(request, identifier, false); 
    }

    private class DataLocation {
    	public Resource graph;
    	public VirtuosoConfiguration vc;
    	
    	DataLocation(Resource graph, VirtuosoConfiguration vc) {
    		this.graph = graph;
    		this.vc = vc;
    	}
    }
    
    private DataLocation getGraph(String identifier) {
		Element e = endpointsCache.get(identifier);
		if (e != null) {
			return (DataLocation)e.getObjectValue();
		}

    	String sparql =  "SELECT ?graph FROM <" + SEMAVocabulary.contentGraph + "> WHERE { ?graph <" + DCTVocabulary.identifier + "> \"" + identifier + "\" } ";
		
    	DataLocation dl = null;
    	
    	loop:
    	for (VirtuosoConfiguration ivc : virtuosoConfiguration.values()) {
	    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(ivc.getSparqlEndpoint(), sparql)) {
	//    	try (QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.118.1:7200/repositories/stirdata", sparql)) {
	    	
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					dl = new DataLocation(sol.get("graph").asResource(), ivc);
					break loop;
				}
	    	}
	    }
    	
    	endpointsCache.put(new Element(identifier, dl));
    	
    	return dl;
    }
    
    public ResponseEntity<?> sparql(HttpServletRequest request, String identifier, boolean queryStringParams) throws Exception {

//    	System.out.println(request.getQueryString());
    	DataLocation dl = getGraph(identifier);
//    	System.out.println("GRAPH > " + graph);
    	
    	if (dl == null) {
    		return ResponseEntity.notFound().build();
    	}

//    	UriComponentsBuilder does not encode correctly!
//		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(virtuosoConfiguration.getSparqlEndpoint());
    	StringBuffer uri = new StringBuffer(dl.vc.getSparqlEndpoint());

		String stringBody = null;
		MultiValueMap<String, String> formBody = null;
		
    	if (queryStringParams) {
//    		builder.queryParam("default-graph-uri", graph.toString());
       		uri.append("?default-graph-uri=" + URLEncoder.encode(dl.graph.toString())); // it will fail if length of URI is exceeded!!!
//    			
	    	for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
	    		if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
	    			continue;
	    		}
//	    		
//	    		Object[] obj = new Object[entry.getValue().length];
	    		for (int i = 0; i < entry.getValue().length; i++) {
//	    			obj[i] = entry.getValue()[i];
	    			
            		uri.append("&" + entry.getKey() + "=" + URLEncoder.encode(entry.getValue()[i]));
	    		}
//	    		
//	    		builder = builder.queryParam(entry.getKey(), obj);
	    	}
	    	
	    	
			try {
				stringBody = IOUtils.toString(request.getInputStream(), Charset.forName(request.getCharacterEncoding()));
			} catch (IOException e1) {
				e1.printStackTrace();
				return ResponseEntity.badRequest().build();
			}
			
//    		System.out.println("STRING");
//    		System.out.println(stringBody);
//    		System.out.println(builder);

    	} else {
//    		request.getParameterMap();
    		
    		formBody = new LinkedMultiValueMap<String, String>();
    		for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
    			if (entry.getKey().equalsIgnoreCase("default-graph-uri")) {
    				continue;
    			}
    			formBody.addAll(entry.getKey(), Arrays.asList(entry.getValue()));
    		}
    		
    		formBody.add("default-graph-uri", dl.graph.getURI());
    		
//    		System.out.println("FORM");
//    		System.out.println(formBody);
    		
    	}
    	
//    	UriComponents uri = builder.build();
    	
    	RestTemplate restTemplate = new RestTemplate();
    	
//    	System.out.println(" " + uri.toUri());
//    	System.out.println(" " + uri.toString());
//		System.out.println("BODY " + body);
//		System.out.println("METHOD " + request.getMethod());
		
		HttpHeaders headers = new HttpHeaders();

    	Enumeration<String> en = request.getHeaderNames();
    	while (en.hasMoreElements()) {
    		String header = en.nextElement();
    		if (header.equalsIgnoreCase("accept-language") || header.equalsIgnoreCase("accept-encoding")) {
    			continue;
    		}

        	headers.add(header, request.getHeader(header));
    	}
    	
        try {
	    	ResponseEntity<String> s = restTemplate.exchange(
//	    			uri.toUri(),
	    			new URI(uri.toString()),
	    			HttpMethod.valueOf(request.getMethod()), 
	    			stringBody != null ? new HttpEntity<>(stringBody, headers) : new HttpEntity<>(formBody, headers)  , 
	    			String.class);
	    	
//	    	System.out.println(s);
	    	return s;
    	
        } catch (final HttpClientErrorException e) {
        	e.printStackTrace();
            return new ResponseEntity<>(e.getResponseBodyAsByteArray(), e.getResponseHeaders(), e.getStatusCode());
        }

    }
    
    @GetMapping(value = "/{identifier}/view",
//    		    produces = "text/plain; charset=UTF-8")
    		    produces = "text/turtle; charset=UTF-8")
    public ResponseEntity<?> view(HttpServletRequest request, @PathVariable("identifier") String identifier, @RequestParam("uri") String uri, @RequestParam(defaultValue = "turtle", name = "format") String format) {

    	String sparql =  "SELECT ?graph FROM <" + SEMAVocabulary.contentGraph + "> WHERE { ?graph <" + DCTVocabulary.identifier + "> \"" + identifier + "\" } ";
		
    	DataLocation dl = null;
    	
    	loop:
    	for (VirtuosoConfiguration ivc : virtuosoConfiguration.values()) { 
	    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(ivc.getSparqlEndpoint(), sparql)) {
	//       	try (QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.118.1:7200/repositories/stirdata", sparql)) {
	
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					dl = new DataLocation(sol.get("graph").asResource(), ivc);
					break loop;
				}
			}
    	}
    	
    	if (dl == null) {
    		return ResponseEntity.notFound().build();
    	}

    	StringBuffer res = new StringBuffer();
    	
		List<String> uris = new ArrayList<>();
		uris.add(uri);
		
		for (int i = 0; i < uris.size(); i++) {
			String s = readEntity(dl.vc, dl.graph.getURI(), uri, uris.get(i), uris, format);
			if (s.length() > 0) {
				if (i == 0) {
					res.append(s);
				} else {
					res.append("\n\n");
					res.append(s);
				}
			}
		}
		
		return ResponseEntity.ok(res.toString());    	

    }
    
    public static String findPrefix(String c, Set<String> prefixes) {
    	String result = null;
    	
    	for (String prefix : prefixes) {
    		Pattern p = Pattern.compile("^(" + Matcher.quoteReplacement(prefix) + ")");
    		
    		Matcher m = p.matcher(c);
    		if (m.find()) {
    			String f = m.group(1);
    			if (result == null || f.length() > result.length()) {
    				result = prefix;
    			}
    		}
    	}

    	return result;
    }
    
    @GetMapping(value = "/view",
		    produces = "text/turtle; charset=UTF-8")
	public ResponseEntity<?> cview(HttpServletRequest request, @RequestParam("uri") String uri, @RequestParam(defaultValue = "turtle", name = "format") String format) {

    	StringBuffer res = new StringBuffer();
    	
    	if (!(uri.startsWith("http://") || uri.startsWith("https://"))) {
    		return ResponseEntity.ok(res.toString());
    	}
    	
//    	System.out.println(vocs.getMap());

    	String prefix = findPrefix(uri, vocs.getMap().keySet());
    	
//    	System.out.println(prefix);

    	if (prefix == null) {
    		return ResponseEntity.ok(res.toString());
    	}
    	
    	VocabularyInfo vi = vocs.getMap().get(prefix);
    	
    	if (vi == null) {
    		return ResponseEntity.ok(res.toString());
    	}
		
		List<String> uris = new ArrayList<>();
		uris.add(uri);
		
//		System.out.println("VI " + vi.getVirtuoso());
//		System.out.println("VI " + vi.getGraph());
//		System.out.println("VI " + uris);
		
		for (int i = 0; i < uris.size(); i++) {
			String s = readEntity(vi.getVirtuoso(), vi.getGraph(), uri, uris.get(i), uris, format);
			if (s.length() > 0) {
				if (i == 0) {
					res.append(s);
				} else {
					res.append("\n\n");
					res.append(s);
				}
			}
		}
		
		return ResponseEntity.ok(res.toString());    	
	
	}
    
    private String readEntity(VirtuosoConfiguration vc, String graph, String rootUri, String uri, List<String> uris, String format) {
		
    	String rUri = (rootUri.endsWith("/") || rootUri.endsWith("#")) ? uri.substring(0, uri.length() - 1) : uri; 
    			
		String sparql = "SELECT DISTINCT ?o FROM <" + graph + "> WHERE { <" + uri + "> ?p ?o . FILTER(isIRI(?o)) } ORDER BY ?o";
		
//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.118.1:7200/repositories/stirdata", QueryFactory.create(sparql, Syntax.syntaxARQ))) {

			ResultSet results = qe.execSelect();
			
			while (results.hasNext()) {
				QuerySolution qs = results.next();
	
				String newUri = qs.get("o").asResource().getURI();
				
				if ((newUri.startsWith(rUri + "/") || newUri.startsWith(rUri + "#")) && !uris.contains(newUri)) {
					uris.add(newUri);
				}
			}
		}

		StringBuffer sb = null;
		int i;
		for (i = 1; ; i++) {
			sb = new StringBuffer();
			
//			sb.append("SELECT COUNT(DISTINCT ?o" + i + ")  FROM <" + graph + "> WHERE { <" + uri + "> ?p1 ?o1 . ");
			sb.append("SELECT (COUNT(DISTINCT ?o" + i + ") AS ?x) FROM <" + graph + "> WHERE { <" + uri + "> ?p1 ?o1 . ");
			for (int j = 2; j <= i; j++) {
				sb.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
			}
			for (int j = 2; j <= i; j++) {
				sb.append(" } ");
			}
			
			sb.append("}");
			
//			System.out.println(QueryFactory.create(sb.toString(), Syntax.syntaxARQ));
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sb.toString(), Syntax.syntaxARQ))) {
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.118.1:7200/repositories/stirdata", QueryFactory.create(sb.toString(), Syntax.syntaxARQ))) {

				ResultSet results = qe.execSelect();
				String name = results.getResultVars().get(0);
				
				QuerySolution qs = results.next();
	
				if (qs.get(name).asLiteral().getInt() == 0) {
					break;
				}
			}
			
		}

		
		StringBuffer db = new StringBuffer();

		db.append("CONSTRUCT { ?s ?p1 ?o1 . ");
		for (int j = 2; j < i; j++) {
			db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
		}
		db.append("} FROM <" + graph + "> WHERE { VALUES ?s { <" + uri + "> }  ?s ?p1 ?o1 . ");
		for (int j = 2; j < i; j++) {
			db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
		}			
		for (int j = 2; j < i; j++) {
			db.append(" } ");
		}
		db.append("}");
				
		
		Model model = ModelFactory.createDefaultModel();
		
		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(db.toString(), Syntax.syntaxARQ))) {
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.118.1:7200/repositories/stirdata", QueryFactory.create(db.toString(), Syntax.syntaxARQ))) {
			
			Model md = qe.execConstruct();
			model.add(md.listStatements());
		}
		
		StringWriter sw = new StringWriter();
		try {
			model.write(sw, format);
		} catch (org.apache.jena.riot.RiotException e) {
			model.write(sw, "turtle");
		}
		
		return sw.toString();
		
    }
    
}
