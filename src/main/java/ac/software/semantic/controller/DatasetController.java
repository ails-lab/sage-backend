package ac.software.semantic.controller;

import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import ac.software.semantic.model.*;
import ac.software.semantic.repository.UserRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.payload.SearchRequest;
import ac.software.semantic.repository.AccessRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AccessService;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SACCVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.semaspace.query.Searcher;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Dataset Functions")
@RestController
@RequestMapping("/api/f/datasets")
public class DatasetController {

	@Autowired
	@Qualifier("virtuoso-configuration")
	private Map<String,VirtuosoConfiguration> virtuosoConfigurations;

    @Autowired
    private AccessRepository accessRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
	private UserRepository userRepository;

	@Autowired
	@Qualifier("searcher")
	private Searcher searcher;
	
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

	@GetMapping(value = "/getAll", produces = "application/json")
	public String getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("typeUri") List<String> typeUri, @RequestParam("root") Optional<String> root)
			throws Exception {

//		System.out.println(currentUser);
		
		String type = "";
		for (String s: typeUri) {
			if (type.length() > 0) {
				type += " UNION ";
			}
			type += "{ ?url a  <" + s + "> } ";
		}
		
		String sparql = legacyUris ? 
				"CONSTRUCT { ?url a ?type . " + " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . "
				+ " ?url <http://sw.islab.ntua.gr/semaspace/model/target> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/language> ?language . "
				+ " ?url <http://sw.islab.ntua.gr/semaspace/model/scheme> ?scheme . "
				+ " ?url <http://purl.org/dc/elements/1.1/hasPart> ?part . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
				+ SEMAVocabulary.contentGraph + "> { " + type + ".  ?url a ?type ."
				+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/language> ?language } ."
				+ " OPTIONAL { ?url <http://sw.islab.ntua.gr/semaspace/model/scheme> ?scheme } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } . "
				+ " OPTIONAL { ?url <http://purl.org/dc/terms/hasPart> ?part }   . "
				+ (root.isPresent() ? "{ <" + root.get() + "> <http://purl.org/dc/terms/hasPart> ?url } UNION " + // ATTN: should have <http://purl.org/dc/terms/hasPart>* but it is not working : limit to one level !
				                      "{ BIND(<" + root.get() + "> AS ?url) }"  : "")  
				+ " OPTIONAL { ?alurl <http://sw.islab.ntua.gr/semaspace/model/source> ?url . "
				+ "  ?alurl <http://sw.islab.ntua.gr/semaspace/model/target>/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } }"
				+ " GRAPH <" + SEMAVocabulary.accessGraph + "> { "
                + " ?group <http://sw.islab.ntua.gr/semaspace/access/dataset> ?url . "				
				+ (currentUser != null ? 
				  " ?group <http://sw.islab.ntua.gr/semaspace/access/member> <"+ SEMAVocabulary.getUser(currentUser.getUuid()) + "> . " +
				  "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . " + 
				  "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) } "
				: "  ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . "  
				+ "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) ")
				+ "} } " :
					
				"CONSTRUCT { ?url a ?type . " + " ?url <" + RDFSVocabulary.label + "> ?label . "
				+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/language> ?language . "
				+ " ?url <http://sw.islab.ntua.gr/semaspace/model/scheme> ?scheme . "
				+ " ?url <http://purl.org/dc/elements/1.1/hasPart> ?part . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
				+ SEMAVocabulary.contentGraph + "> { " + type + ".  ?url a ?type ."
				+ " OPTIONAL { ?url <" + RDFSVocabulary.label + "> ?label } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.language + "> ?language } ."
				+ " OPTIONAL { ?url <" + SEMAVocabulary.scheme + "> ?scheme } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } . "
				+ " OPTIONAL { ?url <" + DCTVocabulary.hasPart + "> ?part }   . "
				+ (root.isPresent() ? "{ <" + root.get() + "> <" + DCTVocabulary.hasPart + "> ?url } UNION " + // ATTN: should have <http://purl.org/dc/terms/hasPart>* but it is not working : limit to one level !
				                      "{ BIND(<" + root.get() + "> AS ?url) }"  : "")  
				+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
				+ "  ?alurl <" + SEMAVocabulary.target + ">/<" + RDFSVocabulary.label + "> ?targetlabel  } }"
				+ " GRAPH <" + SEMAVocabulary.accessGraph + "> { "
                + " ?group <" + SACCVocabulary.dataset + "> ?url . "				
				+ (currentUser != null ? 
				  " ?group <" + SACCVocabulary.member + "> <"+ SEMAVocabulary.getUser(currentUser.getUuid()) + "> . " +
				  "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . " + 
				  "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) } "
				: "  ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . "  
				+ "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) ")
				+ "} } ";
					

//    	System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		Writer sw = new StringWriter();
		
		Model model = ModelFactory.createDefaultModel();
		
		for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
				model.add(qe.execConstruct());
			}
		}

		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
	
		return sw.toString();
	}

	@Operation(summary = "Get datasets that the editor has published to the virtuoso")
	@GetMapping(value = "/getEditorDatasets", produces = "application/json")
	public String getEditorDatasets(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("typeUri") String typeUri)
			throws Exception {
		
		String sparql = legacyUris ? 
				"CONSTRUCT { ?url a ?type . " + " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . "
				+ " ?url <http://sw.islab.ntua.gr/semaspace/model/target> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
				+ SEMAVocabulary.contentGraph + "> { " + " ?url a <" + typeUri + "> ." + " ?url a ?type ."
				+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }  . "
				+ " OPTIONAL { ?alurl <http://sw.islab.ntua.gr/semaspace/model/source> ?url . "
				+ "  ?alurl <http://sw.islab.ntua.gr/semaspace/model/target>/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } "
				+ " GRAPH <" + SEMAVocabulary.accessGraph + "> { "
				+ " ?group <http://sw.islab.ntua.gr/semaspace/access/dataset> ?url . "
				+ " FILTER (?group != <http://sw.islab.ntua.gr/semaspace/resource/group/default>) . "
				+ " ?group <http://sw.islab.ntua.gr/semaspace/access/member> <" + SEMAVocabulary.getUser(currentUser.getUuid()) + "> . " 
				+ "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . BIND(<" + SACCVocabulary.PublicGroup.toString()
				+ "> AS ?public) } } } " :
					
				"CONSTRUCT { ?url a ?type . " + " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . "
				+ " ?url <http://sw.islab.ntua.gr/semaspace/model/target> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
				+ SEMAVocabulary.contentGraph + "> { " + " ?url a <" + typeUri + "> ." + " ?url a ?type ."
				+ " OPTIONAL { ?url <" + RDFSVocabulary.label + "> ?label } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } }  . "
				+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
				+ "  ?alurl <" + SEMAVocabulary.target + ">/<" + RDFSVocabulary.label + "> ?targetlabel  } "
				+ " GRAPH <" + SEMAVocabulary.accessGraph + "> { "
				+ " ?group <" + SACCVocabulary.dataset + "> ?url . "
				+ " FILTER (?group != <" + SEMAVocabulary.getGroup("default") + ">) . "
				+ " ?group <" + SACCVocabulary.member + "> <" + SEMAVocabulary.getUser(currentUser.getUuid()) + "> . " 
				+ "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . BIND(<" + SACCVocabulary.PublicGroup.toString()
				+ "> AS ?public) } } } ";

//    	System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		Writer sw = new StringWriter();

		Model model = ModelFactory.createDefaultModel();
		for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
					QueryFactory.create(sparql, Syntax.syntaxARQ))) {
				model.add(qe.execConstruct());
			}				
		}

		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
//		System.out.println(sw);

		return sw.toString();
	}

	@Operation(summary = "Get from virtuoso collections by combination editorId - validatorId.", description = "Autodetects if token is by editor or validator. Can be called by both editor and validator")
	@GetMapping(value = "/getAccessedDatasets", produces = "application/json")
	public String getAccessedDatasets(
			@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			@Parameter String userId)
			throws Exception {

		StringBuffer urls = new StringBuffer();

		if (currentUser.getType() == UserType.EDITOR || currentUser.getType() == UserType.SUPER) {
			for (Access acc : accessRepository.findByCreatorIdAndUserIdAndAccessType(new ObjectId(currentUser.getId()), new ObjectId(userId), AccessType.VALIDATOR)) {
				urls.append("<" + SEMAVocabulary.getDataset(acc.getCollectionUuid()).toString() + "> ");
			}
		}
		else {
			for (Access acc : accessRepository.findByCreatorIdAndUserIdAndAccessType(new ObjectId(userId), new ObjectId(currentUser.getId()), AccessType.VALIDATOR)) {
				urls.append("<" + SEMAVocabulary.getDataset(acc.getCollectionUuid()).toString() + "> ");
			}
		}

		Writer sw = new StringWriter();

		if (urls.length() > 0) {

			String sparql = legacyUris ? 
					"CONSTRUCT { ?url a ?type . " + " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . "
					+ " ?url <http://sw.islab.ntua.gr/semaspace/model/target> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ SEMAVocabulary.contentGraph + "> { " + " ?url a <http://sw.islab.ntua.gr/semaspace/model/DataCollection> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }  . "
					+ " OPTIONAL { ?alurl <http://sw.islab.ntua.gr/semaspace/model/source> ?url . "
					+ "  ?alurl <http://sw.islab.ntua.gr/semaspace/model/target>/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } " :
						
					"CONSTRUCT { ?url a ?type . " + " ?url <" + RDFSVocabulary.label + "> ?label . "
					+ " ?url <http://sw.islab.ntua.gr/semaspace/model/target> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ SEMAVocabulary.contentGraph + "> { " + " ?url a <" + SEMAVocabulary.DataCollection + "> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <" + RDFSVocabulary.label + "> ?label } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<"  + RDFSVocabulary.label + "> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } ";
						

//	    	System.out.println(sparql);
//	    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

			Model model = ModelFactory.createDefaultModel();
			for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) {
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
					model.add(qe.execConstruct());
				}
			}
				
			RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);

			return sw.toString();
		}
		else {
			return "[]";
		}
	}

	@Operation(summary = "Get from virtuoso the collections that validator has been assigned to.")
	@GetMapping(value = "/getValidatorDatasets", produces = "application/json")
	public String getValidatorDatasets(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)
			throws Exception {
		
		StringBuffer urls = new StringBuffer();
		
		for (Access acc : accessRepository.findByUserIdAndAccessType(new ObjectId(currentUser.getId()), AccessType.VALIDATOR)) {
			Optional<Dataset> ds  = datasetRepository.findById(acc.getCollectionId());
			if (ds.isPresent()) {
				urls.append("<" + SEMAVocabulary.getDataset(ds.get().getUuid()).toString() + "> ");
			}
		}
		
		Writer sw = new StringWriter();
		
		if (urls.length() > 0) {
			
			String sparql = legacyUris ? 
					"CONSTRUCT { ?url a ?type . " + " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . "
					+ " ?url <http://sw.islab.ntua.gr/semaspace/model/target> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ SEMAVocabulary.contentGraph + "> { " + " ?url a <http://sw.islab.ntua.gr/semaspace/model/DataCollection> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }  . "
					+ " OPTIONAL { ?alurl <http://sw.islab.ntua.gr/semaspace/model/source> ?url . "
					+ "  ?alurl <http://sw.islab.ntua.gr/semaspace/model/target>/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } " :
						
					"CONSTRUCT { ?url a ?type . " + " ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label . "
					+ " ?url <http://sw.islab.ntua.gr/semaspace/model/target> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ SEMAVocabulary.contentGraph + "> { " + " ?url a <" + SEMAVocabulary.DataCollection + "> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <" + RDFSVocabulary.label + "> ?label } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<" + RDFSVocabulary.label + "> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } ";
						
	
//	    	System.out.println(sparql);
//	    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
	
			Model model = ModelFactory.createDefaultModel();
			for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) {
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
						QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
					model.add(qe.execConstruct());
				}
			}
			
			RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);

			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(sw.toString());
			ObjectNode nd = mapper.createObjectNode();
			if (node.isArray()) {
				ArrayNode arrayNode = (ArrayNode) node;
				for(int i = 0; i < arrayNode.size(); i++) {
					JsonNode arrayElement = arrayNode.get(i);
					String datasetUuid = arrayElement.get("@id").asText();
//					System.out.println(SEMAVocabulary.getId(datasetUuid));
					Optional<Dataset> dsOpt = datasetRepository.findByUuid(SEMAVocabulary.getId(datasetUuid));
					if (dsOpt.isPresent()) {
						Dataset ds = dsOpt.get();
						ObjectId creatorId = ds.getUserId();
						Optional<User> userOpt = userRepository.findById(creatorId.toString());
						if (userOpt.isPresent()) {
							User user = userOpt.get();
							((ObjectNode) arrayElement).put("creatorJobDescription", user.getJobDescription());
							((ObjectNode) arrayElement).put("creatorName", user.getName());
						}
					}

				}
			}
//			System.out.println(node.toString());

	
//			System.out.println(sw);
			return node.toString();
		}
		else {
			return "[]";
		}
	}

	@PostMapping(value = "/cquery", produces = "application/json")
	public String cqAnswer(@RequestBody String cq) {
		
		Model model = searcher.cqAnswer(cq);
		
		Writer sw = new StringWriter();
    	RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
    	
		return sw.toString(); 
	}

	@PostMapping(value = "/search", produces = "application/json")
	public String searchCollections(@RequestBody SearchRequest sr) {
		return searcher.searchCollections(sr.getCollections(), sr.getTime(), sr.getEndTime(), sr.getPlace(),sr.getTerms());
	}

}
