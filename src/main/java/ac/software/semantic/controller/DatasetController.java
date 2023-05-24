package ac.software.semantic.controller;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Access;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.AccessType;
import ac.software.semantic.model.constants.UserRoleType;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.ClassStructureResponse;
import ac.software.semantic.payload.SearchRequest;
import ac.software.semantic.payload.stats.AnnotationsStatistics;
import ac.software.semantic.repository.AccessRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.CampaignRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService;
import ac.software.semantic.service.AnnotationsStatisticsService;
import ac.software.semantic.service.SchemaService;
import ac.software.semantic.service.SchemaService.ClassStructure;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import ac.software.semantic.vocs.SACCVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.semaspace.query.Searcher;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Dataset Functions")
@RestController
@RequestMapping("/api/f/datasets")
public class DatasetController {

	@Autowired
	@Qualifier("triplestore-configurations")
	private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    private AccessRepository accessRepository;

    @Autowired
    private CampaignRepository campaignRepository;
    
    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
 	private AnnotationEditGroupService aegService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
    @Autowired
 	private AnnotatorDocumentRepository annotatorRepository;

    @Autowired
 	private SchemaService schemaService;
    
    @Autowired
    private AnnotationsStatisticsService anStatService;

	
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
				"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
				+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/language> ?language . "
				+ " ?url <" + SEMAVocabulary.scheme + "> ?scheme . "
				+ " ?url <http://purl.org/dc/elements/1.1/hasPart> ?part . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
				+ resourceVocabulary.getContentGraphResource() + "> { " + type + ".  ?url a ?type ."
				+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/language> ?language } ."
				+ " OPTIONAL { ?url <" + SEMAVocabulary.scheme + "> ?scheme } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } . "
				+ " OPTIONAL { ?url <http://purl.org/dc/terms/hasPart> ?part }   . "
				+ (root.isPresent() ? "{ <" + root.get() + "> <http://purl.org/dc/terms/hasPart> ?url } UNION " + // ATTN: should have <http://purl.org/dc/terms/hasPart>* but it is not working : limit to one level !
				                      "{ BIND(<" + root.get() + "> AS ?url) }"  : "")  
				+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
				+ "  ?alurl <" + SEMAVocabulary.target + ">/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } }"
				+ " GRAPH <" + resourceVocabulary.getAccessGraphResource() + "> { "
                + " ?group <" + SACCVocabulary.dataset + "> ?url . "				
				+ (currentUser != null ? 
				  " ?group <" + SACCVocabulary.member + "> <"+ resourceVocabulary.getUserAsResource(currentUser.getUuid()) + "> . " +
				  "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . " + 
				  "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) } "
				: "  ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . "  
				+ "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) ")
				+ "} } " :
					
				"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
				+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/language> ?language . "
				+ " ?url <" + SEMAVocabulary.scheme + "> ?scheme . "
				+ " ?url <http://purl.org/dc/elements/1.1/hasPart> ?part . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
				+ resourceVocabulary.getContentGraphResource() + "> { " + type + ".  ?url a ?type ."
				+ " OPTIONAL { ?url <" + RDFSVocabulary.label + ">|<" + DCTVocabulary.title + "> ?label } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.language + "> ?language } ."
				+ " OPTIONAL { ?url <" + SEMAVocabulary.scheme + "> ?scheme } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } . "
				+ " OPTIONAL { ?url <" + DCTVocabulary.hasPart + "> ?part }   . "
				+ (root.isPresent() ? "{ <" + root.get() + "> <" + DCTVocabulary.hasPart + "> ?url } UNION " + // ATTN: should have <http://purl.org/dc/terms/hasPart>* but it is not working : limit to one level !
				                      "{ BIND(<" + root.get() + "> AS ?url) }"  : "")  
				+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
				+ "  ?alurl <" + SEMAVocabulary.target + ">/<" + RDFSVocabulary.label + "> ?targetlabel  } }"
				+ " GRAPH <" + resourceVocabulary.getAccessGraphResource() + "> { "
                + " ?group <" + SACCVocabulary.dataset + "> ?url . "				
				+ (currentUser != null ? 
				  " ?group <" + SACCVocabulary.member + "> <"+ resourceVocabulary.getUserAsResource(currentUser.getUuid()) + "> . " +
				  "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . " + 
				  "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) } "
				: "  ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . "  
				+ "  BIND(<" + SACCVocabulary.PublicGroup.toString() + "> AS ?public) ")
				+ "} } ";
					

//    	System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		Writer sw = new StringWriter();
		
		Model model = ModelFactory.createDefaultModel();
		
		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
				model.add(qe.execConstruct());
			}
		}

		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);
	
		return sw.toString();
	}

	@Operation(summary = "Get datasets that the editor has published to the virtuoso")
	@GetMapping(value = "/getEditorDatasets", produces = "application/json")
	public String getEditorDatasets(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("typeUri") List<String> typeUri)
			throws Exception {
		
		String types = "";
		for (String s : typeUri) {
			types += "<" + s + "> ";
		}
		
		String sparql = legacyUris ? 
				"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
				+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
//				+ resourceVocabulary.getContentGraphResource() + "> { ?url a <" + typeUri + "> ." + " ?url a ?type ."
                + resourceVocabulary.getContentGraphResource() + "> { ?url a ?type . VALUES ?type { " + types + " } . "
				+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
				+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }  . "
				+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
				+ "  ?alurl <" + SEMAVocabulary.target + ">/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } "
				+ " GRAPH <" + resourceVocabulary.getAccessGraphResource() + "> { "
				+ " ?group <" + SACCVocabulary.dataset + "> ?url . "
				+ " FILTER (?group != <" + resourceVocabulary.getDefaultGroupResource() + ">) . "
				+ " ?group <" + SACCVocabulary.member + "> <" + resourceVocabulary.getUserAsResource(currentUser.getUuid()) + "> . " 
				+ "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . BIND(<" + SACCVocabulary.PublicGroup.toString()
				+ "> AS ?public) } } } " 
				
				:
				
				"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
				+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
				+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
				+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ " ?url <http://purl.org/dc/elements/1.1/isPartOf> ?public . } " + "WHERE { " + " GRAPH <"
//				+ resourceVocabulary.getContentGraphResource() + "> { " + " ?url a <" + typeUri + "> ." + " ?url a ?type ."
                + resourceVocabulary.getContentGraphResource() + "> { ?url a ?type . VALUES ?type { " + types + " } . "
				+ " OPTIONAL { ?url <" + RDFSVocabulary.label + ">|<" + DCTVocabulary.title + "> ?label } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
				+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } }  . "
				+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
				+ "  ?alurl <" + SEMAVocabulary.target + ">/<" + RDFSVocabulary.label + "> ?targetlabel  } "
				+ " GRAPH <" + resourceVocabulary.getAccessGraphResource() + "> { "
				+ " ?group <" + SACCVocabulary.dataset + "> ?url . "
				+ " FILTER (?group != <" + resourceVocabulary.getGroupAsResource("default") + ">) . "
				+ " ?group <" + SACCVocabulary.member + "> <" + resourceVocabulary.getUserAsResource(currentUser.getUuid()) + "> . " 
				+ "  OPTIONAL { ?group a <" + SACCVocabulary.PublicGroup.toString() + "> . BIND(<" + SACCVocabulary.PublicGroup.toString()
				+ "> AS ?public) } } } ";

//    	System.out.println(sparql);
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		Writer sw = new StringWriter();

		Model model = ModelFactory.createDefaultModel();
		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
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

//		if (currentUser.getType() == UserType.EDITOR || currentUser.getType() == UserType.SUPER) {
		if (currentUser.getType() == UserRoleType.EDITOR) {
			for (Access acc : accessRepository.findByCreatorIdAndUserIdAndAccessType(new ObjectId(currentUser.getId()), new ObjectId(userId), AccessType.VALIDATOR)) {
				urls.append("<" + resourceVocabulary.getDatasetAsResource(acc.getCollectionUuid()).toString() + "> ");
			}
		}
		else {
			for (Access acc : accessRepository.findByCreatorIdAndUserIdAndAccessType(new ObjectId(userId), new ObjectId(currentUser.getId()), AccessType.VALIDATOR)) {
				urls.append("<" + resourceVocabulary.getDatasetAsResource(acc.getCollectionUuid()).toString() + "> ");
			}
		}

		Writer sw = new StringWriter();

		if (urls.length() > 0) {

			String sparql = legacyUris ? 
					"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
					+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ resourceVocabulary.getContentGraphResource() + "> { " + " ?url a <" + SEMAVocabulary.DataCollection + "> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } " :
						
					"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
					+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ resourceVocabulary.getContentGraphResource() + "> { " + " ?url a <" + SEMAVocabulary.DataCollection + "> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <" + RDFSVocabulary.label + ">|<" + DCTVocabulary.title + "> ?label } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<"  + RDFSVocabulary.label + "> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } ";
						

//	    	System.out.println(sparql);
//	    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

			Model model = ModelFactory.createDefaultModel();
			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
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
	
	@Operation(summary = "Get from virtuoso collections by combination editorId - validatorId.", description = "Autodetects if token is by editor or validator. Can be called by both editor and validator")
	@GetMapping(value = "/get-assigned-datasets", produces = "application/json")
	public String getFAccessedDatasets(
			@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			@Parameter String campaignId,
	        @Parameter(required = false) String userId)
			throws Exception {

		StringBuffer urls = new StringBuffer();
		
//		if (currentUser.getType() == UserType.EDITOR || currentUser.getType() == UserType.SUPER) {
		if (currentUser.getType() == UserRoleType.EDITOR) {
			if (userId != null) {
				for (Access acc : accessRepository.findByCampaignIdAndUserIdAndAccessType(new ObjectId(campaignId), new ObjectId(userId), AccessType.VALIDATOR)) {
					urls.append("<" + resourceVocabulary.getDatasetAsResource(acc.getCollectionUuid()).toString() + "> ");
				}
			} else {
				for (Access acc : accessRepository.findByCampaignIdAndAccessType(new ObjectId(campaignId), AccessType.VALIDATOR)) {
					urls.append("<" + resourceVocabulary.getDatasetAsResource(acc.getCollectionUuid()).toString() + "> ");
				}
				
			}
		}
		else {
			for (Access acc : accessRepository.findByCampaignIdAndUserIdAndAccessType(new ObjectId(campaignId), new ObjectId(currentUser.getId()), AccessType.VALIDATOR)) {
				urls.append("<" + resourceVocabulary.getDatasetAsResource(acc.getCollectionUuid()).toString() + "> ");
			}
		}

		Writer sw = new StringWriter();

		if (urls.length() > 0) {

			String sparql = legacyUris ? 
					"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
					+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ resourceVocabulary.getContentGraphResource() + "> { ?url a ?type . VALUES ?type { <" + SEMAVocabulary.DataCollection + "> <" + SEMAVocabulary.DataCatalog + "> } "
					+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } " :
						
					"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
					+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ resourceVocabulary.getContentGraphResource() + "> { ?url a ?type . VALUES ?type { <" + SEMAVocabulary.DataCollection + "> <" + SEMAVocabulary.DataCatalog + "> } "
					+ " OPTIONAL { ?url <" + RDFSVocabulary.label + ">|<" + DCTVocabulary.title + "> ?label } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<"  + RDFSVocabulary.label + "> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } ";
						

//	    	System.out.println(sparql);
//	    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

			Model model = ModelFactory.createDefaultModel();
			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
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
		
		Map<String, List<String>> datasetCampaignMap = new HashMap<>();
		Map<String, String> campaignNameMap = new HashMap<>();
		
		for (Access acc : accessRepository.findByUserIdAndAccessType(new ObjectId(currentUser.getId()), AccessType.VALIDATOR)) {
			Optional<Dataset> ds  = datasetRepository.findById(acc.getCollectionId());
			if (ds.isPresent()) {
				String datasetUri = resourceVocabulary.getDatasetAsResource(ds.get().getUuid()).toString();
				
				urls.append("<" + datasetUri + "> ");
				
				String campaignName = campaignNameMap.get(acc.getCampaignId().toString());
				if (campaignName == null) {
					Optional<Campaign> campOpt = campaignRepository.findById(acc.getCampaignId());
					if (campOpt.isPresent()) {
						campaignName = campOpt.get().getName();
					}
				}
				
				if (campaignName != null) {
					List<String> campaigns = datasetCampaignMap.get(datasetUri);
					if (campaigns == null) {
						campaigns = new ArrayList<>();
						datasetCampaignMap.put(datasetUri, campaigns);
					}
					
					campaigns.add(campaignName);
				}
			}
		}
		
		Writer sw = new StringWriter();
		
		if (urls.length() > 0) {
			
			String sparql = legacyUris ? 
					"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
					+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ resourceVocabulary.getContentGraphResource() + "> { " + " ?url a <" + SEMAVocabulary.DataCollection + "> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <http://www.w3.org/2000/01/rdf-schema#label> ?label } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } ."
					+ " OPTIONAL { ?url <http://purl.org/dc/elements/1.1/creator> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<http://www.w3.org/2000/01/rdf-schema#label> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } " :
						
					"CONSTRUCT { ?url a ?type . " + " ?url <" + DCTVocabulary.title + "> ?label . "
					+ " ?url <" + SEMAVocabulary.target + "> ?targetlabel ."
					+ " ?url <http://purl.org/dc/elements/1.1/creator> ?creator . "
					+ " ?url <http://purl.org/dc/elements/1.1/identifier> ?identifier } " + "WHERE { " + " GRAPH <"
					+ resourceVocabulary.getContentGraphResource() + "> { " + " ?url a <" + SEMAVocabulary.DataCollection + "> ." + " ?url a ?type ."
					+ " OPTIONAL { ?url <" + RDFSVocabulary.label + ">|<" + DCTVocabulary.title + "> ?label } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.identifier + "> ?identifier } ."
					+ " OPTIONAL { ?url <" + DCTVocabulary.creator + "> ?creator } }  . "
					+ " OPTIONAL { ?alurl <" + SEMAVocabulary.source + "> ?url . "
					+ "  ?alurl <" + SEMAVocabulary.target + ">/<" + RDFSVocabulary.label + "> ?targetlabel  } "
					+ " VALUES ?url { " + urls + " }"
					+ " } ";
						
	
//	    	System.out.println(sparql);
//	    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));
	
			Model model = ModelFactory.createDefaultModel();
			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
						QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
					model.add(qe.execConstruct());
				}
			}
			
			RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY);

			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(sw.toString());

			if (node.isArray()) {
				ArrayNode arrayNode = (ArrayNode) node;

				for(int i = 0; i < arrayNode.size(); i++) {
					JsonNode arrayElement = arrayNode.get(i);
					String datasetUuid = arrayElement.get("@id").asText();
//					System.out.println(SEMAVocabulary.getId(datasetUuid));
//					Optional<Dataset> dsOpt = datasetRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(datasetUuid));
//					
//					if (dsOpt.isPresent()) {
//						Dataset ds = dsOpt.get();
//						ObjectId creatorId = ds.getUserId();
//						Optional<User> userOpt = userRepository.findById(creatorId.toString());
//						if (userOpt.isPresent()) {
//							User user = userOpt.get();
//							((ObjectNode) arrayElement).put("creatorJobDescription", user.getJobDescription());
//							((ObjectNode) arrayElement).put("creatorName", user.getName());
//						}
//					}
					
					ArrayNode array = mapper.createArrayNode();
					
					List<String> str = datasetCampaignMap.get(datasetUuid);
					if (str != null) {
						for (String s : str) {
							array.add(s);
						}
						
						((ObjectNode) arrayElement).put("campaigns", array);
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

   public class AnnotatorStatisticsCube {
    	public Dataset dataset;
    	public List<AnnotatorDocument> annotators;
    	public DatasetCatalog dcg;
    	public ProcessStateContainer psv;
    	public String fromClause; 
    	public Map<String, List<AnnotatorDocument>> perFieldAnnotators;
    	
    	public String allAnnotatorsFilter; 
    	public Map<String, String> perFieldAnnotatorsFilter;
    	
    	public AnnotatorStatisticsCube(Dataset dataset) {
    		this.dataset = dataset;
    		
    		annotators = annotatorRepository.findByDatasetUuid(dataset.getUuid());
    		
    		psv = dataset.getCurrentPublishState(virtuosoConfigurations.values());

    		dcg = schemaService.asCatalog(dataset.getUuid());
    		fromClause = schemaService.buildFromClause(dcg);

			perFieldAnnotators = new HashMap<>();
	
			List<String> allAnnotatorUuids = new ArrayList<>();
	
			for (AnnotatorDocument adoc : annotators) {
				allAnnotatorUuids.add(adoc.getUuid());
				
				String path = PathElement.onPathStringListAsSPARQLString(adoc.getOnProperty());
				
				List<AnnotatorDocument> list = perFieldAnnotators.get(path);
				if (list == null) {
					list = new ArrayList<>();
					perFieldAnnotators.put(path, list);
				}
				list.add(adoc);
			}
			
			allAnnotatorsFilter = aegService.annotatorFilter("v", allAnnotatorUuids);
			
			perFieldAnnotatorsFilter = new HashMap<>();
			
			for (Map.Entry<String, List<AnnotatorDocument>> entry : perFieldAnnotators.entrySet()) {
				
				List<String> list = new ArrayList<>();
				
				for (AnnotatorDocument adoc : entry.getValue()) {
					list.add(adoc.getUuid());
				}
				
				perFieldAnnotatorsFilter.put(entry.getKey(), aegService.annotatorFilter("v", list));
			}
			
    	}

    }
   
   
    @GetMapping(value = "/annotations-statistics",
            produces = "application/json")
	public ResponseEntity<?> annotationStatistics(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @RequestParam String datasetUri)  {
		
    	Optional<Dataset> datasetOpt = datasetRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(datasetUri));
    	
		if (!datasetOpt.isPresent()) {
			return new ResponseEntity<>(APIResponse.FailureResponse(), HttpStatus.NOT_FOUND);
		}

		Dataset dataset = datasetOpt.get();
		
    	AnnotatorStatisticsCube asc = new AnnotatorStatisticsCube(dataset);

    	AnnotationsStatistics res = new AnnotationsStatistics(resourceVocabulary.getDatasetAsResource(asc.dataset.getUuid()).toString());
    	res = anStatService.annotatedItems(asc, res, Arrays.asList(new String[] { "total" , "fresh" }));
    	res = anStatService.annotations(asc, res, Arrays.asList(new String[] { "total" , "fresh", "accepted", "rejected" }));
    	res = anStatService.mostFrequentAnnotations(10, asc, res, Arrays.asList(new String[] { "total" , "fresh", "accepted", "rejected" }));
    	res = anStatService.computeValidationDistribution(10, asc, res, Arrays.asList(new String[] { "total" , "fresh", "accepted", "rejected" }));
//    	res = anStatService.mostFrequentFreshUrisStatistics(10, asc, res);

		return ResponseEntity.ok(res);
	}
    
    @GetMapping(value = "/schema-classes")
    public ResponseEntity<?> schemaClasses(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam String datasetUri,  @RequestParam(required = false) List<String> classUris)  {

    	try {
    		Optional<Dataset> datasetOpt = datasetRepository.findByUuid(resourceVocabulary.getUuidFromResourceUri(datasetUri));
			if (!datasetOpt.isPresent()) {
				return new ResponseEntity<>(APIResponse.FailureResponse("Dataset not found."), HttpStatus.NOT_FOUND);
			}
			
			Dataset dataset = datasetOpt.get();
			
			List<ClassStructureResponse> cs = new ArrayList<>();
			
			if (classUris == null) {
				List<ClassStructure> tcs = schemaService.readTopClasses(datasetOpt.get());
				for (ClassStructure css : tcs) {
					cs.add(ClassStructureResponse.createFrom(css));
				}
			} else {
				for (String classUri : classUris) {
					cs.add(schemaService.readTopClass(dataset, classUri));
				}
			}
			
			return ResponseEntity.ok(cs);
		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(APIResponse.FailureResponse(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
		}
    }
}
