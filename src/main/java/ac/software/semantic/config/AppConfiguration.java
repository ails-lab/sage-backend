package ac.software.semantic.config;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.client.RestTemplate;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.repository.DatabaseRepository;
import ac.software.semantic.repository.ElasticConfigurationRepository;
import ac.software.semantic.repository.FileSystemConfigurationRepository;
import ac.software.semantic.repository.VirtuosoConfigurationRepository;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.semaspace.index.Indexer;
import edu.ntua.isci.ac.semaspace.query.Searcher;
import edu.ntua.isci.ac.semaspace.query.URIDescriptor;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

@Configuration
//@ComponentScan
public class AppConfiguration {

	private final static Logger logger = LoggerFactory.getLogger(AppConfiguration.class);

	@Value("${ontology.query}")
	private String ontologyFile;

	@Value("${database.name}")
	private String database;

	@Value("${database.virtuosoConfiguration.name}")
	private String virtuosoConfiguration;
	
    @Value("${virtuoso.isql.username}")
    private String isqlUsername;

    @Value("${virtuoso.isql.password}")
    private String isqlPassword;

    @Value("${virtuoso.sftp.username}")
    private String sftpUsername;

    @Value("${virtuoso.sftp.password}")
    private String sftpPassword;

	@Value("${database.elasticConfiguration.name}")
	private String elasticConfiguration;

	@Value("${database.fileSystemConfiguration.name:#{null}}")
	private String fileSystemConfiguration;

	@Value("${database.fileSystemConfiguration.folder:#{null}}")
	private String fileSystemConfigurationFolder;

	@Value("${cache.labels.size}")
	private int cacheSize;

	@Value("${cache.labels.live-time}")
	private int liveTime;
	
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

	@Autowired
	private DatabaseRepository databaseRepository;

	@Autowired
	private VirtuosoConfigurationRepository virtuosoConfigurationRepository;

	@Autowired
	private ElasticConfigurationRepository elasticConfigurationRepository;

	@Autowired
	private FileSystemConfigurationRepository fileSystemConfigurationRepository;

	@Autowired
	private Environment env;

	@Autowired
	ResourceLoader resourceLoader;

	@Bean(name = "query-ontology")
	public OWLOntology getQueryOntology() {

		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = null;

		try (InputStream input = resourceLoader.getResource("classpath:" + env.getProperty("ontology.query"))
				.getInputStream()) {

//			ontology = manager.loadOntologyFromOntologyDocument(IRI.create(ontologyFile));
			ontology = manager.loadOntologyFromOntologyDocument(input);
			logger.info("Query ontology file loaded.");

		} catch (Exception e) {
			logger.error("Could not load query ontology file.");
		}

		return ontology;
	}

	@Bean(name = "preprocess-functions")
	public Map<Resource, List<String>> getPreprocessFunctions() {
		return D2RMLOPVocabulary.functions;
	}

	@Bean(name = "database")
	public Database getDatabase() {

		Optional<Database> db = databaseRepository.findByName(database);
		if (db.isPresent()) {
			return db.get();
		} else {
			return null;
		}
	}

	@Bean(name = "virtuoso-configuration")
	public Map<String,VirtuosoConfiguration> getVirtuosoConfiguration() {
		Map<String,VirtuosoConfiguration> res = new HashMap<>();
		
		String[] vc = virtuosoConfiguration.split(",");
		String[] iusername = isqlUsername.split(",");
		String[] ipassword = isqlPassword.split(",");
		String[] fusername = sftpUsername.split(",");
		String[] fpassword = sftpPassword.split(",");
		
		for (int i = 0; i < vc.length; i++) {
			Optional<VirtuosoConfiguration> db = virtuosoConfigurationRepository.findByName(vc[i]);
			if (db.isPresent()) {
				VirtuosoConfiguration conf = db.get();
				conf.setIsqlUsername(iusername[i]);
				conf.setIsqlPassword(ipassword[i]);
				conf.setSftpUsername(fusername[i]);
				conf.setSftpPassword(fpassword[i]);
				
				res.put(conf.getName(), conf);
				
//				System.out.println(conf.getName() + "*" + conf.getSftpUsername() + "*" + conf.getSftpPassword() + "*" + conf.getFileServer() + "*" + conf.getUploadFolder());
			}
			
		}
		
		return res;
	}

	@Bean(name = "elastic-configuration")
	public ElasticConfiguration getElasticConfiguration() {

		Optional<ElasticConfiguration> db = elasticConfigurationRepository.findByName(elasticConfiguration);
		if (db.isPresent()) {
			return db.get();
		} else {
			return null;
		}
	}

	@Bean(name = "filesystem-configuration")
	@DependsOn({ "database" })
	public FileSystemConfiguration getFileSystemConfiguration(Database db) throws Exception {

		if (fileSystemConfiguration != null) {
			Optional<FileSystemConfiguration> fs = fileSystemConfigurationRepository
					.findByName(fileSystemConfiguration);
			if (fs.isPresent()) {
				return fs.get();
			} else {
				return null;
			}
		} else if (fileSystemConfigurationFolder != null) {
			if (!fileSystemConfigurationFolder.endsWith("/") && !fileSystemConfigurationFolder.endsWith("\\")) {
				fileSystemConfigurationFolder = fileSystemConfigurationFolder + "/";
			}

			String macAddress = "MAC:" + getSystemMac();

			Optional<FileSystemConfiguration> fs = fileSystemConfigurationRepository.findByName(macAddress);

			if (fs.isPresent()) {
				FileSystemConfiguration fsd = fs.get();
				if (!fsd.getDataFolder().equals(fileSystemConfigurationFolder)) {
					fsd.setDataFolder(fileSystemConfigurationFolder);
					fileSystemConfigurationRepository.save(fsd);
				}
				return fsd;
			} else {
				logger.info(
						"Creating new filesystem configuration: " + macAddress + " @ " + fileSystemConfigurationFolder);

				FileSystemConfiguration fsd = new FileSystemConfiguration();
				fsd.setDatabaseId(db.getId());
				fsd.setName(macAddress);
				fsd.setDataFolder(fileSystemConfigurationFolder);

				return fileSystemConfigurationRepository.save(fsd);
			}
		}

		return null;
	}
	
	@Bean(name = "labels-cache")
	public Cache getLabelsCache() {
	    CacheManager singletonManager = CacheManager.create();
	    if (!singletonManager.cacheExists("labels")) {
		    singletonManager.addCache(new Cache("labels", cacheSize, false, false, liveTime, liveTime));
		    
			logger.info("Created labels cache.");
	    }
	    
	    return singletonManager.getCache("labels");
	}
	
	@Bean(name = "endpoints-cache")
	public Cache getEndpointsCache() {
	    CacheManager singletonManager = CacheManager.create();
	    if (!singletonManager.cacheExists("endpoints")) {
		    singletonManager.addCache(new Cache("endpoints", cacheSize, false, false, liveTime, liveTime));
		    
			logger.info("Created endpoints cache.");
	    }
	    
	    return singletonManager.getCache("endpoints");
	    
	}

	public static String getSystemMac() throws Exception {
		String OSName = System.getProperty("os.name");
		if (OSName.contains("Windows")) {
			return (getMAC4Windows());
		} else {
			String mac = getMAC4Linux("eth0");
			if (mac == null) {
				mac = getMAC4Linux("eth1");
				if (mac == null) {
					mac = getMAC4Linux("eth2");
					if (mac == null) {
						mac = getMAC4Linux("usb0");
					}
				}
			}
			return mac;
		}
	}

	/**
	 * Method for get MAc of Linux Machine
	 * 
	 * @param name
	 * @return
	 * @throws SocketException 
	 */
	private static String getMAC4Linux(String name) {
		try {
			NetworkInterface network = NetworkInterface.getByName(name);
			byte[] mac = network.getHardwareAddress();
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < mac.length; i++) {
				sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
			}
			return (sb.toString());
		} catch (Exception ex) {
			return null;
		}
	}

	/**
	 * Method for get Mac Address of Windows Machine
	 * 
	 * @return
	 * @throws Exception 
	 */
	private static String getMAC4Windows() {
		try {
			InetAddress addr = InetAddress.getLocalHost();
			NetworkInterface network = NetworkInterface.getByInetAddress(addr);
	
			byte[] mac = network.getHardwareAddress();
	
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < mac.length; i++) {
				sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
			}
	
			return sb.toString();
		} catch (Exception ex) {
			return null;
		} 
	}

//	@Bean(name = "indexer")
//	@DependsOn({ "virtuoso-configuration" })
//	public Indexer getIndexer(@Qualifier("virtuoso-configuration") Map<String,VirtuosoConfiguration> vc) {
//		return new Indexer(vc.values().iterator().next().getSparqlEndpoint());
//	}

	// NOT CORRECT FOR MULTIPLE VIRTUOSOS! SEARCHES ONLY THE FIRST VIRTUOSO
	@Bean(name = "searcher")
	@DependsOn({ "virtuoso-configuration" })
	public Searcher getSearcher(@Qualifier("virtuoso-configuration") Map<String,VirtuosoConfiguration> vc) {
		return new Searcher(vc.values().iterator().next().getSparqlEndpoint());
	}

	@Bean(name = "vocabularies")
	@DependsOn({ "virtuoso-configuration" })
	public VocabulariesBean vocs(@Qualifier("virtuoso-configuration") Map<String,VirtuosoConfiguration> vcs) {
		VocabulariesBean vb = new VocabulariesBean();
		for (VirtuosoConfiguration vc : vcs.values()) {
			vb.setMap(createVocabulariesMap(vc, legacyUris));
		}

		return vb;
	}
	
	@Bean(name = "all-datasets")
	@DependsOn({ "virtuoso-configuration" })
	public VocabulariesBean dataset(@Qualifier("virtuoso-configuration") Map<String,VirtuosoConfiguration> vcs) {
		VocabulariesBean vb = new VocabulariesBean();
		for (VirtuosoConfiguration vc : vcs.values()) {
			vb.setMap(createDatasetsMap(vc, legacyUris));
		}

		return vb;
	}	
	
	public static Map<String, VocabularyInfo> createDatasetsMap(VirtuosoConfiguration vc, boolean legacyUris) {
		String sparql = legacyUris ? 
				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { "  
				+ "   ?d a ?tt . VALUES ?t { <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . "
				+ "   ?d <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/endpoint> ?endpoint . } "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/class> ?cp . "
				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
				+ "              ?cp <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix } ."
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?dp . "
				+ "              ?dp <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . "
				+ "              ?dp <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProp . } } " :
					
				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp FROM <" + SEMAVocabulary.contentGraph + "> WHERE { " 
				+ "   ?d a ?tt . VALUES ?t { <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . "
				+ "   ?d <" + DCTVocabulary.identifier + "> ?identifier . "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.endpoint + "> ?endpoint . } "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.clazz + "> ?cp . "
				+ "              ?cp a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } . "  
				+ "              ?cp <" + SEMAVocabulary.prefix + "> ?prefix } ."
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.dataProperty + "> ?dp . "
				+ "              ?dp <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . "
				+ "              ?dp <" + SEMAVocabulary.uri + "> ?labelProp . } } ";
					

		Map<String, VocabularyInfo> map = new HashMap<>();

//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
			ResultSet rs = qe.execSelect();
//
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
//				System.out.println(qs);
//
				String graph = qs.get("d").toString();
				RDFNode prefix = qs.get("prefix");

				if (prefix != null) { // SHOULD NOT BE NULL!!
//					String identifier = qs.get("identifier").toString();

					RDFNode endpoint = qs.get("endpoint");

					VocabularyInfo vi = map.get(prefix.toString());
					if (vi == null) {
						if (endpoint == null) {
							vi = new VocabularyInfo(graph);
						} else {
							vi = new VocabularyInfo(graph, endpoint.toString());
						}
						vi.setVirtuoso(vc);
						
						map.put(prefix.toString(), vi);
					}
				}
			}
		}

		return map;
	}

//	@Bean(name = "all-prefixes")
//	@DependsOn({ "virtuoso-configuration" })
//    private static Set<URIDescriptor> allPrefixes(@Qualifier("virtuoso-configuration") VirtuosoConfiguration vc) {
//    	Set<URIDescriptor> res = new HashSet<>();
//    
//		String sparql = "SELECT ?prefix ?type FROM <" + SEMAVocabulary.contentGraph + "> WHERE { " +
//				   "?url  a ?type . VALUES ?type { <" + SEMAVocabulary.AssertionCollection + "> <" + SEMAVocabulary.VocabularyCollection + "> <" + SEMAVocabulary.DataCollection + "> } . " +
//		           "?url <" + SEMAVocabulary.clazz + "> ?c . " + 
//		           "?c a ?ctype . VALUES ?ctype { <" + SEMAVocabulary.VocabularyTerm + "> <" + SEMAVocabulary.CollectionResource + "> } ." + 
//		           "?c <" + SEMAVocabulary.prefix + "> ?prefix  }";
//
//		System.out.println(QueryFactory.create(sparql.toString(), Syntax.syntaxARQ));
//		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql.toString(), Syntax.syntaxARQ))) {
//			ResultSet rs = qe.execSelect();
//			while (rs.hasNext()) {
//				QuerySolution sol = rs.next();
//				String prefix = sol.get("prefix").toString();
//				String type = sol.get("type").toString();
//				res.add(new URIDescriptor(prefix, type));
//			}
//		}
//		
//		return res;
//
//    }
	
	public static Map<String, VocabularyInfo> createVocabulariesMap(VirtuosoConfiguration vc, boolean legacyUris) {
		String sparql = legacyUris ? 
				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp " + "WHERE { " + "GRAPH <"
				+ SEMAVocabulary.contentGraph + "> { " + "   ?d a <" + SEMAVocabulary.VocabularyCollection + "> . "
				+ "   ?d <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/endpoint> ?endpoint . } "
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/class> ?cp . "
				+ "              ?cp <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix } ."
				+ "   OPTIONAL { ?d <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?dp . "
				+ "              ?dp <http://purl.org/dc/elements/1.1/type> <http://www.w3.org/2000/01/rdf-schema#label> . "
				+ "              ?dp <http://sw.islab.ntua.gr/apollonis/ms/uri> ?labelProp . } } }" :
					
				"SELECT ?d ?endpoint ?identifier ?prefix ?labelProp " + "WHERE { " + "GRAPH <"
				+ SEMAVocabulary.contentGraph + "> { " + "   ?d a <" + SEMAVocabulary.VocabularyCollection + "> . "
				+ "   ?d <http://purl.org/dc/elements/1.1/identifier> ?identifier . "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.endpoint + "> ?endpoint . } "
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.clazz + "> ?cp . "
				+ "              ?cp <" + SEMAVocabulary.prefix + "> ?prefix } ."
				+ "   OPTIONAL { ?d <" + SEMAVocabulary.dataProperty + "> ?dp . "
				+ "              ?dp <" + DCTVocabulary.type + "> <" + RDFSVocabulary.label + "> . "
				+ "              ?dp <" + SEMAVocabulary.uri + "> ?labelProp . } } }";
					

		Map<String, VocabularyInfo> map = new HashMap<>();

//		System.out.println(QueryFactory.create(sparql, Syntax.syntaxARQ));

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
			ResultSet rs = qe.execSelect();
//
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
//				System.out.println(qs);
//
				String graph = qs.get("d").toString();
				RDFNode prefix = qs.get("prefix");

				if (prefix != null) { // SHOULD NOT BE NULL!!
//					String identifier = qs.get("identifier").toString();

					RDFNode endpoint = qs.get("endpoint");

					VocabularyInfo vi = map.get(prefix.toString());
					if (vi == null) {
						if (endpoint == null) {
							vi = new VocabularyInfo(graph);
						} else {
							vi = new VocabularyInfo(graph, endpoint.toString());
						}
						vi.setVirtuoso(vc);
						
						map.put(prefix.toString(), vi);
					}
				}
			}
		}

//		System.out.println("END");

		return map;
	}

	@Bean(name = "prefixes")
	@DependsOn({ "virtuoso-configuration" })
    private static Set<URIDescriptor> vocabularyPrefixes(@Qualifier("virtuoso-configuration") Map<String,VirtuosoConfiguration> vcs) {
    	Set<URIDescriptor> res = new HashSet<>();
    
		String sparql = "SELECT ?prefix ?type FROM <" + SEMAVocabulary.contentGraph + "> WHERE { " +
				   "?url  a ?type . VALUES ?type { <http://sw.islab.ntua.gr/semaspace/model/AssertionCollection> <http://sw.islab.ntua.gr/semaspace/model/VocabularyCollection> } . " +
		           "?url <http://sw.islab.ntua.gr/apollonis/ms/class> " + 
		           "     [ a       <http://sw.islab.ntua.gr/apollonis/ms/VocabularyTerm> ;" + 
		           "       <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix ]  }";

//		System.out.println(QueryFactory.create(sparql.toString(), Syntax.syntaxARQ));
		for (VirtuosoConfiguration vc : vcs.values()) { 
	
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql.toString(), Syntax.syntaxARQ))) {
				ResultSet rs = qe.execSelect();
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					String prefix = sol.get("prefix").toString();
					String type = sol.get("type").toString();
					res.add(new URIDescriptor(prefix, type));
				}
			}
		}
		
		return res;

    }
	
	@Bean(name = "date-format")
	public SimpleDateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	}
    
//    @Bean(name="base-time-graph")
//    public GraphDescriptor getBaseTimeGraph() {	
//    	return collectionsService.getCollectionGraphUrlByIdentifier("http://sw.islab.ntua.gr/kb/timeline");
//    }
//    
//    @Bean(name="base-space-graph")
//    public GraphDescriptor getBaseSpaceGraph() {	
//    	return collectionsService.getCollectionGraphUrlByIdentifier("http://sws.geonames.org/");
//    }
//    
//    @Bean(name="aligned-time-graphs")
//    public List<AlignmentDescriptor> getAlignedTimeGraphs() {
//		return collectionsService.getAlignmentGraphUrlsByType(SEMAVocabulary.TemporalCollection.toString());
//    }
//    
////    @Bean(name="aligned-vocabulary-graphs")
////    public List<GraphDescriptor> getAlignedVocabularyGraphs() {
////		return collectionsService.getAlignmentGraphUrlsByType(SEMAVocabulary.VocabularyCollection.toString());
////    }
//	
//    @Bean(name="query-parameters")
//    public Map<String[], List<?>> getQueryParameters() {
//    	List<GraphDescriptor> baseTimeGraphs = new ArrayList<>();
//    	baseTimeGraphs.add(getBaseTimeGraph());
//
//    	List<GraphDescriptor> baseSpaceGraphs = new ArrayList<>();
//    	baseSpaceGraphs.add(getBaseSpaceGraph());
//
//    	Map<String[], List<?>> values = new HashMap<>();
//    	values.put(new String[] {"@@BASE_TIME_GRAPH@@"}, baseTimeGraphs);
//    	values.put(new String[] {"@@BASE_SPACE_GRAPH@@"}, baseSpaceGraphs);
//    	values.put(new String[] {"@@ALIGNMENT_TIME_GRAPH@@", "@@SOURCE_TIME_GRAPH@@", "@@BASE_TIME_GRAPH@@"}, getAlignedTimeGraphs());
//    	
//    	return values;
//    }

	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder builder) {
		// Do any additional configuration here
		return builder.build();
	}

//    @Autowired
//    private ApplicationContext applicationContext;
//    
//    @Autowired
//    private SpringExtension springExtension;
// 
//    @Bean
//    public ActorSystem actorSystem() {
//        ActorSystem system = ActorSystem.create("akka-spring-demo", ConfigFactory.load());
//        
////        SpringExtension.SPRING_EXTENSION_PROVIDER.get(system).initialize(applicationContext);
//        springExtension.initialize(applicationContext);
//        
//        return system;
//    }

//    @Bean
//    public MultipartResolver multipartResolver() {
//       return new CommonsMultipartResolver();
//    }
//    
//    @Bean
//    @Order(0)
//    public MultipartFilter multipartFilter() {
//        MultipartFilter multipartFilter = new MultipartFilter();
//        multipartFilter.setMultipartResolverBeanName("multipartResolver");
//        return multipartFilter;
//    }
	


}
