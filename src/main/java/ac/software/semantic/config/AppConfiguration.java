package ac.software.semantic.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.JsonLDWriteContext;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.out.NodeToLabel;
import org.apache.jena.riot.system.SyntaxLabels;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.bson.types.ObjectId;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdApi;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;

import ac.software.semantic.model.BlazegraphConfiguration;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.GraphDBConfiguration;
import ac.software.semantic.model.LodViewConfiguration;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.UserTaskDocument;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.payload.response.UserTaskResponse;
import ac.software.semantic.repository.core.BlazegraphConfigurationRepository;
import ac.software.semantic.repository.core.ElasticConfigurationRepository;
import ac.software.semantic.repository.core.FileSystemConfigurationRepository;
import ac.software.semantic.repository.core.GraphDBConfigurationRepository;
import ac.software.semantic.repository.core.LodViewConfigurationRepository;
import ac.software.semantic.repository.core.VirtuosoConfigurationRepository;
import ac.software.semantic.repository.root.DataServiceRepository;
import ac.software.semantic.repository.root.DatabaseRepository;
import ac.software.semantic.repository.root.VocabularyRepository;
import ac.software.semantic.service.UserTaskService.ScheduledUserTask;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.exception.ScheduleException;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.FunctionImplementation;
import edu.ntua.isci.ac.d2rml.vocabulary.FunctionProcessor;
import edu.ntua.isci.ac.semaspace.query.Searcher;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

@Configuration
//@EnableCaching
//@ComponentScan
public class AppConfiguration {

	private final static Logger logger = LoggerFactory.getLogger(AppConfiguration.class);

	@Value("${ontology.query}")
	private String ontologyFile;

	@Value("${database.name}")
	private String database;

	@Value("${database.virtuosoConfiguration.name}")
	private List<String> virtuosoConfiguration;

	@Value("${database.virtuosoConfiguration.localImport:}")
	private List<Boolean> virtuosoLocalImport;

	@Value("${database.lodviewConfiguration.name}")
	private String lodViewConfiguration;

    @Value("${virtuoso.isql.username}")
    private List<String> isqlUsername;

    @Value("${virtuoso.isql.password}")
    private List<String> isqlPassword;

    @Value("${virtuoso.sftp.username}")
    private List<String> sftpUsername;

    @Value("${virtuoso.sftp.password}")
    private List<String> sftpPassword;

	@Value("${database.elasticConfiguration.name}")
	private String elasticConfiguration;
	
    @Value("${elastic.username}")
    private List<String> elasticUsername;

    @Value("${elastic.password}")
    private List<String> elasticPassword;

	@Value("${database.fileSystemConfiguration.name:#{null}}")
	private String fileSystemConfiguration;

	@Value("${database.fileSystemConfiguration.folder:#{null}}")
	private String fileSystemConfigurationFolder;

	@Value("${cache.labels.size}")
	private int cacheSize;

	@Value("${cache.labels.live-time}")
	private int liveTime;
	
	@Autowired
	private DatabaseRepository databaseRepository;
	
	@Lazy
	@Autowired
	private VocabularyRepository vocRepository;
	
	@Autowired
	private DataServiceRepository dataServiceRepository;

	@Autowired
	private VirtuosoConfigurationRepository virtuosoConfigurationRepository;

	@Autowired
	private BlazegraphConfigurationRepository blazegraphConfigurationRepository;

	@Autowired
	private GraphDBConfigurationRepository graphdbConfigurationRepository;

	@Autowired
	private ElasticConfigurationRepository elasticConfigurationRepository;

	@Autowired
	private FileSystemConfigurationRepository fileSystemConfigurationRepository;
	
	@Autowired
	private LodViewConfigurationRepository lodViewConfigurationRepository;

//	@Autowired
//	private Environment env;

	@Autowired
	private ResourceLoader resourceLoader;
	
	@Lazy
	@Autowired
	private ConfigUtils cfgUtils;

	@Bean
	public PasswordEncoder passwordEncoder() {
	    return new BCryptPasswordEncoder();
	}
	
	@Bean(name = "query-ontology")
	public OWLOntology getQueryOntology() {

		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = null;

		try (InputStream input = resourceLoader.getResource("classpath:" + ontologyFile)
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
		Map<Resource, List<String>> res = new LinkedHashMap<>();
		
		for (Map.Entry<Resource, FunctionImplementation> entry : FunctionProcessor.functions.entrySet()) {
			res.put(entry.getKey(), entry.getValue().getArguments());
		}
		
		for (Map.Entry<Resource, List<String>> entry : D2RMLOPVocabulary.functions.entrySet()) {
			if (D2RMLOPVocabulary.exposedFunctions.contains(entry.getKey())) {
				res.put(entry.getKey(), entry.getValue());
			}
		}
		
		return res;
	}
	
	@Bean(name = "preprocess-operations")
	public Map<Resource, List<String>> getPreprocessOperations() {
		Map<Resource, List<String>> res = new LinkedHashMap<>();
		
		for (Map.Entry<Resource, FunctionImplementation> entry : FunctionProcessor.operators.entrySet()) {
			res.put(entry.getKey(), entry.getValue().getArguments());
		}
		
		for (Resource entry : D2RMLOPVocabulary.exposedOperators) {
			List<String> arg = new ArrayList<>();
			arg.add("argument");
			res.put(entry, arg);
		}
		
		return res;
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
	
	@Bean(name = "rdf-vocabularies")
	@DependsOn("database")
	public VocabularyContainer<Vocabulary> getVocabularies(@Qualifier("database") Database database) {
		return cfgUtils.loadRDFVocabularies(database);
	}

	
	@Bean(name = "annotators")
	@DependsOn("database")
	public Map<String, DataService> getAnnotators(@Qualifier("database") Database database) {
		logger.info("Loading annotators");

		Map<String, DataService> res = new HashMap<>();
		
		for (DataService ds : dataServiceRepository.findByDatabaseIdAndTypeOrderByTitleAsc(database.getId(), DataServiceType.ANNOTATOR)) {
			res.put(ds.getIdentifier(), ds);
		}
		
		return res;
	}

	@Bean(name = "clusterers")
	@DependsOn("database")
	public Map<String, DataService> getClusterers(@Qualifier("database") Database database) {
		logger.info("Loading clusterers");

		Map<String, DataService> res = new HashMap<>();
		
		for (DataService ds : dataServiceRepository.findByDatabaseIdAndTypeOrderByTitleAsc(database.getId(), DataServiceType.CLUSTERER)) {
			res.put(ds.getIdentifier(), ds);
		}
		
		return res;
	}
	
	@Bean(name = "embedders")
	@DependsOn("database")
	public Map<String, DataService> getEmbedders(@Qualifier("database") Database database) {
		logger.info("Loading embedders");

		Map<String, DataService> res = new HashMap<>();
		
		for (DataService ds : dataServiceRepository.findByDatabaseIdAndTypeOrderByTitleAsc(database.getId(), DataServiceType.EMBEDDER)) {
			res.put(ds.getIdentifier(), ds);
		}
		
		return res;
	}

	
	@Bean(name = "triplestore-configurations")
	@DependsOn("database")
	public ConfigurationContainer<TripleStoreConfiguration> getTripleStoreConfigurations(@Qualifier("database") Database database) {

		Set<String> asProperties = new TreeSet<>();
		
		List<DataService> services = dataServiceRepository.findByDatabaseId(database.getId());
		for (DataService ds : services) {
			if (ds.getAsProperties() != null) {
				asProperties.addAll(ds.getAsProperties());
			}
		}
		asProperties.add(database.getResourcePrefix() + "graph/content");
		
		ConfigurationContainer<TripleStoreConfiguration> tscc = new ConfigurationContainer<>();
		
		int c = 1;
		for (int i = 0; i < virtuosoConfiguration.size(); i++) {
			Optional<VirtuosoConfiguration> db = virtuosoConfigurationRepository.findByName(virtuosoConfiguration.get(i));
			if (db.isPresent()) {
				VirtuosoConfiguration conf = db.get();
				conf.setIsqlUsername(isqlUsername.get(i));
				conf.setIsqlPassword(isqlPassword.get(i));
				if (sftpUsername != null) {
					conf.setSftpUsername(sftpUsername.get(i));
					conf.setSftpPassword(sftpPassword.get(i));
				}
			
				if (virtuosoLocalImport.size() == 0) {
					conf.setLocalImport(true); 
				} else {
					conf.setLocalImport(virtuosoLocalImport.get(i));
				}
				
				conf.setOrder(c++);

				int cc = 1;
				for (String s : asProperties) {
					conf.addGraph(s, cc++ + "");
				}
				
				tscc.add(conf);
//				System.out.println(">>> " + conf.getName() + " " + conf.getClass());
			}
		}
		
		for (int i = 0; i < virtuosoConfiguration.size(); i++) {
			Optional<BlazegraphConfiguration> db = blazegraphConfigurationRepository.findByName(virtuosoConfiguration.get(i));
			if (db.isPresent()) {
				BlazegraphConfiguration conf = db.get();
				conf.setSftpUsername(sftpUsername.get(i));
				conf.setSftpPassword(sftpPassword.get(i));
				
				if (virtuosoLocalImport.size() == 0) {
					conf.setLocalImport(true); 
				} else {
					conf.setLocalImport(virtuosoLocalImport.get(i));
				}
				
				tscc.add(conf);
//				System.out.println(">>> " + conf.getName() + " " + conf.getClass());
			}
		}
		
		for (int i = 0; i < virtuosoConfiguration.size(); i++) {
			Optional<GraphDBConfiguration> db = graphdbConfigurationRepository.findByName(virtuosoConfiguration.get(i));
			if (db.isPresent()) {
				GraphDBConfiguration conf = db.get();
				conf.setSftpUsername(sftpUsername.get(i));
				conf.setSftpPassword(sftpPassword.get(i));
				
				if (virtuosoLocalImport.size() == 0) {
					conf.setLocalImport(true); 
				} else {
					conf.setLocalImport(virtuosoLocalImport.get(i));
				}
				
				tscc.add(conf);
//				System.out.println(">>> " + conf.getName() + " " + conf.getClass());
			}
		}

		
		return tscc;
	}

	@Bean(name = "elastic-configurations")
	public ConfigurationContainer<ElasticConfiguration> getElasticConfigurations() {

		ConfigurationContainer<ElasticConfiguration> ecc = new ConfigurationContainer<>();
		
		if (elasticConfiguration != null) {
			String[] ec = elasticConfiguration.split(",");
			
			for (int i = 0; i < ec.length; i++) {
				Optional<ElasticConfiguration> db = elasticConfigurationRepository.findByName(ec[i]);
				if (db.isPresent()) {
					ElasticConfiguration conf = db.get();
					if (elasticUsername != null) {
						conf.setUsername(elasticUsername.get(i));
						conf.setPassword(elasticPassword.get(i));
					}
					
					if (conf.test()) {
						ecc.add(conf);
					}
				}
				
			}
		}
		
		return ecc;
	}

	@Bean(name = "lodview-configuration")
	public LodViewConfiguration getLodViewConfiguration() {
		return lodViewConfigurationRepository.findByName(lodViewConfiguration).orElseGet(null);
	}
	
	@Bean(name = "filesystem-configuration")
	@DependsOn({ "database" })
	public FileSystemConfiguration getFileSystemConfiguration(Database database) throws Exception {

		if (fileSystemConfiguration != null) {
			return fileSystemConfigurationRepository.findByName(fileSystemConfiguration).orElse(null);

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
				fsd.setDatabaseId(database.getId());
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
	
	@Bean(name = "skos-cache")
	public Cache getSkosCache() {
	    CacheManager singletonManager = CacheManager.create();
	    if (!singletonManager.cacheExists("skos")) {
		    singletonManager.addCache(new Cache("skos", cacheSize, false, false, liveTime, liveTime));
		    
			logger.info("Created skos cache.");
	    }
	    
	    return singletonManager.getCache("skos");
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
	
	@Bean(name = "api-cache")
	public org.ehcache.Cache<String,Map> getApiCache() {
		
		org.ehcache.CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder() 
			    .withCache("api",
			        CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, Map.class, ResourcePoolsBuilder.heap(20000))) 
			    .build(); 
		cacheManager.init(); 

		logger.info("Created api cache.");

		return cacheManager.getCache("api", String.class, Map.class); 
	}
	
//	@Bean(name = "api-cache")
//	public org.ehcache.Cache getApiCache() {
//	    CacheManager singletonManager = CacheManager.create();
//	    if (!singletonManager.cacheExists("api")) {
//		    singletonManager.addCache(new Cache("api", cacheSize, false, false, liveTime, liveTime));
//		    
//			logger.info("Created api cache.");
//	    }
//	    
//	    return singletonManager.getCache("api");
//	}
	
	@Bean(name = "thesauri-cache")
	public Cache getThesauriCache() {
	    CacheManager singletonManager = CacheManager.create();
	    if (!singletonManager.cacheExists("thesauri")) {
		    singletonManager.addCache(new Cache("thesauri", 100, false, false, liveTime, liveTime));
		    
			logger.info("Created thesauri cache.");
	    }
	    
	    return singletonManager.getCache("thesauri");
	}
	
	@Bean(name = "indices-cache")
	public Cache getIndicesCache() {
	    CacheManager singletonManager = CacheManager.create();
	    if (!singletonManager.cacheExists("indices")) {
		    singletonManager.addCache(new Cache("indices", cacheSize, false, false, liveTime, liveTime));
		    
			logger.info("Created indices cache.");
	    }
	    
	    return singletonManager.getCache("indices");
	    
	}

	@Bean(name = "system-mac-address")
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
	@DependsOn({ "triplestore-configurations" })
	public Searcher getSearcher(@Qualifier("triplestore-configurations") ConfigurationContainer<TripleStoreConfiguration> vc) {
		return new Searcher(vc.values().iterator().next().getSparqlEndpoint());
	}


	
	@Bean(name = "date-format")
	public SimpleDateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	}
	
    @Bean(name="mappingExecutor")
    public TaskExecutor mappingExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("MappingTask-");
        threadPoolTaskExecutor.setCorePoolSize(3);
        threadPoolTaskExecutor.setMaxPoolSize(3);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("Mapping ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }
    
    @Bean(name="shaclValidationExecutor")
    public TaskExecutor shaclValidationExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("ShaclValidationTask-");
        threadPoolTaskExecutor.setCorePoolSize(3);
        threadPoolTaskExecutor.setMaxPoolSize(3);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("ShaclValidation ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }
    
    @Bean(name="publishExecutor")
    public TaskExecutor publishExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("PublishTask-");
        threadPoolTaskExecutor.setCorePoolSize(3);
        threadPoolTaskExecutor.setMaxPoolSize(3);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("Publish ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }  
    
    @Bean(name="createDistributionExecutor")
    public TaskExecutor createDistributionExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("CreateDistributionTask-");
        threadPoolTaskExecutor.setCorePoolSize(1);
        threadPoolTaskExecutor.setMaxPoolSize(1);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("CreateDistribution ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }    
    
    @Bean(name="indexExecutor")
    public TaskExecutor indexExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("IndexTask-");
        threadPoolTaskExecutor.setCorePoolSize(1);
        threadPoolTaskExecutor.setMaxPoolSize(1);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("Index ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }    
    
    @Bean(name="pagedAnnotationValidationExecutor")
    public TaskExecutor pagedAnnotationValidationExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("PagedAnnotationValidationTask-");
        threadPoolTaskExecutor.setCorePoolSize(3);
        threadPoolTaskExecutor.setMaxPoolSize(3);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("PagedAnnotationValidation ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }
   
    @Bean(name="filterAnnotationValidationExecutor")
    public TaskExecutor filterAnnotationValidationExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("FilterAnnotationValidationTask-");
        threadPoolTaskExecutor.setCorePoolSize(3);
        threadPoolTaskExecutor.setMaxPoolSize(3);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("FilterAnnotationValidation ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }    
    
    @Bean(name="user-task-scheduler")
    public UserTaskScheduler poolScheduler() {
        return new UserTaskScheduler();
    }
    
    @Bean(name="annotationsExportExecutor")
    public TaskExecutor annotationsExportExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("AnnotationExportTask-");
        threadPoolTaskExecutor.setCorePoolSize(3);
        threadPoolTaskExecutor.setMaxPoolSize(3);
        threadPoolTaskExecutor.setQueueCapacity(600);
        threadPoolTaskExecutor.afterPropertiesSet();
        logger.info("AnnotationsExport ThreadPoolTaskExecutor set");
        return threadPoolTaskExecutor;
    }  

    private final Map<Object, ScheduledFuture<?>> scheduledTasks = new HashMap<>();
    
    public class UserTaskScheduler extends ThreadPoolTaskScheduler{

    	public ScheduledFuture<?> schedule(Runnable task, String cronExpression)  throws Exception {
    		ScheduledUserTask runnable = (ScheduledUserTask) task;
    		EnclosedObjectContainer<UserTaskDocument,UserTaskResponse,?> oc = runnable.getUserTaskContainer();
    		
    		if (scheduledTasks.get(runnable.getUserTaskContainer().getPrimaryId().toHexString()) != null) {
    			throw ScheduleException.alreadyScheduled(oc);
    		}
    		
    		CronTrigger trigger;
    		try {
    			trigger = new CronTrigger(cronExpression);
    		} catch (Exception ex) {
    			throw ScheduleException.invalidCronExpression(cronExpression);
    		}
    		
    		ScheduledFuture<?> future = super.schedule(task, trigger);
            
            scheduledTasks.put(runnable.getUserTaskContainer().getPrimaryId().toHexString(), future);

            return future;
        }

    	public void unschedule(ObjectId id) {
            ScheduledFuture<?> future = scheduledTasks.remove(id.toHexString());

            if (future != null) {
            	future.cancel(true);
            }
            
//            return future;
        }

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

//	@Bean(name = "w3c-anno-jsonld-context")
//	public JsonLDWriteContext getContext() {
//		JsonLDWriteContext ctx = new JsonLDWriteContext();
//		
//        HttpGet request = new HttpGet("http://www.w3.org/ns/anno.jsonld");
//        try (CloseableHttpResponse response = HttpClients.createDefault().execute(request)) {
//        	String contextString = EntityUtils.toString(response.getEntity());
//        	
//        	
//        	ObjectMapper mapper = new ObjectMapper();
//        	Map<String, Object> contextMap = mapper.readValue(contextString, Map.class);
//        	
//        	
//        	Map<String, Object> frame = new HashMap<>();
//	        frame.put("@type" , "http://www.w3.org/ns/oa#Annotation");
////        	frame.put("@type" , "oa:Annotation");
//	        ctx.setFrame(frame);
////	        frame.put("@context", contextMap);
//	        ctx.setJsonLDContext(contextMap);
//	        
////	        ctx.setJsonLDContext(EntityUtils.toString(response.getEntity()));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		return ctx;
//	}
	

	@Value("${jsonld.definition.folder}")
	private String jsonLdFolder;
	
	@Bean(name = "annotation-jsonld-context")
	public Map<String, Object> getAnnotationContext() {
		
		try (InputStream inputStream = resourceLoader.getResource("classpath:" + jsonLdFolder + "anno.jsonld").getInputStream()) {
			String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			return (Map<String, Object>)new ObjectMapper().readValue(str, Map.class);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Failed to load anno.jsonld");
			return null;
		}
		
	}
	
	@Bean(name = "dataset-schema-jsonld-context")
	public Map<String, Object> getDatasetSchemaContext() {
		
		try (InputStream inputStream = resourceLoader.getResource("classpath:" + jsonLdFolder + "dataset.jsonld").getInputStream()) {
			String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			return (Map<String, Object>)new ObjectMapper().readValue(str, Map.class);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Failed to load dataset.jsonld");
			return null;
		}
	}
	
	@Bean(name = "dataset-metadata-jsonld-context")
	public Map<String, Object> getDatasetMetadataContext() {
		
		try (InputStream inputStream = resourceLoader.getResource("classpath:" + jsonLdFolder + "dataset-metadata.jsonld").getInputStream()) {
			String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			return (Map<String, Object>)new ObjectMapper().readValue(str, Map.class);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Failed to load dataset-metadata.jsonld");
			return null;
		}
	}

	@Bean(name = "label-jsonld-context")
	public Map<String, Object> getLabelContext() {
		
		try (InputStream inputStream = resourceLoader.getResource("classpath:" + jsonLdFolder + "label.jsonld").getInputStream()) {
			String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			return (Map<String, Object>)new ObjectMapper().readValue(str, Map.class);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Failed to load label.jsonld");
			return null;
		}
	}
	
	@Bean(name = "d2rml-jsonld-context")
	public Map<String, Object> getD2rmlContext() {
		
		try (InputStream inputStream = resourceLoader.getResource("classpath:" + jsonLdFolder + "d2rml.jsonld").getInputStream()) {
			String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
			return (Map<String, Object>)new ObjectMapper().readValue(str, Map.class);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Failed to load d2rml.jsonld");
			return null;
		}
		
	}

	private static Map<String, Object> getRemoteJsonLDContext(String uri) throws ClientProtocolException, IOException {
        HttpGet request = new HttpGet(uri);
        try (CloseableHttpResponse response = HttpClients.createDefault().execute(request)) {
        	String contextString = EntityUtils.toString(response.getEntity());
        	return new ObjectMapper().readValue(contextString, Map.class);
        }
		
	}
	
	public static void main(String[] args) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		
		JsonLDWriteContext ctx = new JsonLDWriteContext();
		
		Map<String, Object> annoContextMap = getRemoteJsonLDContext("http://www.w3.org/ns/anno.jsonld");
		
       	Map<String, Object> frame = new HashMap<>();
        ctx.setFrame(frame);
    	frame.put("@type" , "http://www.w3.org/ns/oa#Annotation");
    	frame.put("@context", annoContextMap.get("@context"));

        
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, "file://d:/data/crafted/annotation-test.ttl");
//	        
        JsonLdOptions options = new JsonLdOptions();
//        options.setProduceGeneralizedRdf(true);
        options.setCompactArrays(true);
        options.useNamespaces = true ; 
        options.setUseNativeTypes(true); 	      
        options.setOmitGraph(false);
//	        
        final RDFDataset jsonldDataset = (new JenaRDF2JSONLD()).parse(DatasetFactory.wrap(model).asDatasetGraph());
        Object obj = (new JsonLdApi(options)).fromRDF(jsonldDataset, true);
//	        
        Map<String, Object> jn = JsonLdProcessor.frame(obj, frame, options);
//	     
        StringWriter sw = new StringWriter();
        mapper.writerWithDefaultPrettyPrinter().writeValue(sw, jn);
        System.out.println(sw.toString());
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
	

//	@Bean
////	@Bean(name = "object-mapper")
//	public ObjectMapper objectMapper() {
//		System.out.println("CALLING");
//	    ObjectMapper mapper = new ObjectMapper();
////	    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//
//	    SimpleModule module = new SimpleModule();
//	    module.addSerializer(IndexElement.class, new IndexElementSerializer());
//	    mapper.registerModule(module);
//	    
////	    CollectionType propertiesListType = mapper.getTypeFactory().constructCollectionType(List.class, Property.class);
////	    SimpleModule module = new SimpleModule();
////	    module.addSerializer(new PropertyListJSONSerializer(propertiesListType));
////	    mapper.registerModule(module);
//
//	    return mapper;
//	}
	

	public static class JenaRDF2JSONLD implements com.github.jsonldjava.core.RDFParser {
	    NodeToLabel labels = SyntaxLabels.createNodeToLabel() ;

	    @Override
	    public RDFDataset parse(Object object) throws JsonLdError {
	        RDFDataset result = new RDFDataset() ;
	        if ( object instanceof DatasetGraph )
	        {
	            DatasetGraph dsg = (DatasetGraph)object ;

	            Iterator<Quad> iter = dsg.find() ;
	            for ( ; iter.hasNext() ; )
	            {
	                Quad q = iter.next() ;
	                Node s = q.getSubject() ;
	                Node p = q.getPredicate() ;
	                Node o = q.getObject() ;
	                Node g = q.getGraph() ;
	                
	                String gq = null ;
	                if ( g != null && ! Quad.isDefaultGraph(g) ) {
	                    gq = blankNodeOrIRIString(g) ;
	                    if ( gq == null )
	                        throw new RiotException("Graph node is not a URI or a blank node") ;
	                }
	                
	                String sq = blankNodeOrIRIString(s) ;
	                if ( sq == null )
	                    throw new RiotException("Subject node is not a URI or a blank node") ;
	                
	                String pq = p.getURI() ;
	                if ( o.isLiteral() )
	                {
	                    String lex = o.getLiteralLexicalForm() ; 
	                    String lang = o.getLiteralLanguage() ;
	                    String dt = o.getLiteralDatatypeURI() ;
	                    if (lang != null && lang.length() == 0)
	                    {
	                        lang = null ;
	                        //dt = RDF.getURI()+"langString" ;
	                    }
	                    if (dt == null )
	                        dt = XSDDatatype.XSDstring.getURI() ;

	                    result.addQuad(sq, pq, lex, dt, lang, gq) ;
	                }
	                else
	                {
	                    String oq = blankNodeOrIRIString(o) ;
	                    result.addQuad(sq, pq, oq, gq) ;
	                }
	            }
	        }                
	        else
	            Log.warn(JenaRDF2JSONLD.class, "unknown") ;
	        return result ;
	    }

	    private String blankNodeOrIRIString(Node x)
	    {
	        if ( x.isURI() ) return x.getURI() ;
	        if ( x.isBlank() )
	            return labels.get(null,  x) ;
	        return null ;
	    }
	}
}
