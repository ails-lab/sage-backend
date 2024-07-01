package ac.software.semantic;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.bson.types.ObjectId;
import org.semanticweb.owlapi.model.OWLOntology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.security.crypto.password.PasswordEncoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.FilterAnnotationValidation;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.PrototypeDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.UserTaskDocument;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.state.IndexingState;
import ac.software.semantic.model.constants.type.DocumentType;
import ac.software.semantic.model.constants.type.MappingType;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.IndexStateOld;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.DistributionDocumentRepository;
import ac.software.semantic.repository.core.EmbedderDocumentRepository;
import ac.software.semantic.repository.core.FileDocumentRepository;
import ac.software.semantic.repository.core.FilterAnnotationValidationRepository;
import ac.software.semantic.repository.core.IndexDocumentRepository;
import ac.software.semantic.repository.core.IndexStructureRepository;
import ac.software.semantic.repository.core.MappingDocumentRepository;
import ac.software.semantic.repository.core.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.core.PrototypeDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.core.UserTaskDocumentRepository;
import ac.software.semantic.repository.root.DatabaseRepository;
import ac.software.semantic.service.ContentService;
import ac.software.semantic.service.DatabaseConfigurationService;
import ac.software.semantic.service.DatabaseService;
import ac.software.semantic.service.LodViewService;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.UserService;
import ac.software.semantic.service.UserTaskService;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import edu.ntua.isci.ac.common.db.rdf.wktLiteralDatatype;
import edu.ntua.isci.ac.common.utils.Counter;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

@SpringBootApplication(exclude = {
		  MongoAutoConfiguration.class, 
		  MongoDataAutoConfiguration.class }
)
@IntegrationComponentScan
@EnableIntegration
@EnableScheduling
@EnableAsync(proxyTargetClass = true)
public class Application implements CommandLineRunner {

	@Autowired
	private DatabaseRepository dbRepository;

	@Autowired
	private PrototypeDocumentRepository prototypeRepository;

	@Autowired
	private IndexDocumentRepository indexRepository;

	@Autowired
	private IndexStructureRepository indexStructureRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private EmbedderDocumentRepository embedderRepository;

	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;
	
	@Autowired
	private MappingDocumentRepository mappingRepository;

	@Autowired
	private FilterAnnotationValidationRepository favRepository;

	@Autowired
	private PagedAnnotationValidationRepository pavRepository;

	@Autowired
	private AnnotationEditGroupRepository aegRepository;

	@Autowired
	private DistributionDocumentRepository distributionRepository;

	@Autowired
	private FileDocumentRepository fileRepository;

	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private UserTaskDocumentRepository userTaskRepository;

	@Autowired
	private ContentService contentService;

	@Autowired
	private DatabaseService dbService;

	@Autowired
	private UserService userService;

	@Autowired
	private TaskService taskService;

	@Autowired
	private LodViewService lodViewService;

	@Autowired
	private UserTaskService userTaskService;

    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

    @Autowired
    @Qualifier("system-mac-address")
    private String mac;
    
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> tripleStoreConfigurations;
	
    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    Logger logger = LoggerFactory.getLogger(Application.class);
    

	@Override
	public void run(String... args) throws Exception {
		
		logger.info("Using legacy scheme uris: " + legacyUris);
		
		logger.info("System MAC address: " + mac + ".");
		
		logger.info("Current database: " + database.getName() + ".");
		String vs = "";
		for (Map.Entry<String, TripleStoreConfiguration> entry : tripleStoreConfigurations.getNameMap().entrySet()) {
			if (vs.length() > 0) {
				vs += ", ";
			}
			vs += entry.getKey() + " @ " + entry.getValue().getSparqlEndpoint();
		}
		logger.info("Current triple stores: " + vs);
		logger.info("Current file system: " + fileSystemConfiguration.getName() + " @ " + fileSystemConfiguration.getDataFolder());
		String es = "";
		for (Map.Entry<String, ElasticConfiguration> entry : elasticConfigurations.getNameMap().entrySet()) {
			if (es.length() > 0) {
				es += ", ";
			}
			es += entry.getKey() + " @ " + entry.getValue().getIndexIp() + ":" + entry.getValue().getIndexPort();
		}
		logger.info("Current elasticsearches: " + es);
		
		logger.info("Failing unfinished tasks ");
		taskService.failUnfinishedTasks();
		contentService.failUnfinishedTasks();
		
		lodViewService.updateLodView();
//		userService.addMissingDatabaseUsersToVirtuoso();
	
//		VirtuosoConfiguration vc = virtuosoConfiguration.get("stirdata-1-virtuoso-chameleon");
//		sftpUploadGateway.upload(new File("d:/data/tt4.xml"), vc);
//		sftpUploadGateway.upload(new File("d:/data/tt5.xml"), vc);
//		
//		sftpDeleteGateway.deleteFile("tt5.xml", vc);


		// load vocabulary prefixes
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(Vocabulary.class));
        Set<BeanDefinition> components = provider.findCandidateComponents("edu/ntua/isci/ac/lod");
        for (BeanDefinition c : components) {
        	Class.forName(c.getBeanClassName());
        }
        
        logger.info("Activating scheduled tasks.");
        userTaskService.activateScheduled();
        
        checkPublicPaths();
//        System.out.println(UUID.randomUUID());
        
//        String sparqlQuery = "SELECT ?uri ?legalName ?addressCountry WHERE { ?uri a <http://data.europa.eu/s66#Organisation> . ?uri <http://data.europa.eu/s66#legalName> ?legalName . ?uri <http://data.europa.eu/s66#hasSite>/<http://data.europa.eu/s66#hasAddress>/<http://data.europa.eu/s66#addressCountry> ?addressCountry }";
//        
//        ElasticConfiguration ec = elasticConfigurations.getIdMap().values().iterator().next();
//        
//        int k = 0;
//        QueryExecution qe = QueryExecutionFactory.sparqlService("http://jumbo.image.ece.ntua.gr:8902/sparql", QueryFactory.create(sparqlQuery, Syntax.syntaxSPARQL_11));
//        ResultSet rs = qe.execSelect();
//        while (rs.hasNext()) {
//        	QuerySolution qs = rs.next();
//        	String legalName = qs.get("legalName").toString();
//        	String addressCountry = qs.get("addressCountry").toString();
//        	
//    		SearchRequest.Builder searchRequest = new SearchRequest.Builder();
//    		searchRequest.index("sage-dev-null-cordis-organisations");
//
//    		String elasticQuery = "{ \"query\": { \"bool\": { \"must\": [ "
////    				+ "{ \"match\": { \"legalName\": { \"query\": \"" + legalName + "\", \"operator\" : \"and\", \"fuzziness\": \"AUTO\" } } }, "
//    				+ "{ \"match\": { \"legalName\": { \"query\": \"" + legalName + "\", \"operator\" : \"and\" } } }, "
//    				+ "{ \"term\": { \"addressCountry\": \"" + addressCountry + "\" }  }"
//    				+ "] } } }";
//
//        	searchRequest.withJson(new StringReader(elasticQuery));
//        	
//        	SearchResponse<Object> searchResponse = ec.getClient().search(searchRequest.build(), Object.class);
//    			
//    		ObjectMapper mapper = new ObjectMapper();
//    		ArrayNode array = mapper.createArrayNode();
//
//    		List<String> ff = new ArrayList<>();
//    		ff.add("legalName");
//    		ff.add("identifier");
//    		ff.add("addressCountry");
//    		
//    		HitsMetadata<Object> hm = searchResponse.hits();
//    		
//    		int i = 0;
//    		for (Hit<Object> hit : hm.hits()) {
//    			Map<String, String> map = (Map<String, String>)hit.source();
//    			
//    			ObjectNode match = mapper.createObjectNode();
////    			match.put("uri", map.get("iri").toString());
//    			match.put("score", hit.score());
//    				
//    			for (String f : ff) {
//    				ArrayNode arr = mapper.createArrayNode();
//    				Object obj = map.get(f);
//    				if (obj != null) {
//    					if (obj instanceof Collection) {
//    						for (Object s : ((Collection)obj)) {
//    							arr.add(s.toString());
//    						}
//    					} else {
//    						arr.add(obj.toString());
//    					}
//    					
//    					match.put(f, arr);
//    				}
//
//    			}
//    			
//    			array.add(match);
//    			if (hm.hits().size() > 1) {
//    				if (i++ == 0) {
//    					System.out.println(">>> " + k++ + " " + legalName);
//    				}
//    				System.out.println("\t" + mapper.writeValueAsString(match));
//    			}
//    		}
//    		
//    		
//    		
//    		
//        }
        
        
        
//	    for (TaskDescription task : taskRepository.findByDatabaseId(database.getId())) {
//	    	  if (task.getAnnotatorId() != null) {
//	    		  task.setDocumentType(DocumentType.ANNOTATOR);
//	    	  } else if (task.getEmbedderId() != null) {
//	    		  task.setDocumentType(DocumentType.EMBEDDER);
//	    	  } else if (task.getPagedAnnotationValidationId() != null) {
//	    		  task.setDocumentType(DocumentType.PAGED_ANNOTATION_VALIDATION);
//	    	  } else if (task.getFilterAnnotationValidationId() != null) {
//	    		  task.setDocumentType(DocumentType.FILTER_ANNOTATION_VALIDATION);
//	    	  } else if (task.getMappingId() != null) {
//	    		  task.setDocumentType(DocumentType.MAPPING);
//	    	  } else if (task.getFileId() != null) {
//	    		  task.setDocumentType(DocumentType.FILE);
//	    	  } else if (task.getUserTaskId() != null) {
//	    		  task.setDocumentType(DocumentType.USER_TASK);
//	    	  } else if (task.getIndexId() != null) {
//	    		  task.setDocumentType(DocumentType.INDEX);
//	    	  } else if (task.getDatasetId() != null) {
//	    		  task.setDocumentType(DocumentType.DATASET);
//	    	  }
//	    	
//	    	  taskRepository.save(task);
//	    	
//	    }   

//        for (Dataset dataset : datasetRepository.findByDatabaseId(database.getId())) {
//        	System.out.println(dataset.getName() + " " + dataset.getDatasetType() + " " + dataset.getScope() + " " + dataset.getTypeUri());
//    	}

//        System.out.println(database.getName());
//        
//        if (database.getName().equals("stirdata")) {
//          ObjectId stirdataUserId = new ObjectId("652e34bda7b11b00069410be");
//    	  System.out.println("CHECKING DATASETS");
//
//	      for (TaskDescription dataset : taskRepository.findByDatabaseId(database.getId())) {
//	    	  ObjectId userId = dataset.getUserId();
//	    	  if (userId != null && userId.equals(new ObjectId("5dde55213e5b954808fa7a0e"))) {
//	    		  System.out.println("ACHORT");
////		    	  dataset.setUserId(stirdataUserId);
////		    	  taskRepository.save(dataset);
//	    	  } else {
//	    		  System.out.println("****** " + dataset.getId());
//	    	  }
//	    	  
//	    	}
//        }
        
//        for (Dataset dataset : datasetRepository.findByDatabaseId(database.getId())) {
//        	System.out.println("CHECKING MAPPINGS");
//        	for (MappingDocument mdoc : mappingRepository.findByDatasetId(dataset.getId())) {
//        		if (mdoc.getDatabaseId() == null || mdoc.getDatasetId() == null || mdoc.getDatasetUuid() == null) {
//        			System.out.println(mdoc.getName() + " " + mdoc.getDatabaseId()  + " " + mdoc.getDatasetId() + " " + mdoc.getDatasetUuid());
////            		mdoc.setDatabaseId(database.getId());
//   		mdoc.setDatasetUuid(dataset.getUuid());
////	        		mdoc.setDatasetId(dataset.getId());
//	        		
////	        		mappingRepository.save(mdoc);
//        		}
//        	}
//        	
//        	System.out.println("CHECKING ANNOTATORS");
//        	for (AnnotatorDocument mdoc : annotatorRepository.findByDatasetUuid(dataset.getUuid())) {
//        		if (mdoc.getDatabaseId() == null || mdoc.getDatasetId() == null || mdoc.getDatasetUuid() == null) {
//        			System.out.println(mdoc.getAnnotator() + " " + mdoc.getDatabaseId()  + " " + mdoc.getDatasetId() + " " + mdoc.getDatasetUuid());
////            		mdoc.setDatabaseId(database.getId());
////////////   		mdoc.setDatasetUuid(dataset.getUuid());
//	        		mdoc.setDatasetId(dataset.getId());
//        			
////        			annotatorRepository.save(mdoc);
//        		}
//        	}
//
//        	System.out.println("CHECKING EMBEDDERS");
//        	for (EmbedderDocument mdoc : embedderRepository.findByDatasetUuid(dataset.getUuid())) {
//        		if (mdoc.getDatabaseId() == null || mdoc.getDatasetId() == null || mdoc.getDatasetUuid() == null) {
//        			System.out.println(mdoc.getEmbedder() + " " + mdoc.getDatabaseId()  + " " + mdoc.getDatasetId() + " " + mdoc.getDatasetUuid());
////            		mdoc.setDatabaseId(database.getId());
////////////   		mdoc.setDatasetUuid(dataset.getUuid());
//	        		mdoc.setDatasetId(dataset.getId());
//	        		
////	        		embedderRepository.save(mdoc);
//        		}
//        	}
//
//        	
//        	System.out.println("CHECKING PAVS");
//        	for (PagedAnnotationValidation mdoc : pavRepository.findByDatasetUuid(dataset.getUuid())) {
//        		if (mdoc.getDatabaseId() == null || mdoc.getDatasetId() == null || mdoc.getDatasetUuid() == null) {
//        			System.out.println(mdoc.getName() + " " + mdoc.getDatabaseId()  + " " + mdoc.getDatasetId() + " " + mdoc.getDatasetUuid());
////            		mdoc.setDatabaseId(database.getId());
////////////   		mdoc.setDatasetUuid(dataset.getUuid());
//	        		mdoc.setDatasetId(dataset.getId());
//        			
////        			pavRepository.save(mdoc);
//        		}
//        	}
//        	
//        	System.out.println("CHECKING FAVS");
//        	for (FilterAnnotationValidation mdoc : favRepository.findByDatasetUuid(dataset.getUuid())) {
//        		if (mdoc.getDatabaseId() == null || mdoc.getDatasetId() == null || mdoc.getDatasetUuid() == null) {
//        			System.out.println(mdoc.getName() + " " + mdoc.getDatabaseId()  + " " + mdoc.getDatasetId() + " " + mdoc.getDatasetUuid());
////            		mdoc.setDatabaseId(database.getId());
////////////   		mdoc.setDatasetUuid(dataset.getUuid());
//	        		mdoc.setDatasetId(dataset.getId());
//
////            		favRepository.save(mdoc);
//        		}
//        	}
//        	
//        	System.out.println("CHECKING AEGS");
//        	for (AnnotationEditGroup mdoc : aegRepository.findByDatasetUuid(dataset.getUuid())) {
//        		if (mdoc.getDatabaseId() == null || mdoc.getDatasetId() == null || mdoc.getDatasetUuid() == null) {
//        			System.out.println(mdoc.getUuid() + " " + mdoc.getDatabaseId()  + " " + mdoc.getDatasetId() + " " + mdoc.getDatasetUuid());
//            		
//        			mdoc.setDatabaseId(database.getId());
////////////   		mdoc.setDatasetUuid(dataset.getUuid());
//	        		mdoc.setDatasetId(dataset.getId());
//	        		
////	        		aegRepository.save(mdoc);
//        		}
//        	}
//
//
//        }
        
//    	System.out.println("ADD THESAURUS ID TO ANNOTATORS");
//    	for (AnnotatorDocument mdoc : annotatorRepository.findByDatabaseId(database.getId())) {
//    		if (mdoc.getThesaurus() != null) {
//    			System.out.println(mdoc.getUuid() + " " + mdoc.getThesaurus());
//    			Optional<Dataset> dataset = datasetRepository.findByIdentifierAndDatabaseId(mdoc.getThesaurus(), database.getId());
//    			System.out.println(dataset.get().getId() + " " + dataset.get().getName()); 
//    			
//    			mdoc.setThesaurusId(dataset.get().getId());
//    			annotatorRepository.save(mdoc);
////    			break;
//    		}
//    	}
        
//    	System.out.println("ADD GROUP AND ORDER TO ANNOTATORS");
//    	for (AnnotatorDocument mdoc : annotatorRepository.findByDatabaseId(database.getId())) {
//    		if (mdoc.getOnClass() != null) {
//    			mdoc.setGroup(mdoc.getOnClass().hashCode());
//    		} else {
//    			mdoc.setGroup(String.join(",", mdoc.getOnProperty()).hashCode());
//    		}
//			annotatorRepository.save(mdoc);
//    	}        
//    	for (Dataset dataset : datasetRepository.findByDatabaseId(database.getId())) {
//    		Map<Integer, Counter> map = new HashMap<>();
//    		
//	    	for (AnnotatorDocument mdoc : annotatorRepository.findByDatasetUuid(dataset.getUuid())) {
//	    		int group = mdoc.getGroup();
//	    		Counter c = map.get(group);
//	    		if (c == null) {
//	    			c = new Counter(0);
//	    			map.put(group, c);
//	    		}
//	    		mdoc.setOrder(c.getValue());
//	    		c.increase();
//	    		
//				annotatorRepository.save(mdoc);
//	    	}
//    	}
    	
        // mapping ordering
//      for (Dataset dataset : datasetRepository.findByDatabaseId(database.getId())) {
//    	System.out.println("CHECKING MAPPINGS " + dataset.getName());
//    	List<MappingDocument> mdocs = mappingRepository.findByDatasetId(dataset.getId());
//    	Collections.sort(mdocs, new Comparator<MappingDocument>() {
//
//			@Override
//			public int compare(MappingDocument o1, MappingDocument o2) {
//				if (o1.getType() == MappingType.HEADER && o2.getType() == MappingType.CONTENT) {
//					return -1;
//				} else if (o2.getType() == MappingType.HEADER && o1.getType() == MappingType.CONTENT) {
//					return 1;
//				} else {
//					return o1.getName().compareTo(o2.getName());
//				}
//			}
//    		
//    	});
//    	
//    	int i = 0;
//    	for (MappingDocument mdoc : mdocs) {
//    		System.out.println("\t" + mdoc.getName() + " " + mdoc.getOrder());
//    		mdoc.setOrder(i++);
//    		mappingRepository.save(mdoc);
//    	}
//      }
        
        // index ordering
//      for (Dataset dataset : datasetRepository.findByDatabaseId(database.getId())) {
//    	System.out.println("CHECKING MAPPINGS " + dataset.getName());
//    	List<IndexDocument> mdocs = indexRepository.findByDatasetId(dataset.getId());
//
//    	int i = 0;
//    	for (IndexDocument mdoc : mdocs) {
//    		System.out.println("\t" + mdoc.getUuid() + " " + mdoc.getOrder());
//    		mdoc.setOrder(i++);
//    		indexRepository.save(mdoc);
//    	}
//      }        
        
        // index migration
//        for (Dataset dataset : datasetRepository.findByDatabaseId(database.getId())) {
//        	List<IndexStateOld> indices = dataset.getIndex();
//        	if (indices != null) {
//        		for (IndexStateOld is : indices) {
//        			System.out.println(dataset.getName() + " " + is.getElasticConfigurationId() + " " + is.getIndexStructureId());
//        			
//        			String uuid = UUID.randomUUID().toString();
//
//        			IndexDocument idoc = new IndexDocument();
//        			idoc.setUuid(uuid);
//        			idoc.setDatabaseId(database.getId());
//        			idoc.setUserId(dataset.getUserId());
//
//        			idoc.setDatasetId(dataset.getId());
//        			idoc.setDatasetUuid(dataset.getUuid());
//        			idoc.setIndexStructureId(is.getIndexStructureId());
//        			idoc.setElasticConfigurationId(is.getElasticConfigurationId());
//        			
//        			idoc.setUpdatedAt(new Date());
//        			
//        			CreateState cs = idoc.getCreateState(is.getElasticConfigurationId());
//        			IndexingState iss = is.getIndexState();
//        			if (iss == IndexingState.INDEXED) {
//        				cs.setCreateState(CreatingState.CREATED);
//        			} else if (iss == IndexingState.INDEXING) {
//        				cs.setCreateState(CreatingState.CREATING);
//        			} else if (iss == IndexingState.INDEXING_FAILED) {
//        				cs.setCreateState(CreatingState.CREATING_FAILED);
//        			} else if (iss == IndexingState.NOT_INDEXED) {
//        				cs.setCreateState(CreatingState.NOT_CREATED);
//        			} else if (iss == IndexingState.UNINDEXING) {
//        				cs.setCreateState(CreatingState.DESTOYING);
//        			} else if (iss == IndexingState.UNINDEXING_FAILED) {
//        				cs.setCreateState(CreatingState.DESTOYING_FAILED);
//        			}
//        			cs.setCreateStartedAt(is.getIndexStartedAt());
//        			cs.setCreateCompletedAt(is.getIndexCompletedAt());
//        			cs.setElasticConfigurationId(is.getElasticConfigurationId());
//
//        			idoc = indexRepository.save(idoc);
//        			
//        			is.setIndexId(idoc.getId());
//        		}
//        		
//        		datasetRepository.save(dataset);
//        		
//        	}
//        }
        
        
//		checkIndices();
		
//		Message<Boolean> fileDeleteRequest = MessageBuilder.withPayload(true)
//	    		   .setHeader(FileHeaders.REMOTE_DIRECTORY, "imports/")
//	    		   .setHeader(FileHeaders.REMOTE_FILE, "60e2a345-0cea-4e85-a856-6876df1c7e47.trig").build();
//
	    		    
//		userService.transferUsersToNewConfiguration();
		
//		userService.deleteAllUsers();
//		
		//Create new database
//		dbService.createDatabase("europeana", "Europeana");
//		
//		Optional<Database> db = dbRepository.findByName("europeana");
//		
//		dbcService.createVirtuosoConfiguration(db.get().getId(), "europeana-virtuoso-chameleon", "http://virtuoso.ails.ece.ntua.gr:8891/sparql");
//		dbcService.createVirtuosoConfiguration(db.get().getId(), "europeana-virtuoso-kimon", "http://kimon.image.ece.ntua.gr:8890/sparql", "jdbc:virtuoso://localhost:1111/charset=UTF-8/log_enable=2");
//		dbcService.createElasticConfiguration(db.get().getId(), "europeana-elastic-chameleon", "147.102.11.33", "europeana-data", "europeana-vocabulary");

		
//		userService.deleteAllUsers();
//		
//		userService.createUser("achort", "achort", UserType.ADMIN);
				 
//		checkPaths();
        
//        IndexStructure st = indexStructureRepository.findById(new ObjectId("653fab21bf8f715010d3a093")).get();
//        for (IndexKeyMetadata ikm : st.getKeysMetadata()) {
//        	if ( ikm.getAnalyzer() != null) {
//            	System.out.println(ikm.getName() + " ");
//        		 System.out.println(ikm.getAnalyzer().buildName());
//        		 System.out.println(ikm.getAnalyzer().buildCharFilter());
//        		 ikm.getAnalyzer().toElasticAnalyzer();
//        	}
//        }
        
//        System.out.println(passwordEncoder.encode("6985Alex"));
        
        RDFDatatype rtype = wktLiteralDatatype.theWktLiteralType;
        TypeMapper.getInstance().registerDatatype(rtype);
        
        logger.info("Ready.");
		
	}
	
	@Autowired
	private PasswordEncoder passwordEncoder;
	
	private void checkPublicPaths() throws Exception {
		File df = new File(fileSystemConfiguration.getDataFolder());
		if (!df.exists()) {
			throw new Exception("Data folder " + df.getAbsolutePath() + " does not exits");
		}

		df = new File(fileSystemConfiguration.getPublicFolder());
		df.mkdir();

	}

	
}

