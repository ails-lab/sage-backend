package ac.software.semantic;


import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.bson.types.ObjectId;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.semanticweb.owlapi.model.OWLOntology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.outbound.SftpMessageHandler;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.APIIndexController;
import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.SemanticProperty;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DatabaseRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.SemanticPropertyRepository;
import ac.software.semantic.repository.UserRepository;
import ac.software.semantic.service.CollectionsService;
import ac.software.semantic.service.DatabaseConfigurationService;
import ac.software.semantic.service.DatabaseService;
import ac.software.semantic.service.ExecuteMonitor;
import ac.software.semantic.service.IndexService;
import ac.software.semantic.service.LodViewService;
import ac.software.semantic.service.PagedAnnotationValidationService;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.UserService;

@SpringBootApplication
@IntegrationComponentScan
@EnableIntegration
@EnableScheduling
@EnableAsync(proxyTargetClass = true)
public class Application implements CommandLineRunner {

	@Autowired
	private DatabaseConfigurationService dbcService;


	@Autowired
	private DatabaseRepository dbRepository;
	
	@Autowired
	private DatabaseService dbService;

	@Autowired
	private UserService userService;

	@Autowired
	private TaskService taskService;

	@Autowired
	private LodViewService lodViewService;

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
		
		lodViewService.updateLodView();
	
//		VirtuosoConfiguration vc = virtuosoConfiguration.get("stirdata-1-virtuoso-chameleon");
//		sftpUploadGateway.upload(new File("d:/data/tt4.xml"), vc);
//		sftpUploadGateway.upload(new File("d:/data/tt5.xml"), vc);
//		
//		sftpDeleteGateway.deleteFile("tt5.xml", vc);

		userService.addMissingDatabaseUsersToVirtuoso();
		
//		checkIndices();
		
//		Message<Boolean> fileDeleteRequest = MessageBuilder.withPayload(true)
//	    		   .setHeader(FileHeaders.REMOTE_DIRECTORY, "imports/")
//	    		   .setHeader(FileHeaders.REMOTE_FILE, "60e2a345-0cea-4e85-a856-6876df1c7e47.trig").build();
//
	    		    
//		userService.transferUsersToNewConfiguration();
		
//		userService.deleteAllUsers();
//		
//		userService.createUser("amavro", "amavro", UserType.NORMAL);
		
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
//		userService.createUser("tzortz", "tzortz", UserType.ADMIN);
//		userService.createUser("sbekiaris", "sbekiaris", UserType.ADMIN);
//		userService.createUser("achristaki", "achristaki", UserType.ADMIN);
//		userService.createUser("root", "root", UserType.SUPER);
				 
//		checkPaths();
		
	}
	
	private void checkIndices() {
		ElasticConfiguration ec = elasticConfigurations.values().iterator().next(); //legacy get first!
		
		try(RestHighLevelClient client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost(ec.getIndexIp(), 9200, "http")))) {
		
			GetIndexRequest getRequest = new GetIndexRequest(ec.getIndexDataName()); 
				
			logger.info("Data index: " + client.indices().exists(getRequest, RequestOptions.DEFAULT));
			
			if (!client.indices().exists(getRequest, RequestOptions.DEFAULT)) {
			
				CreateIndexRequest request = new CreateIndexRequest(ec.getIndexDataName());
				
//				request.settings( 
//						"{ " +
//				        "     \"analysis\": { " +
//				        "       \"analyzer\": { " +
//				        "         \"stem_analyzer\": {" +
//			            "            \"type\" : \"custom\", " +
//				        "            \"tokenizer\" : \"standard\", " +
//				        "            \"filter\": [\"lower_greek\", \"stem_greek\"] " +
//				        "         } " +
//				        "       }, " +
//				        "       \"filter\": { " +
//				        "         \"lower_greek\": { " +
//				        "           \"type\": \"lowercase\", " +
//				        "           \"language\": \"greek\" " +
//				        "         }, " +
//				        "         \"stem_greek\": { " +
//				        "           \"type\": \"skroutz_stem_greek\" " +
//				        "         } " +
//				        "       } " +
//				        "     } " +
//				        " } ",
//				        XContentType.JSON);
				  request.mapping("_doc", //??????
						" { " +
					    "      \"properties\": { " +
					    "        \"ctype\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
					    "        \"iri\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
					    "        \"graph\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
					    "        \"time\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
					    "        \"place\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
					    "        \"term\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
//					    "        \"property\": { " +
//					    "          \"type\": \"keyword\" " +
//					    "        }, " +
					    "        \"text\": { " +
					    "          \"type\": \"text\", " +
					    "          \"fields\": { " +
					    "            \"raw\": { " + 
					    "              \"type\": \"keyword\" " +
					    "            }, " +
					    "            \"english\": { " + 
				        "               \"type\": \"text\", " +
				        "               \"analyzer\": \"english\" " + 
                        "            }, " +
					    "            \"greek\": { " + 
				        "               \"type\": \"text\", " +
				        "               \"analyzer\": \"greek\" " + 
                        "            } " +
				        "          } " +
					    "       } " +
						"    } " +			    
					    "} ", 
					    XContentType.JSON);
				
				client.indices().create(request, RequestOptions.DEFAULT);
			}		

			getRequest = new GetIndexRequest(ec.getIndexVocabularyName()); 
			
			logger.info("Vocabulary index: " + client.indices().exists(getRequest, RequestOptions.DEFAULT));
			
			if (!client.indices().exists(getRequest, RequestOptions.DEFAULT)) {
			
				CreateIndexRequest request = new CreateIndexRequest(ec.getIndexVocabularyName());
				
				  request.mapping("_doc", //??????
						" { " +
					    "    \"properties\": { " +
					    "        \"ctype\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +						
					    "        \"iri\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
					    "        \"graph\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +					    
					    "        \"property\": { " +
					    "          \"type\": \"keyword\" " +
					    "        }, " +
					    "        \"value\": { " +
					    "          \"type\": \"text\", " +
					    "          \"fields\": { " +
					    "            \"raw\": { " + 
					    "              \"type\": \"keyword\" " +
					    "            } " +
					    "          } " +
						"        }, " +			    
					    "        \"language\": { " +
					    "          \"type\": \"keyword\" " +
					    "        } " +
					    "   } " + 
					    "} ", 
					    XContentType.JSON);
				
				client.indices().create(request, RequestOptions.DEFAULT);
			}		

		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
}

