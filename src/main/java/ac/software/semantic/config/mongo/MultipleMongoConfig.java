package ac.software.semantic.config.mongo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.mongodb.MongoClient;

@Configuration
public class MultipleMongoConfig {

	 @Value("${spring.data.mongodb.host}")
	 private String coreHost;

	 @Value("${spring.data.mongodb.port}")
	 private int corePort;
	 
	 @Value("${spring.data.mongodb.database}")
	 private String coreDatabase;

	 @Value("${mongodb.root.host}")
	 private String rootHost;

	 @Value("${mongodb.root.port}")
	 private int rootPort;
	 
	 @Value("${mongodb.root.database}")
	 private String rootDatabase;

//  @Primary
    @Bean(name = "coreMongoTemplate")
    public MongoTemplate coreMongoTemplate() throws Exception {
    	return new MongoTemplate(coreFactory(coreHost, corePort, coreDatabase));
    }

    @Bean(name = "rootMongoTemplate")
    public MongoTemplate rootMongoTemplate() throws Exception {
    	if (rootHost == null) {
    		return new MongoTemplate(rootFactory(coreHost, corePort, coreDatabase));
    	} else {
    		return new MongoTemplate(rootFactory(rootHost, rootPort, rootDatabase));
    	}
    }

    @Bean
//	@Primary
	public MongoDbFactory coreFactory(String host, int port, String database) throws Exception {
	    return new SimpleMongoDbFactory(new MongoClient(host, port), database);
	}
	
	@Bean
	public MongoDbFactory rootFactory(String host, int port, String database) throws Exception {
	    return new SimpleMongoDbFactory(new MongoClient(host, port), database);
	}

}