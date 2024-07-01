package ac.software.semantic.config.mongo;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@EnableMongoRepositories(basePackages = "ac.software.semantic.repository.root", mongoTemplateRef = "rootMongoTemplate")
public class RootMongoConfig {

}
