package ac.software.semantic.service;

import java.sql.SQLException;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.repository.ElasticConfigurationRepository;
import ac.software.semantic.repository.VirtuosoConfigurationRepository;

@Service
public class DatabaseConfigurationService {

//	@Autowired
//	private VirtuosoConfigurationRepository virtuosoRepository;
//
//	@Autowired
//	private ElasticConfigurationRepository elasticRepository;
//
//	public boolean createVirtuosoConfiguration(ObjectId databaseId, String name, String sparqlEndpoint, String isqlLocation) throws SQLException {
//		
//		Optional<VirtuosoConfiguration> db = virtuosoRepository.findByName(name);
//		if (db.isPresent()) {
//			return false;
//		}
//
//		VirtuosoConfiguration vc = new VirtuosoConfiguration();
//		vc.setDatabaseId(databaseId);
//		vc.setName(name);
//		vc.setSparqlEndpoint(sparqlEndpoint);
//		vc.setIsqlLocation(isqlLocation);
//		
//		virtuosoRepository.save(vc);
//		
//		return true;
//	}
//	
//	public boolean createElasticConfiguration(ObjectId databaseId, String name, String indexIp, String indexDataName, String indexVocabularyName) throws SQLException {
//		
//		Optional<ElasticConfiguration> db = elasticRepository.findByName(name);
//		if (db.isPresent()) {
//			return false;
//		}
//
//		ElasticConfiguration vc = new ElasticConfiguration();
//		vc.setDatabaseId(databaseId);
//		vc.setName(name);
//		vc.setIndexIp(indexIp);
//		vc.setIndexDataName(indexDataName);
//		vc.setIndexVocabularyName(indexVocabularyName);
//		
//		elasticRepository.save(vc);
//		
//		return true;
//	}


}
