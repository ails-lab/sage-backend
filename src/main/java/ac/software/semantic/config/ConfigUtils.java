package ac.software.semantic.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.repository.root.VocabularyRepository;
import net.sf.ehcache.Cache;

@Service
public class ConfigUtils {

	private final static Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

	@Autowired
	private VocabularyRepository vocRepository;
	
	@Autowired
    @Qualifier("database")
    private Database database;
	
	@Lazy
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer<Vocabulary> vocc;
	
	@Autowired
	@Qualifier("labels-cache")
	private Cache labelsCache;
	
	public VocabularyContainer<Vocabulary> loadRDFVocabularies(Database database) {
		logger.info("Loading vocabularies");

		VocabularyContainer<Vocabulary> vocc = new VocabularyContainer<>();
		
		for (Vocabulary voc : vocRepository.findByDatabaseId(database.getId())) {
			if (voc.getSpecification() != null) {
				logger.info("Loading " + voc.getSpecification());
			}
			voc.load();
			vocc.add(voc);
		}
		
		return vocc;
	}
	
	public void reloadRDFVocabularies() {
		logger.info("Reloading vocabularies");

		vocc.clear();
		
		for (Vocabulary voc : vocRepository.findByDatabaseId(database.getId())) {
			if (voc.getSpecification() != null) {
				logger.info("Loading " + voc.getSpecification());
			}
			voc.load();
			vocc.add(voc);
		}
		
		labelsCache.removeAll();
	}
}
