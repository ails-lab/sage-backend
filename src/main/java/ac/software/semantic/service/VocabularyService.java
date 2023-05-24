package ac.software.semantic.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.payload.VocabularyResponse;
import ac.software.semantic.repository.VocabularyRepository;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;


@Service
public class VocabularyService {

	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer vocc;
	
	public String prefixize(String resource) {
		return vocc.prefixize(resource);
	}
	
	
	public String onPathStringListAsPrettyString(List<String> path) {
		String s = "";
		
//		boolean beforeClass = false;
		for (int i = 0; i < path.size(); ) {
//			if (!beforeClass && i > 0) {
//				s += "/" ;
//			}
			if (i > 0) {
				s += " / " ;
			}
			if (path.get(i).equals(RDFSVocabulary.Class.toString())) {
				String p = path.get(++i);
				String pretty = prefixize(p);
				if (pretty.equals(p)) {
					pretty = "<" + pretty + ">";
				}
				s += pretty;
//				s += "[" + prefixize(path.get(++i)) + "]";
//				beforeClass = true;
			} else {
				String p = path.get(i);
				String pretty = prefixize(p);
				if (pretty.equals(p)) {
					pretty = "<" + pretty + ">";
				}
				s += pretty;
//				s += "<" + prefixize(path.get(i)) + ">";
//				beforeClass = false;
			}
			i++;
		}
	
		return s.trim();
	}
}