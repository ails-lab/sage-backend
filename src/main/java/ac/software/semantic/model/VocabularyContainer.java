package ac.software.semantic.model;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import ac.software.semantic.payload.PrefixizedUri;
import edu.ntua.isci.ac.common.utils.SimpleTrie;

public class VocabularyContainer {

	private Map<String, Vocabulary> vocsByName;
	private Map<String, Vocabulary> vocsByPrefix;
	private Map<String, Vocabulary> vocsByNamespace;
	
	private SimpleTrie<Vocabulary> namespaceMap;
	
	public VocabularyContainer() {
		vocsByName = new TreeMap<>();
		vocsByPrefix = new TreeMap<>();
		vocsByNamespace = new TreeMap<>();
		
		namespaceMap = new SimpleTrie<>();
	}

	public Map<String, Vocabulary> getVocsByName() {
		return vocsByName;
	}

//	public void setVocsByName(Map<String, Vocabulary> vocsByName) {
//		this.vocsByName = vocsByName;
//	}

	public Map<String, Vocabulary> getVocsByPrefix() {
		return vocsByPrefix;
	}
	
	public Map<String, Vocabulary> getVocsByNamespace() {
		return vocsByNamespace;
	}

//	public void setVocsByPrefix(Map<String, Vocabulary> vocsByPrefix) {
//		this.vocsByPrefix = vocsByPrefix;
//	}
	
	public void add(Vocabulary voc) {
		vocsByName.put(voc.getName(), voc);
		vocsByPrefix.put(voc.getPrefix(), voc);
		vocsByNamespace.put(voc.getNamespace(), voc);
		
		namespaceMap.put(voc.getNamespace(), voc);
	}
	
	
	public String prefixize(String url) {  
		Map.Entry<String, Vocabulary> entry = namespaceMap.getLongestPrefix(url);
		if (entry != null) {
			return entry.getValue().getPrefix() + ":" + url.substring(entry.getKey().length()); 
		}
		
		return url;
	}	
	
	public PrefixizedUri arrayPrefixize(String url) { 
		if (url == null) {
			return null;
		}
		PrefixizedUri pu = new PrefixizedUri(url);

		Map.Entry<String, Vocabulary> entry = namespaceMap.getLongestPrefix(url);
		if (entry != null) {
			Vocabulary voc = entry.getValue();
			pu.setNamespace(voc.getNamespace());
			pu.setPrefix(voc.getPrefix());
			pu.setLocalName(url.substring(entry.getKey().length())); 
		}
		
		return pu;
		
	}
}
