package ac.software.semantic.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.types.ObjectId;

import ac.software.semantic.payload.PrefixizedUri;
import edu.ntua.isci.ac.common.utils.SimpleTrie;

public class VocabularyContainer<T extends ResourceContext> {

	private Map<ObjectId, T> vocsById;
	private Map<String, T> vocsByName;
//	private Map<String, ResourceContext> vocsByPrefix;
//	private Map<String, ResourceContext> vocsByNamespace;
	
	private SimpleTrie<T> namespaceMap;
	
	public VocabularyContainer() {
		vocsById = new HashMap<>();
		vocsByName = new TreeMap<>();
//		vocsByPrefix = new TreeMap<>();
//		vocsByNamespace = new TreeMap<>();
		
		namespaceMap = new SimpleTrie<>();
	}

	public Map<String, T> getVocsByName() {
		return vocsByName;
	}
	
	public void clear() {
		vocsById = new HashMap<>();
		vocsByName = new TreeMap<>();
//		vocsByPrefix = new TreeMap<>();
//		vocsByNamespace = new TreeMap<>();
		
		namespaceMap = new SimpleTrie<>();
	}

//	public Map<String, ResourceContext> getVocsByPrefix() {
//		return vocsByPrefix;
//	}
	
//	public Map<String, ResourceContext> getVocsByNamespace() {
//		return vocsByNamespace;
//	}

	public void add(T voc) {
		vocsById.put(voc.getId(), voc);
		vocsByName.put(voc.getName(), voc);
		if (voc.getUriDescriptors() != null) {
			for (VocabularyEntityDescriptor ved : voc.getUriDescriptors()) {
//				vocsByPrefix.put(ved.getPrefix(), voc);
//				vocsByNamespace.put(ved.getNamespace(), voc);
				
//				System.out.println("ADDING " + ved.getNamespace());
				namespaceMap.put(ved.getNamespace(), voc);
			}
		}
	}
	
	
	public String prefixize(String url) {  
		Map.Entry<String, List<T>> entry = namespaceMap.getLongestPrefix(url);
		if (entry != null) {
			//return entry.getValue().getPrefix() + ":" + url.substring(entry.getKey().length());
		
			List<T> voc = entry.getValue();
			String namespace = entry.getKey();
			String prefix = voc.get(0).getPrefixMap().get(namespace); // get first !!! how to fix this?
			
			if (prefix != null) {
				return prefix + ":" + url.substring(namespace.length());
			}
		}
		
		return url;
	}	
	
	public List<T> resolve(String url) {  
		Map.Entry<String, List<T>> entry = namespaceMap.getLongestPrefix(url);
		if (entry != null) {
			return entry.getValue();
		}
		
		return null;
	}
	
	public PrefixizedUri arrayPrefixize(String url) { 
		if (url == null) {
			return null;
		}
		PrefixizedUri pu = new PrefixizedUri(url);

		Map.Entry<String, List<T>> entry = namespaceMap.getLongestPrefix(url);
		if (entry != null) {
			List<T> voc = entry.getValue();
			
			String namespace = entry.getKey();
			pu.setNamespace(namespace);
			pu.setPrefix(voc.get(0).getPrefixMap().get(namespace)); // get first !!! how to fix this?
			
			pu.setLocalName(url.substring(entry.getKey().length())); 
		}
		
		return pu;
		
	}

	public Map<ObjectId, T> getVocsById() {
		return vocsById;
	}

}
