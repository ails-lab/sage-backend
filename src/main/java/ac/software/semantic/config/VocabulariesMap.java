package ac.software.semantic.config;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VocabulariesMap {
	private Map<String, VocabularyInfo> prefixMap;
	
	private Map<String, Map<String, VocabularyInfo>> datasetMap;
	
	public VocabulariesMap() {
		prefixMap = new HashMap<>(); 
		datasetMap = new HashMap<>();
	}

	public void addMap(String dataset, String prefix, VocabularyInfo vi) {
		prefixMap.put(prefix, vi);
		Map<String, VocabularyInfo> map = datasetMap.get(dataset);
		if (map == null) {
			map = new HashMap<>();
			datasetMap.put(dataset, map);
		}
		map.put(prefix, vi);
	}
	
	public void addMap(String dataset, Map<String, VocabularyInfo> map) {
		prefixMap.putAll(map);
		datasetMap.put(dataset, map);
	}
	
	public void removeMap(String dataset) {
		Map<String, VocabularyInfo> map = datasetMap.get(dataset);
		if (map != null) {
			for (String s : map.keySet()) {
				prefixMap.remove(s);
			}
		}
	}
	
    public VocabularyInfo findPrefix(String uri) {
    	String result = null;
    	
    	for (String prefix : prefixMap.keySet()) {
    		Pattern p = Pattern.compile("^(" + Matcher.quoteReplacement(prefix) + ")");
    		
    		Matcher m = p.matcher(uri);
    		if (m.find()) {
    			String f = m.group(1);
    			if (result == null || f.length() > result.length()) {
    				result = prefix;
    			}
    		}
    	}

    	if (result != null) {
    		return prefixMap.get(result);
    	} else {
    		return null;
    	}
    }
    
    public VocabularyInfo findPrefix(String graph, String uri) {
    	String result = null;
    	
    	Map<String, VocabularyInfo> map = datasetMap.get(graph);
    	if (map == null) {
    		return null;
    	}
    	
    	for (String prefix : map.keySet()) {
    		Pattern p = Pattern.compile("^(" + Matcher.quoteReplacement(prefix) + ")");
    		
    		Matcher m = p.matcher(uri);
    		if (m.find()) {
    			String f = m.group(1);
    			if (result == null || f.length() > result.length()) {
    				result = prefix;
    			}
    		}
    	}

    	if (result != null) {
    		return prefixMap.get(result);
    	} else {
    		return null;
    	}
    }
}
