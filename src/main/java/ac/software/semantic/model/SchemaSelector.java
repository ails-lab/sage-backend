package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaSelector {
	
	private String name;
	private ClassIndexElement element;
	   
	private List<IndexKeyMetadata> keysMetadata;
	
	private DataServiceRank clusterMultiplicity;
	private Boolean clusterKey;
	
	public SchemaSelector() {
		
	}
	
	public ClassIndexElement getElement() {
		return element;
	}

	public void setElement(ClassIndexElement element) {
		this.element = element;
	}

	public List<IndexKeyMetadata> getKeysMetadata() {
		return keysMetadata;
	}

	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
		this.keysMetadata = keysMetadata;
	}
	
	public Map<Integer, List<String>> indexToPropertiesMap() {
		Map<Integer, List<String>> map = new HashMap<>();
		
		element.indexToPropertiesMap(map);
		
		return map;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public List<String> getKeys() {
		List<String> keys = new ArrayList<>();
		for (IndexKeyMetadata ikm : keysMetadata) {
			keys.add(ikm.getName());
		}

		return keys;
	}

	public DataServiceRank getClusterMultiplicity() {
		return clusterMultiplicity;
	}

	public void setClusterMultiplicity(DataServiceRank clusterMultiplicity) {
		this.clusterMultiplicity = clusterMultiplicity;
	}

	public Boolean getClusterKey() {
		return clusterKey;
	}

	public void setClusterKey(Boolean clusterKey) {
		this.clusterKey = clusterKey;
	}
	
	public boolean isClusterKey() {
		return clusterKey != null && clusterKey;
	}

}
