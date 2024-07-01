package ac.software.semantic.service.lookup;

import java.util.Set;

import org.bson.types.ObjectId;

public class IndexLookupProperties implements LookupProperties {
	
	private Set<ObjectId> elasticConfigurationId;
	
	public IndexLookupProperties() {
		
	}

	public Set<ObjectId> getElasticConfigurationId() {
		return elasticConfigurationId;
	}

	public void setElasticConfigurationId(Set<ObjectId> elasticConfigurationId) {
		this.elasticConfigurationId = elasticConfigurationId;
	}
	
}
