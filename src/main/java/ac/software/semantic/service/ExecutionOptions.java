package ac.software.semantic.service;

import java.util.Map;

import ac.software.semantic.model.DataServiceRank;
import ac.software.semantic.model.index.IndexKeyMetadata;

public class ExecutionOptions {

	private Map<String, Object> params;
	
	private Map<String, IndexKeyMetadata> targets;

	public ExecutionOptions(Map<String, Object> params, Map<String, IndexKeyMetadata> targets) {
		this.params = params;
		this.targets = targets;
	}
	
	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}

	public Map<String, IndexKeyMetadata> getTargets() {
		return targets;
	}

	public void setTargets(Map<String, IndexKeyMetadata> targets) {
		this.targets = targets;
	}
	
	public DataServiceRank getRank() {
		if (targets == null || targets.size() == 1 && targets.containsKey("r0") && targets.get("r0").getName().equals("")) {
			return DataServiceRank.SINGLE;
		} else {
			return DataServiceRank.MULTIPLE;
		}
		
	}
}
