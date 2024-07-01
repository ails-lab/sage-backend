package ac.software.semantic.payload.request;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IndexUpdateRequest implements UpdateRequest {

	private String indexStructureId;
//	private String indexStructureIdentifier;
	private String indexEngine;
	
//	@JsonProperty("default")
//	private boolean idefault;
	
//	private List<ClassIndexElement> indexStructures;
//	private List<IndexKeyMetadata> keysMetadata;
//	
	private String name;
	
	@JsonIgnore
	private ElasticConfiguration elasticConfiguration;
	
	public IndexUpdateRequest() {}

	public String getIndexStructureId() {
		return indexStructureId;
	}

	public void setIndexStructureId(String indexStructureId) {
		this.indexStructureId = indexStructureId;
	}

//	public List<ClassIndexElement> getIndexStructures() {
//		return indexStructures;
//	}
//
//	public void setIndexStructures(List<ClassIndexElement> indexStructures) {
//		this.indexStructures = indexStructures;
//	}

//	public String getIndexStructureIdentifier() {
//		return indexStructureIdentifier;
//	}
//
//	public void setIndexStructureIdentifier(String indexStructureIdentifier) {
//		this.indexStructureIdentifier = indexStructureIdentifier;
//	}

//	public List<IndexKeyMetadata> getKeysMetadata() {
//		return keysMetadata;
//	}
//
//	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
//		this.keysMetadata = keysMetadata;
//	}

	public String getIndexEngine() {
		return indexEngine;
	}

	public void setIndexEngine(String indexEngine) {
		this.indexEngine = indexEngine;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ElasticConfiguration getElasticConfiguration() {
		return elasticConfiguration;
	}

	public void setElasticConfiguration(ElasticConfiguration elasticConfiguration) {
		this.elasticConfiguration = elasticConfiguration;
	}

//	public boolean isIdefault() {
//		return idefault;
//	}
//
//	public void setIdefault(boolean idefault) {
//		this.idefault = idefault;
//	}

}
