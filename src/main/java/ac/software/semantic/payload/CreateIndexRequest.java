package ac.software.semantic.payload;

import java.util.List;

import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;

public class CreateIndexRequest {

	private String indexId;
	private String indexIdentifier;
	private String indexEngine;
	
	private List<ClassIndexElement> indexStructures;
	private List<IndexKeyMetadata> keysMetadata;
	
	public CreateIndexRequest() {}

	public String getIndexId() {
		return indexId;
	}

	public void setIndexId(String indexId) {
		this.indexId = indexId;
	}

	public List<ClassIndexElement> getIndexStructures() {
		return indexStructures;
	}

	public void setIndexStructures(List<ClassIndexElement> indexStructures) {
		this.indexStructures = indexStructures;
	}

	public String getIndexIdentifier() {
		return indexIdentifier;
	}

	public void setIndexIdentifier(String indexIdentifier) {
		this.indexIdentifier = indexIdentifier;
	}

	public List<IndexKeyMetadata> getKeysMetadata() {
		return keysMetadata;
	}

	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
		this.keysMetadata = keysMetadata;
	}

	public String getIndexEngine() {
		return indexEngine;
	}

	public void setIndexEngine(String indexEngine) {
		this.indexEngine = indexEngine;
	}

}
