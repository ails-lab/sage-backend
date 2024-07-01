package ac.software.semantic.model.index;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.DatasetContext;
import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.VocabularyContainer;

public class ExpandIndexKeyMetadata {

	private List<ExpandType> mode;
	private List<ObjectId> datasetId;
	
	@Transient
	@JsonIgnore
	private VocabularyContainer<ResourceContext> vocabularContainer;
	
	public ExpandIndexKeyMetadata() {
		
	}

	public List<ExpandType> getMode() {
		return mode;
	}

	public void setMode(List<ExpandType> mode) {
		this.mode = mode;
	}

	public List<ObjectId> getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(List<ObjectId> datasetId) {
		this.datasetId = datasetId;
	}

	public VocabularyContainer<ResourceContext> getVocabularContainer() {
		return vocabularContainer;
	}

	public void setVocabularContainer(VocabularyContainer<ResourceContext> vocabularContainer) {
		this.vocabularContainer = vocabularContainer;
	}
}
