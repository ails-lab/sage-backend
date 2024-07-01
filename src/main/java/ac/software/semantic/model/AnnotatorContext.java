package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnnotatorContext {

	private String name;
	
	@JsonIgnore
	private ObjectId id;
	
	@Transient
	@JsonIgnore
	private VocabularyContainer<ResourceContext> vocabularyContainer;
	
	public AnnotatorContext() {
		
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public VocabularyContainer<ResourceContext> getVocabularyContainer() {
		return vocabularyContainer;
	}

	public void setVocabularyContainer(VocabularyContainer<ResourceContext> vocabularyContainer) {
		this.vocabularyContainer = vocabularyContainer;
	}
	
	
	
}
