package ac.software.semantic.service;

import java.util.List;

import ac.software.semantic.model.AnnotatorDocument;

public interface AnnotationContainerBase {

	public List<String> getOnProperty();
	
	public List<AnnotatorDocument> getAnnotators(); 
}
