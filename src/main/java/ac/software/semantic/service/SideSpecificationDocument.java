package ac.software.semantic.service;

import org.apache.jena.rdf.model.Resource;

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.vocs.SEMRVocabulary;

public interface SideSpecificationDocument extends SpecificationDocument {
	
	public String getTripleStoreGraph(SEMRVocabulary resourceVocabulary, boolean separate);
	
	// legacy
	public default String getTOCGraph(SEMRVocabulary resourceVocabulary, boolean separate) {
		if (this instanceof AnnotatorDocument) {
			if (((AnnotatorDocument)this).getAsProperty() != null) {
				return resourceVocabulary.getAnnotationGraphResource().toString();
			}
		}
		
		return getTripleStoreGraph(resourceVocabulary, separate);
	}
	
	public Resource asResource(SEMRVocabulary resourceVocabulary);
}
