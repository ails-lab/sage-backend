package ac.software.semantic.model;

import java.util.List;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;

import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.service.SideSpecificationDocument;
import ac.software.semantic.vocs.SEMRVocabulary;

public interface AnnotationValidation extends SideSpecificationDocument {

	public String getAsProperty();
	
	public String getTripleStoreGraph(SEMRVocabulary resourceVocabulary, boolean separate);
	
	public MappingExecuteState getExecuteState(ObjectId fileSystemConfigurationId);
	
	public ObjectId getAnnotationEditGroupId();
	
	public List<String> getOnProperty();

	@Override
	public default Resource asResource(SEMRVocabulary resourceVocabulary) {
		return resourceVocabulary.getAnnotationValidatorAsResource(this);
	}

}
