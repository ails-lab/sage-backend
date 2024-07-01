package ac.software.semantic.model.base;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.IndexStateOld;
import ac.software.semantic.model.state.MappingExecuteState;

public interface IndexableDocument {

	public List<IndexStateOld> getIndex();

	public void setIndex(List<IndexStateOld> index);
	
//	public IndexState getIndexStateByStructure(ObjectId elasticConfigurationId, ObjectId indexStructureId);

	public IndexStateOld getIndexStateByIndex(ObjectId indexId, ObjectId elasticConfigurationId);
	
//	public IndexState checkIndexStateByStructure(ObjectId elasticConfigurationId, ObjectId indexStructureId);
	
	public IndexStateOld checkIndexStateByIndex(ObjectId indexId);
	
	public List<IndexStateOld> checkIndexStates(ObjectId elasticConfigurationId);
	
	public void removeIndexState(IndexStateOld is);
}
