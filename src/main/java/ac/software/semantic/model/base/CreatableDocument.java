package ac.software.semantic.model.base;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;

public interface CreatableDocument<C extends CreateState> {

	public List<C> getCreate();

	public void setCreate(List<C> create);
	
	public C getCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId);
	
	public C checkCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId);
	
	public void deleteCreateState(ObjectId elasticConfigurationId, ObjectId fileSystemConfigurationId);
	
	public default ObjectId getElasticConfigurationId() {
		return null;
	}

}
