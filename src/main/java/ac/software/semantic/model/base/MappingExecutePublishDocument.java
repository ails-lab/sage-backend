package ac.software.semantic.model.base;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.PublishState;

public abstract class MappingExecutePublishDocument<E extends ExecuteState, P extends PublishState<E>> extends PublishableDocument<E, P> implements ExecutableDocument<E> {

	private List<E> execute;
	
	public MappingExecutePublishDocument() {
	   super();
//	   execute = new ArrayList<>();
	}
	
	@Override
	public List<E> getExecute() {
		return execute;
	}

	@Override
	public void setExecute(List<E> execute) {
		this.execute = execute;
	}	
	
	@Override
	public E getExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {
			for (E s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			execute = new ArrayList<>();
		}
		
//		E s = new E();
		E s = null;
		try {
	        Type type = getClass().getGenericSuperclass();
	        ParameterizedType paramType = (ParameterizedType)type;
	        Class<E> e = (Class<E>) paramType.getActualTypeArguments()[0];

			s = (E)e.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		s.setExecuteState(MappingState.NOT_EXECUTED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		execute.add(s);
		
		return s;
	}
	
	@Override
	public E checkExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {		
			for (E s : execute) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}		

	@Override
	public void deleteExecuteState(ObjectId databaseConfigurationId) {
		if (execute != null) {
			for (int i = 0; i < execute.size(); i++) {
				if (execute.get(i).getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					execute.remove(i);
					break;
				}
			}
			
			if (execute.size() == 0) {
				execute = null;
			}
		} 
	}

}
