package ac.software.semantic.model;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;

public abstract class PublishDocument<P extends PublishState> {

	private List<P> publish;
	
	public PublishDocument() {
	   publish = new ArrayList<>();
	}
	
	public List<P> getPublish() {
		return publish;
	}

	public void setPublish(List<P> publish) {
		this.publish = publish;
	}

	public P getPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {
			for (P s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		} else {
			publish = new ArrayList<>();
		}

		
//		P s = new P();
		P s = null;
		try {
			s = (P)getTypeParameterClass().newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		s.setPublishState(DatasetState.UNPUBLISHED);
		s.setDatabaseConfigurationId(databaseConfigurationId);
		publish.add(s);
		
		return s;
	}
	
	public P checkPublishState(ObjectId databaseConfigurationId) {
		if (publish != null) {
			for (P s : publish) {
				if (s.getDatabaseConfigurationId().equals(databaseConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}
	
	public synchronized void removePublishState(PublishState ps) {
		if (publish != null) {
			publish.remove(ps);
		} 
	}

	public ProcessStateContainer getCurrentPublishState(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) {
			PublishState ps = checkPublishState(vc.getId());
			if (ps != null) {
				return new ProcessStateContainer(ps, vc);
			}
		}
		
		return null;
	}
	
    private Class<P> getTypeParameterClass() {
        Type type = getClass().getGenericSuperclass();
        ParameterizedType paramType = (ParameterizedType) type;
        return (Class<P>) paramType.getActualTypeArguments()[0];
    }
	
}
