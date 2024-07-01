package ac.software.semantic.model.base;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;

import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.vocs.SEMRVocabulary;

public abstract class PublishableDocument<E extends ExecuteState, P extends PublishState<E>> {

	private List<P> publish;
	
	public PublishableDocument() {
//	   publish = new ArrayList<>();
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
//			s = (P)getTypeParameterClass().newInstance();
	        Type type = getClass().getGenericSuperclass();
	        ParameterizedType paramType = (ParameterizedType)type;
	        
	        Class<P> e = null;
	        if (paramType.getActualTypeArguments()[1] instanceof Class) {
		        e = (Class<P>) paramType.getActualTypeArguments()[1];
	        } else {
	        	e = (Class<P>) ((ParameterizedType)(paramType.getActualTypeArguments()[1])).getRawType();
	        }

			s = (P)e.newInstance();
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
	
	public synchronized void removePublishState(P ps) {
		if (publish != null) {
			publish.remove(ps);
			
			if (publish.size() == 0) {
				publish = null;
			}
		} 
	}

	public ProcessStateContainer<P> getCurrentPublishState(Collection<TripleStoreConfiguration> virtuosoConfigurations) {
		for (TripleStoreConfiguration vc : virtuosoConfigurations) {
			P ps = checkPublishState(vc.getId());
			if (ps != null) {
				return new ProcessStateContainer<P>(ps, vc);
			}
		}
		
		return null;
	}

}
