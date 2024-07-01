package ac.software.semantic.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.bson.types.ObjectId;

import ac.software.semantic.model.ConfigurationObject;
import ac.software.semantic.model.TripleStoreConfiguration;

public class ConfigurationContainer<T extends ConfigurationObject> {

	private Map<String,T> nameMap;
	private Map<ObjectId,T> idMap;
	private Map<Integer,T> orderMap;
	
	public ConfigurationContainer() {
		nameMap = new TreeMap<>();
		idMap = new HashMap<>();
		orderMap = new HashMap<>();
	}

	public Map<String,T> getNameMap() {
		return nameMap;
	}

	public Map<ObjectId,T> getIdMap() {
		return idMap;
	}
	
	public Map<Integer,T> getOrderMap() {
		return orderMap;
	}

	public void add(T tsc) {
		nameMap.put(tsc.getName(), tsc);
		idMap.put(tsc.getId(), tsc);
		orderMap.put(tsc.getOrder(), tsc);
	}
	
	public Collection<T> values() {
		return nameMap.values();
	}
	
	public Collection<String> names() {
		return nameMap.keySet();
	}

	public Collection<ObjectId> ids() {
		return idMap.keySet();
	}
	
	public T getByName(String name) { 
		return nameMap.get(name);
	}
	
	public T getById(ObjectId id) { 
		return idMap.get(id);
	}
	
	public T getByOrder(int order) { 
		return orderMap.get(order);
	}
	
	public boolean isEmpty() {
		return idMap.isEmpty();
	}

}
