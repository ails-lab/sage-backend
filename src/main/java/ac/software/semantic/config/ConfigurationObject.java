package ac.software.semantic.config;

import org.bson.types.ObjectId;

public interface ConfigurationObject {

	public ObjectId getId();
	
	public String getName();
	
	public int getOrder();
}
