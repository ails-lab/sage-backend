package ac.software.semantic.model;

import org.bson.types.ObjectId;

public interface DataDocument {

	public ObjectId getId();
	
	public String getUuid();

}
