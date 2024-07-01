package ac.software.semantic.model.base;

import org.bson.types.ObjectId;

public interface DatasetContained extends SpecificationDocument {

	public ObjectId getId();
	
	public ObjectId getDatasetId();
	
	public String getDatasetUuid();
	
	
}
