package ac.software.semantic.model.base;

import org.bson.types.ObjectId;

import ac.software.semantic.model.DataDocument;

public interface SpecificationDocument extends DataDocument {

	public ObjectId getUserId();
}
