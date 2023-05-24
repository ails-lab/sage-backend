package ac.software.semantic.service;

import org.bson.types.ObjectId;

public class SimpleObjectIdentifier implements ObjectIdentifier {
	
	protected ObjectId id;
	
	public SimpleObjectIdentifier(ObjectId id) {
		this.id = id;
	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	@Override
	public String toHexString() {
		return id.toHexString();
	}

}
