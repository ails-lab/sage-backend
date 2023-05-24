package ac.software.semantic.service;

import org.bson.types.ObjectId;

public class MappingObjectIdentifier extends SimpleObjectIdentifier {

	private ObjectId instanceId;
	
	public MappingObjectIdentifier(ObjectId id, ObjectId instanceId) {
		super(id);
		
		this.instanceId =  instanceId;
	}

	public ObjectId getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(ObjectId instanceId) {
		this.instanceId = instanceId;
	}

	@Override
	public String toHexString() {
		return id.toHexString() + (instanceId != null ? ":" + instanceId.toHexString() : "");
	}
}
