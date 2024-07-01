package ac.software.semantic.service.lookup;

import ac.software.semantic.model.constants.type.PrototypeType;

public class PrototypeLookupProperties implements LookupProperties {
	
	private PrototypeType prototypeType;
	
	public PrototypeType getPrototypeType() {
		return prototypeType;
	}

	public void setPrototypeType(PrototypeType prototypeType) {
		this.prototypeType = prototypeType;
	}

}
