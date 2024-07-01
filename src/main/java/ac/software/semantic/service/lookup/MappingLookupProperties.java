package ac.software.semantic.service.lookup;

import ac.software.semantic.model.constants.type.MappingType;

public class MappingLookupProperties implements GroupLookupProperties {
	
	private MappingType mappingType;
	private Integer group;

	public MappingType getMappingType() {
		return mappingType;
	}

	public void setMappingType(MappingType mappingType) {
		this.mappingType = mappingType;
	}

	@Override
	public Integer getGroup() {
		return group;
	}

	@Override
	public void setGroup(Integer group) {
		this.group = group;
	}
	
}
