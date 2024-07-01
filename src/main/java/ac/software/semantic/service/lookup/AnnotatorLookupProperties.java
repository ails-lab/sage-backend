package ac.software.semantic.service.lookup;

public class AnnotatorLookupProperties implements GroupLookupProperties {
	
	private Integer group;

	@Override
	public Integer getGroup() {
		return group;
	}

	@Override
	public void setGroup(Integer group) {
		this.group = group;
	}
	

}
