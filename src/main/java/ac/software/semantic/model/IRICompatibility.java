package ac.software.semantic.model;

import ac.software.semantic.model.constants.PathElementType;

public class IRICompatibility {
	private PathElementType type;
	private String legacy;
	private String current;
	
	public IRICompatibility() { }
	
	public PathElementType getType() {
		return type;
	}

	public void setType(PathElementType type) {
		this.type = type;
	}

	public String getLegacy() {
		return legacy;
	}

	public void setLegacy(String legacy) {
		this.legacy = legacy;
	}	
	
	public String getCurrent() {
		return current;
	}

	public void setCurrent(String current) {
		this.current = current;
	}		
}