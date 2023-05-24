package ac.software.semantic.model.index;

import ac.software.semantic.model.constants.RDFTermType;

public class IndexKeyMetadata {
	private int index;
	private String name;
	private RDFTermType defaultTermType;
	private boolean languageField = true;
	private String datatype;
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public RDFTermType getDefaultTermType() {
		return defaultTermType;
	}
	
	public void setDefaultTermType(RDFTermType defaultTermType) {
		this.defaultTermType = defaultTermType;
	}

	public boolean isLanguageField() {
		return languageField;
	}

	public void setLanguageField(boolean languageField) {
		this.languageField = languageField;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	
	public String keyName() {
		if (getName() != null) {
			return getName(); 
		} else {
			return "r" + getIndex();
		}
	}
}