package ac.software.semantic.model.index;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.d2rml.vocabulary.lucene.TokenAnalyzer;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class IndexKeyMetadata {
	
	private int index;
	
	private String name;
	
//	private RDFTermType defaultTermType;
	
	private boolean languageField = true;
	
	private String datatype;
	
	private List<String> expand; // to be removed -> vexpand
	
	private ExpandIndexKeyMetadata vexpand;
	
	private TokenAnalyzer analyzer;
	
	private Boolean optional;
	
	public IndexKeyMetadata(int index, String name) {
		this.index = index;
		this.name = name;
	}
	
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
	
//	public RDFTermType getDefaultTermType() {
//		return defaultTermType;
//	}
//	
//	public void setDefaultTermType(RDFTermType defaultTermType) {
//		this.defaultTermType = defaultTermType;
//	}

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

//	public List<String> getExpand() {
//		return expand;
//	}
//
//	public void setExpand(List<String> expand) {
//		this.expand = expand;
//	}
	
	public ExpandIndexKeyMetadata getVexpand() {
		return vexpand;
	}

	public void setVexpand(ExpandIndexKeyMetadata expand) {
		this.vexpand = expand;
	}

	public TokenAnalyzer getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(TokenAnalyzer analyzer) {
		this.analyzer = analyzer;
	}
	
	public String toString() {
		return index + " " + name + " " + languageField + " " + datatype;  
				 
	}

	public Boolean getOptional() {
		return optional;
	}

	public void setOptional(Boolean optional) {
		this.optional = optional;
	}
}