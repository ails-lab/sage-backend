package ac.software.semantic.model.index;

import java.util.List;

import org.apache.jena.rdf.model.RDFNode;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.RDFTermType;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class IndexElementSelector {
	
	private int index;
	private RDFTermType termType;
	private List<String> languages;
	private String target;
	
	private List<String> values;
	
	public IndexElementSelector() { }

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public List<String> getLanguages() {
		return languages;
	}

	public void setLanguages(List<String> languages) {
		this.languages = languages;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public RDFTermType getTermType() {
		return termType;
	}

	public void setTermType(RDFTermType termType) {
		this.termType = termType;
	}

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}
}

