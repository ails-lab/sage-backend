package ac.software.semantic.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValueAnnotationReference {
	
	private String iri;

	private int count;
	
	public ValueAnnotationReference(String iri, int count) {
		this.iri = iri;
		this.count = count;
	}

	public String getIri() {
		return iri;
	}

	public void setIri(String iri) {
		this.iri = iri;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}

