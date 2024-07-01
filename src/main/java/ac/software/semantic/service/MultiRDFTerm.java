package ac.software.semantic.service;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MultiRDFTerm implements RDFTermHandler {

	private List<SingleRDFTerm> terms;
	
	public MultiRDFTerm(List<SingleRDFTerm> terms) {
		this.terms = terms;
	}

	public List<SingleRDFTerm> getTerms() {
		return terms;
	}

	public void setTerms(List<SingleRDFTerm> terms) {
		this.terms = terms;
	}
	
	public int hashCode() {
		return Objects.hash(terms);
	}
	
	public boolean equals(Object obj) {
		
		if (obj instanceof MultiRDFTerm) {
			MultiRDFTerm aev = (MultiRDFTerm)obj;
			
			if (terms == null && aev.terms == null) {
				return true;
			}
			
			if (terms == null && aev.terms != null || terms != null && aev.terms == null) {
				return false;
			}
			
			if (terms.size() != aev.terms.size()) {
				return false;
			}
			
			for (int i = 0; i < terms.size(); i++) {
				if (!terms.get(i).equals(aev.terms.get(i))) {
					return false;
				}
			}
			
			return true;
		} else {
			return false;
		}
		
	}
}

