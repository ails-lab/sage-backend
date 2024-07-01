package ac.software.semantic.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.expr.ComputationResult;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComputationResultWrapper {
	private String iriA;
	private String iriB;
	
	private ComputationResult computation;
	
	public ComputationResultWrapper(String iriA, String iriB) {
		this.iriA = iriA;
		this.iriB = iriB;
	}

	public String getIriA() {
		return iriA;
	}

	public void setIriA(String iriA) {
		this.iriA = iriA;
	}

	public String getIriB() {
		return iriB;
	}

	public void setIriB(String iriB) {
		this.iriB = iriB;
	}

	public ComputationResult getComputation() {
		return computation;
	}

	public void setComputation(ComputationResult computation) {
		this.computation = computation;
	}
	
	
}
