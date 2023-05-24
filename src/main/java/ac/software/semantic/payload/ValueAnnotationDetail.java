package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.AnnotationEditType;
import ac.software.semantic.model.constants.ValidationType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValueAnnotationDetail {
	
	private String id;

	private String value;
	private String value2;
	private AnnotationEditType state;

	private Integer start;
	private Integer end;
	private Double score;
	
	private int othersRejected;
	private int othersAccepted;
	
	private int count;
	
	private ValidationType validation;
	
	private List<ValueAnnotationReference> references;
	
	private Set<String> defaultTargets;
	
	private String selectedTarget;
	private String othersTarget;
	
	private boolean manual;
	
	public ValueAnnotationDetail() {
	}
	

//	public ValueAnnotationDetail(String value, String value2, int start, int end) {
//		this.value = value;
//		this.value2 = value2;
//		this.start = start;
//		this.end = end;
//	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public AnnotationEditType getState() {
		return state;
	}

	public void setState(AnnotationEditType state) {
		this.state = state;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getStart() {
		return start;
	}

	public void setStart(Integer start) {
		this.start = start;
	}

	public Integer getEnd() {
		return end;
	}

	public void setEnd(Integer end) {
		this.end = end;
	}

	public String getValue2() {
		return value2;
	}

	public void setValue2(String value2) {
		this.value2 = value2;
	}


	public int getOthersRejected() {
		return othersRejected;
	}


	public void setOthersRejected(int othersRejected) {
		this.othersRejected = othersRejected;
	}


	public int getOthersAccepted() {
		return othersAccepted;
	}


	public void setOthersAccepted(int othersAccepted) {
		this.othersAccepted = othersAccepted;
	}


	public int getCount() {
		return count;
	}


	public void setCount(int count) {
		this.count = count;
	}
	
	public void addCount(int count) {
		this.count += count;
	}


	public Double getScore() {
		return score;
	}


	public void setScore(Double score) {
		this.score = score;
	}


	public ValidationType getValidation() {
		return validation;
	}


	public void setValidation(ValidationType validation) {
		this.validation = validation;
	}

	public void addReference(ValueAnnotationReference ref) {
		if (references == null) {
			references = new ArrayList<>();
		}
		references.add(ref);
	}
	
	public List<ValueAnnotationReference> getReferences() {
		return references;
	}

	public void setReferences(List<ValueAnnotationReference> references) {
		this.references = references;
	}
	
	public void addDefaultTarget(String defaultTarget) {
		if (defaultTargets == null) {
			defaultTargets = new HashSet<>();
		}
		defaultTargets.add(defaultTarget);
	}
	
	public Set<String> getDefaultTargets() {
		return defaultTargets;
	}


	public String getSelectedTarget() {
		return selectedTarget;
	}


	public void setSelectedTarget(String selectedTarget) {
		this.selectedTarget = selectedTarget;
	}


	public String getOthersTarget() {
		return othersTarget;
	}


	public void setOthersTarget(String othersTarget) {
		this.othersTarget = othersTarget;
	}


	public boolean isManual() {
		return manual;
	}


	public void setManual(boolean manual) {
		this.manual = manual;
	}

}
