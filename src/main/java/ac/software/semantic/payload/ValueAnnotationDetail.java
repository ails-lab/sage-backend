package ac.software.semantic.payload;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.AnnotationEditType;

public class ValueAnnotationDetail {

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String id;

	private String value;
	private String value2;
	private AnnotationEditType state;

	private int start;
	private int end;

	private int othersRejected;
	private int othersAccepted;
	
	private int count;
	
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

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
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



}
