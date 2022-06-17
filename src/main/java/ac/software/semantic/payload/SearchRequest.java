package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.List;

public class SearchRequest {

	private String time;
	private String endTime;
	
	private String place;
	
	private List<String> terms;
	
	private List<String> collections;
	
	SearchRequest() { 
		collections = new ArrayList<>();
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public List<String> getCollections() {
		return collections;
	}

	public void setCollections(List<String> collections) {
		this.collections = collections;
	}

	public String getPlace() {
		return place;
	}

	public void setPlace(String place) {
		this.place = place;
	}

	public List<String> getTerms() {
		return terms;
	}

	public void setTerms(List<String> terms) {
		this.terms = terms;
	}
	
	
}
