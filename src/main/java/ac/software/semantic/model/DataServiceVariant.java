package ac.software.semantic.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class DataServiceVariant {

	private String name;
	
	@JsonIgnore
	private String d2rml;
	
	private DataServiceRank rank;
	
	public DataServiceVariant(String name, String d2rml) {
		this.name = name;
		this.d2rml = d2rml;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getD2rml() {
		return d2rml;
	}

	public void setD2rml(String d2rml) {
		this.d2rml = d2rml;
	}

	public DataServiceRank getRank() {
		return rank;
	}

	public void setRank(DataServiceRank rank) {
		this.rank = rank;
	}

}
