package ac.software.semantic.service;

import org.apache.jena.rdf.model.RDFNode;

public class ValueCount {
	private RDFNode value;
	private int count;
	
	public ValueCount(RDFNode value, int count) {
		this.value = value;
		this.count = count;
	}
	
	public RDFNode getValue() {
		return value;
	}
	
	public int getCount() {
		return count;
	}
	
}