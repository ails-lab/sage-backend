package ac.software.semantic.service;

import java.util.List;

import org.apache.jena.rdf.model.RDFNode;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValueCount {
	private RDFNode value;
	
	private List<RDFNode> values;
	private List<String> names;
	
	private int count;
	
	private Object controlGraph;
	
	public ValueCount(RDFNode value, int count) {
		this.value = value;
		this.count = count;
	}
	
	public ValueCount(List<String> names, List<RDFNode> values, int count) {
		this.names = names;
		this.values = values;
		this.count = count;
	}
	
	public RDFNode getValue() {
		return value;
	}
	
	public int getCount() {
		return count;
	}

	public Object getControlGraph() {
		return controlGraph;
	}

	public void setControlGraph(Object controlGraph) {
		this.controlGraph = controlGraph;
	}

	public List<RDFNode> getValues() {
		return values;
	}

	public void setValues(List<RDFNode> values) {
		this.values = values;
	}

	public List<String> getNames() {
		return names;
	}

	public void setNames(List<String> names) {
		this.names = names;
	}
	
	public String toString() {
		if (value != null) {
			return value.toString();
		} else if (names != null) {
			return "// " + names.toString() + " / " + values.toString() + "//";  
		} else {
			return "";
		}
	}
	
}