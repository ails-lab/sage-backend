package ac.software.semantic.model.expr;

import org.apache.jena.rdf.model.Resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.d2rml.datatype.DatatypeUtils;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetricResult {

	private Metric metric;
	
//	@JsonIgnore
	private Object value;
	
//	private String pair;
	
	@JsonIgnore
	private Resource datatype;

	public MetricResult(Metric metric, Object value) {
		this.metric = metric;
		this.value = value;
		this.datatype = Vocabulary.resolve(metric.getDatatype());
	}
	
	public Metric getMetric() {
		return metric;
	}

	public void setMetric(Metric metric) {
		this.metric = metric;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	public Object typedValue() {
		return DatatypeUtils.typedValue(value, datatype);
	}

	public String toString() {
		return "MetricResult:"  + metric.getName() + " / " + value + " / " + datatype; 
	}

//	public String getPair() {
//		return pair;
//	}
//
//	public void setPair(String pair) {
//		this.pair = pair;
//	}

	
}
