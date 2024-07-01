package ac.software.semantic.model.expr;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.expr.Logic.Variable;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComputationResult {

	@JsonIgnore
	private Map<String, DimensionResult> dimensionResultsA;
	
	@JsonIgnore
	private Map<String, DimensionResult> dimensionResultsB;
	
	@JsonIgnore
	List<Map<String, Object>> flatDimensionResultsA;

	@JsonIgnore
	List<Map<String, Object>> flatDimensionResultsB;

	@JsonIgnore
	public Map<String,Metric> metricsMap;
	
	@JsonIgnore
	private Map<String, MetricResult> metricResults;
	
	private List<MetricResult> metrics;
	
	private List<LogicResult> logic;
	
	private Double result;

	public ComputationResult() {
		metricsMap = new HashMap<>();
		metricResults = new LinkedHashMap<>();
		metrics = new ArrayList<>();
	}
	
//	public List<MetricResult> getMetrics() {
//		return metrics;
//	}
//
	public void setMetricsMap(List<Metric> metrics) {
		for (Metric m : metrics) {
			metricsMap.put(m.getName(), m);
		}
	}
	
	public MetricResult evaluateMetric(Variable variable, Metric metric, List<Object> vListA, List<Object> vListB, PrintStream out) throws Exception {
//		System.out.println(metricsMap.get(metric));
//		System.out.println(" > > " + orderA + " " + flatDimensionResultsA);
//		System.out.println(" > > " + orderB + " " + flatDimensionResultsB);
		
//		System.out.println(metricsMap.get(metric));
//		Map<String, Object> s1 = flatDimensionResultsA.get(orderA);
//		Map<String, Object> s2 = flatDimensionResultsB.get(orderB);
		
		
		MetricResult mr = metric.evaluate(variable.defaultValue, vListA, vListB, out);
		metricResults.put(vListA + "/" + vListB + "//" + variable.storeExpr, mr);
		
//		System.out.println("STORE " + vA + "/" + vB + "//" + variable.storeExpr + " >> " + mr.getValue());
//		if (mr.getValue() != null) {
		if (mr.getValue() instanceof Integer && ((int)mr.getValue()) < 0) {
			
		} else {
			metrics.add(mr);
//			if (orderA != 0 || orderB != 0) {
//				mr.setPair("A:" + orderA + " - B:"+ orderB);
//			}
		}
		return mr;
	}

	public Double getResult() {
		return result;
	}

	public void setResult(Double result) {
		this.result = result;
	}

	public Map<String, DimensionResult> getDimensionsA() {
		return dimensionResultsA;
	}

	public Map<String, DimensionResult> getDimensionsB() {
		return dimensionResultsB;
	}
	
	public List<Map<String, Object>> getFlatDimensionsA() {
		return flatDimensionResultsA;
	}

	public List<Map<String, Object>> getFlatDimensionsB() {
		return flatDimensionResultsB;
	}
	
	public void setDimensionsA(Map<String, DimensionResult> dimensionResultsA, List<Map<String, Object>> flatDimensionResultsA) {
		this.dimensionResultsA = dimensionResultsA;
		this.flatDimensionResultsA = flatDimensionResultsA;
	}

	public void setDimensionsB(Map<String, DimensionResult> dimensionResultsB, List<Map<String, Object>> flatDimensionResultsB) {
		this.dimensionResultsB = dimensionResultsB;
		this.flatDimensionResultsB = flatDimensionResultsB;
	}

	public List<LogicResult> getLogic() {
		return logic;
	}

	public void setLogic(List<LogicResult> logic) {
		this.logic = logic;
	}

	public Map<String, MetricResult> getMetricResults() {
		return metricResults;
	}

	public void setMetricResults(Map<String, MetricResult> metricResults) {
		this.metricResults = metricResults;
	}

	public List<MetricResult> getMetrics() {
		return metrics;
	}
	
	public Metric getMetric(String name) {
		return metricsMap.get(name);
	}

}
