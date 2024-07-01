package ac.software.semantic.model.expr;

import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.jena.rdf.model.Resource;

import org.nfunk.jep.ParseException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.ntua.isci.ac.d2rml.datatype.GeoPoint;
import edu.ntua.isci.ac.lod.vocabularies.GeoSparqlVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.XSDVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Computation {
	
	public static Pattern variablePattern = Pattern.compile("\\{(\\$?[A-z0-9_]+?)(?::([0-9\\.]+))?\\}");

	private List<Dimension> dimensions;
	private List<Metric> metrics;
	
	private List<Logic> logic;
	
	public List<Dimension> getDimensions() {
		return dimensions;
	}
	
	public void setDimensions(List<Dimension> dimensions) {
		this.dimensions = dimensions;
	}

	public List<Metric> getMetrics() {
		return metrics;
	}

	public List<Logic> getLogic() {
		return logic;
	}

	public void setLogic(List<Logic> logic) {
		this.logic = logic;
	}
	
//	private static List<Map<String, DimensionResult>> flatten(Map<String, DimensionResult> input) {
//		
////		System.out.println("FLATTENING " + input);
//		
//		List<Map<String, DimensionResult>> res = new ArrayList<>();
//		
//		for (Map.Entry<String, DimensionResult> entry : input.entrySet()) {
//			
//			String dkey = entry.getKey();
//			DimensionResult dval = entry.getValue();
//
////			System.out.println("KEY " + dkey);
//
//			if (dval.getDatatype() == null) {
//				List<Map<String, DimensionResult>> value = (List<Map<String, DimensionResult>>)dval.getValue();
//				
//				List<List<Map<String, DimensionResult>>> flattenedValue = new ArrayList<>();
//				
//				for (int i = 0; i < value.size(); i++) {
//					flattenedValue.add(flatten(value.get(i)));
//				}
//				
//				if (res.isEmpty()) {
//					res.add(new HashMap<>());
//				}
//
////				System.out.println("RES1_BEFORE " + res);
//
//				List<Map<String, DimensionResult>> newOut = new ArrayList<>(); 
//
//				for (Iterator<Map<String, DimensionResult>> iter = res.iterator(); iter.hasNext();) {
//					Map<String, DimensionResult> out = iter.next();
//					
//					for (List<Map<String, DimensionResult>> fv : flattenedValue) {
//						Map<String, DimensionResult> newVOut = new HashMap<>();
//						newVOut.putAll(out);
//						newOut.add(newVOut);
//					
//						for (Map<String, DimensionResult> vout : fv) {
//							for (Map.Entry<String, DimensionResult> ventry : vout.entrySet()) {
//								newVOut.put(dkey + "." + ventry.getKey(), ventry.getValue());
//							}
//						}
//					}
//					iter.remove();
//				}
//				
//				res.addAll(newOut);
//				
////				System.out.println("RES1_AFTER " + res);
//				
//			} else {
//				if (res.isEmpty()) {
//					res.add(new HashMap<>());
//				}
//				
////				System.out.println("RES2_BEFORE " + res);
//				
//				for (Map<String, DimensionResult> out : res) {
//					out.put(dkey, dval);
//				}
//				
////				System.out.println("RES2_AFTER " + res);
//			}
//			
//		}
//		
////		System.out.println("R ATTENING " + res);
//		return res;
//	}
	
	private static List<Map<String, Object>> flatten(Map<String, DimensionResult> input) {
		
//		System.out.println("FLATTENING " + input);
		
		List<Map<String, Object>> res = new ArrayList<>();
		
		for (Map.Entry<String, DimensionResult> entry : input.entrySet()) {
			
			String dkey = entry.getKey();
			DimensionResult dval = entry.getValue();

//			System.out.println("KEY " + dkey);

			if (dval.getDatatype() == null) {
				List<Map<String, DimensionResult>> value = (List<Map<String, DimensionResult>>)dval.getValue();
				
				List<List<Map<String, Object>>> flattenedValue = new ArrayList<>();
				
				for (int i = 0; i < value.size(); i++) {
					flattenedValue.add(flatten(value.get(i)));
				}
				
				if (res.isEmpty()) {
					res.add(new HashMap<>());
				}

//				System.out.println("RES1_BEFORE " + res);

				List<Map<String, Object>> newOut = new ArrayList<>(); 

				for (Iterator<Map<String, Object>> iter = res.iterator(); iter.hasNext();) {
					Map<String, Object> out = iter.next();
					
					for (List<Map<String, Object>> fv : flattenedValue) {
						Map<String, Object> newVOut = new HashMap<>();
						newVOut.putAll(out);
						newOut.add(newVOut);
					
						for (Map<String, Object> vout : fv) {
							for (Map.Entry<String, Object> ventry : vout.entrySet()) {
								newVOut.put(dkey + "." + ventry.getKey(), ventry.getValue());
							}
						}
					}
					iter.remove();
				}
				
				res.addAll(newOut);
				
//				System.out.println("RES1_AFTER " + res);
				
			} else {
				if (res.isEmpty()) {
					res.add(new HashMap<>());
				}
				
//				System.out.println("RES2_BEFORE " + res);
				
				for (Map<String, Object> out : res) {
					out.put(dkey, dval.getValue());
				}
				
//				System.out.println("RES2_AFTER " + res);
			}
			
		}
		
//		System.out.println("R ATTENING " + res);
		return res;
	}
		
	
	public ComputationResult execute(Map<String, Object> inputsA, Map<String, Object> inputsB, PrintStream out) throws Exception {
		Map<String, DimensionResult> dimensionResultsA = new HashMap<>();
		Map<String, DimensionResult> dimensionResultsB = new HashMap<>();
		
//		System.out.println("IA " + inputsA);
//		System.out.println("IB " + inputsB);
		
		for (Dimension dimension : dimensions) {
//			dimension.prepare();
//			System.out.println("DIM " + dimension.getName());
			DimensionResult drA = dimension.evaluate(inputsA);
			if (drA.getValue() != null) {
				dimensionResultsA.put(dimension.getName(), drA);
//				System.out.println("DIM " + dimension.getName() + " " + drA.getValue());
			}
			DimensionResult drB = dimension.evaluate(inputsB);
			if (drB.getValue() != null) {
				dimensionResultsB.put(dimension.getName(), drB);
//				System.out.println("DIM " + dimension.getName() + " " + drB.getValue());
			}
		}
		
//		System.out.println("DRA " + dimensionResultsA);
//		System.out.println("DRA " + flatten(dimensionResultsA));
//		System.out.println("DRB " + dimensionResultsB);
//		System.out.println("DRB " + flatten(dimensionResultsB));
		
//		Map<String, MetricResult> metricResults = new LinkedHashMap<>();
//		for (Metric metric : metrics) {
////			System.out.println("MET " + metric.getName());
//			metricResults.put(metric.getName(), metric.evaluate(dimensionResultsA, dimensionResultsB));
//		}

		ComputationResult cr = new ComputationResult();
		cr.setDimensionsA(dimensionResultsA, flatten(dimensionResultsA));
		cr.setDimensionsB(dimensionResultsB, flatten(dimensionResultsB));
		cr.setMetricsMap(metrics);
		
//		System.out.println("METR " + metricResults);
		
		List<LogicResult> logicResults = new ArrayList<>();
		cr.setLogic(logicResults);
		
		for (int i = 0; i < logic.size(); i++) {
		    Logic logc = logic.get(i);
		    
		    if (out != null) {
		    	out.println("\nLOGIC " + i + " " + logc.getCondition());
		    }
		    
			LogicResult res = logc.evaluate(cr, out);
			if (res != null) {
				logicResults.add(res);
				res.setRule(i + 1);
				if (res.getStop()) {
					cr.setResult(res.getValue());	
					
//					System.out.println("METR " + cr.getMetricResults());
					if (out != null) {
						out.println("RESULT " + res.getValue());
					}
					
					return cr;
				}
			}
		}
		
//		System.out.println("METR " + cr.getMetricResults());
		return null;
	}
	
//	public static Object typedValue(Object value, Resource datatype) {
//		String valueAsString = value.toString();
//		if (datatype.equals(XSDVocabulary.string)) {
//			return valueAsString;
//		} else if (datatype.equals(XSDVocabulary.integer)) {
//			Double r = Double.parseDouble(valueAsString);
//			return r.intValue();
//		} else if (datatype.equals(XSDVocabulary.decimal)) {
//			return Double.parseDouble(valueAsString);
//		} else if (datatype.equals(GeoSparqlVocabulary.wktLiteral)) {
//			return new GeoPoint(valueAsString);
//		} else {
//			//...
//		}
//		
//		return null;
//	}

	
	
	public static void main(String[] args) throws Exception {
		
//		System.out.println(TokenizerFactory.availableTokenizers());
//		System.out.println(TokenFilterFactory.availableTokenFilters());
//		System.out.println(CharFilterFactory.availableCharFilters());
		
//		AnalysisSPILoader<TokenizerFactory> loader = new AnalysisSPILoader<>(TokenizerFactory.class);
//		System.out.println(loader.availableServices());
		
//		Analyzer analyzer = CustomAnalyzer.builder()
//		        .withTokenizer("icu")  
//		        .addTokenFilter("lowercase")  
//		        .addTokenFilter("asciiFolding")  
//			      .build();
		
//		    Analyzer analyzer = new Analyzer() {
//		        @Override
//		        protected TokenStreamComponents createComponents(String s) {
//		            Tokenizer tokenizer = new NoTokenizer();
//		            TokenStream result = null;
//					try {
//						result = filters.get("asciifolding").newInstance(tokenizer,);
//						result = filters.get("lowercase").newInstance(result);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//		            
//		        
//		            return new TokenStreamComponents(tokenizer, result);
//		        } };
		                         
//		                         ScandinavianNormalizationFilter
//		                         LowerCaseFilter
		    
//		    TokenStream tokenStream = analyzer.tokenStream("*", "MY TEST an I. How are you Société");
//		    CharTermAttribute attr = 
//		             tokenStream.addAttribute(CharTermAttribute.class);
//		    
//		    List<String> result = new ArrayList<>();
//		    
//		    tokenStream.reset();
//		    while (tokenStream.incrementToken()) {
//		        result.add(attr.toString());
//		    }
//		    System.out.println(result);
		
		
		
		String content = new String(Files.readAllBytes(Paths.get("D:/data/cordis/computation.json")));
		
//		System.out.println(content);
		
		ObjectMapper objectMapper = new ObjectMapper();
		Computation computation = objectMapper.readValue(content, Computation.class);
		
		System.out.println(objectMapper.writeValueAsString(computation));
		
		Map<String, Object> inputsA = new HashMap<>();
		inputsA.put("longitude", "54.66");
		inputsA.put("latitude", "34.66");
		inputsA.put("postalCode", "15646");
		inputsA.put("legalName", "Alex. and Sons!");
		inputsA.put("vatNumber", "08765462s");
		inputsA.put("addressCountry", "EL");

		Map<String, Object> inputsB = new HashMap<>();
		inputsB.put("longitude", "54.66");
		inputsB.put("latitude", "34.66");
		inputsB.put("postalCode", "15646");
		inputsB.put("legalName", "Alex and Sonfb");
		inputsB.put("vatNumber", "087654623");
		inputsB.put("addressCountry", "EL");
		
//		System.out.println(inputsA);
//		System.out.println(inputsB);
		
		computation.execute(inputsA, inputsB, System.out);
		
//		System.out.println(UUID.randomUUID());
	}
	
}
