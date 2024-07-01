package ac.software.semantic.model.expr;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.PreprocessInstruction;
import edu.ntua.isci.ac.common.math.Combinatory;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.FunctionProcessor;
import edu.ntua.isci.ac.d2rml.vocabulary.FunctionResult;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Metric {
	
	private String name;
	
	private String function;
//	private Resource functionAsResource;
	private String datatype;

	private List<PreprocessInstruction> preprocess;

	private List<String> dimensions;
	private List<String> dimensionsB;
	
//	public String toString() {
//		return name + "/" + dimension;
//	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

//	public List<String> getDimensions() {
//		return dimensions;
//	}

	public void setDimensions(List<String> dimensions) {
		this.dimensions = dimensions;
	}

	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	
	
//	public MetricResult evaluate(Map<String, DimensionResult> inputA, Map<String, DimensionResult> inputB, PrintStream out) throws Exception {
//		functionAsResource = Vocabulary.resolve(function);
//		
////		System.out.println("METR " + name + " " + functionAsResource);
////		System.out.println("METR " + name + " " + inputA);
////		System.out.println("METR " + name + " " + inputB);
////		System.out.println("DIMENSION " + dimension);
//
//		List<Object> dimensionInputA; 	
//		
//		if (inputA.get(dimension) != null && inputA.get(dimension).getValue() != null) {
//			Object v = inputA.get(dimension).getValue();
//			if (v instanceof List) {
//				dimensionInputA = (List<Object>) v;
//			} else {
//				dimensionInputA = new ArrayList<>();
//				dimensionInputA.add(v);
//			}
//		} else {
//			return new MetricResult(this, null);
//		}
//		
//		List<Object> dimensionInputB;
//		if (inputB.get(dimension) != null && inputB.get(dimension).getValue() != null) {
//			Object v = inputB.get(dimension).getValue();
//			if (v instanceof List) {
//				dimensionInputB = (List<Object>) v;
//			} else {
//				dimensionInputB = new ArrayList<>();
//				dimensionInputB.add(v);
//			}
//		} else {
//			return new MetricResult(this, null);
//		}
//
////		System.out.println("METZ " + functionAsResource + " " + inputs);
//		
//		Object value = null;
//		Combinatory c = new Combinatory(new int[] { dimensionInputA.size(), dimensionInputB.size() });
//		
//		loop:
//		while (c.hasNext()) {
//			int[] p = c.next();
//			
//			Map<String, Object> inputs = new HashMap<>();
//			Object vA = dimensionInputA.get(p[0] - 1);
//			Object vB = dimensionInputB.get(p[1] - 1);
//			
//			if (preprocess != null) {
//				String sA = vA.toString();
//				String sB = vB.toString();
//
//				for (PreprocessInstruction mp : preprocess) {
//					Map<String, Object> valueMapA = new HashMap<>();
//					for (DataServiceParameterValue vp : mp.getParameters()) {
//						valueMapA.put(vp.getName(), vp.getValue());
//					}
//					valueMapA.put(D2RMLOPVocabulary.input, sA);
//					List<FunctionResult<Object>> resA = D2RMLOPVocabulary.evaluateFunction(0, null, ResourceFactory.createResource(mp.getFunction()), valueMapA);					
//
//					Object rA = resA.get(0).getDefaultValue();
//
//					if (rA instanceof Boolean) {
//						if (((boolean)rA == false && mp.getModifier() == null) || ((boolean)rA == true && mp.getModifier() != null && mp.getModifier().equals(D2RMLOPVocabulary.logicalNot.toString()))) {
//							
//							if (out != null) {
//								out.println("METRIC " + this.name + ":" + dimension + "/" + Arrays.toString(p) + " : " +  functionAsResource + " : " + vA + " / " + vB + " = NULL");
//							}
//
//							continue loop;
//						}
//					} else {
//						sA = rA.toString();
//					}
//
//					
//					Map<String, Object> valueMapB = new HashMap<>();
//					for (DataServiceParameterValue vp : mp.getParameters()) {
//						valueMapB.put(vp.getName(), vp.getValue());
//					}
//					valueMapB.put(D2RMLOPVocabulary.input, sB);
//					
//					List<FunctionResult<Object>> resB = D2RMLOPVocabulary.evaluateFunction(0, null, ResourceFactory.createResource(mp.getFunction()), valueMapB);
//
//					Object rB = resB.get(0).getDefaultValue();
//
//					if (rB instanceof Boolean) {
//						if (((boolean)rB == false && mp.getModifier() == null) || ((boolean)rB == true && mp.getModifier() != null && mp.getModifier().equals(D2RMLOPVocabulary.logicalNot.toString()))) {
//							
//							if (out != null) {
//								out.println("METRIC " + this.name + ":" + dimension + "/" + Arrays.toString(p) + " : " +  functionAsResource + " : " + vA + " / " + vB + " = NULL");
//							}
//							
//							continue loop;
//						}
//					} else {
//						sB = rB.toString();
//					}
//				}
//				
//				vA = sA;
//				vB = sB;
//			}
//			
//			inputs.put("#0", vA);
//			inputs.put("#1", vB);
//			
//			List<FunctionResult<Object>> res = FunctionProcessor.evaluate(functionAsResource, inputs, null);
//
//			if (out != null) {
//				out.println("METRIC " + this.name + ":" + dimension + "/" + Arrays.toString(p) + " : " +  functionAsResource + " : " + vA + " / " + vB + " = "  + res);
//			}
//
//			Object newValue = res != null ? res.get(0).getDefaultValue() : null;
//			
//			if (value == null) {
//				value = newValue;
//			} else if (newValue != null) {
//				value = Math.min((double)value, (double)newValue);
//			}
//			
//			if (value != null && (Double)value == 0.0) {
//				break;
//			}
//		}
//
////		if (dimension.startsWith("legalName")) {
////		}
//		
//		return new MetricResult(this, value);
//		
//		
//	}

	public MetricResult evaluate(Double defaultValue, List<Object> dimensionInputA, List<Object> dimensionInputB, PrintStream out) throws Exception {
		Resource functionAsResource = Vocabulary.resolve(function);
		
//		System.out.println("METR " + name + " " + functionAsResource);
//		System.out.println("METR " + name + " " + dimensionInputA + " " + (dimensionInputA != null ? dimensionInputA.size() : "--"));
//		System.out.println("METR " + name + " " + dimensionInputB + " " + (dimensionInputB != null ? dimensionInputB.size() : "--"));
//		System.out.println("DIMENSION " + dimension);
		
		boolean existsA = true;
		boolean existsB = true;

		if (dimensionInputA == null) {
			existsA = false;
		}

		if (dimensionInputB == null) {
			existsB = false;
		}
		
		if (!existsA || !existsB) {
			if (defaultValue != null) {
				return new MetricResult(this, defaultValue);
			} else {
				if (!existsA && !existsB) {
					return new MetricResult(this, -2); // both missing value
				} else {
					return new MetricResult(this, -1); // one missing value
				}
			}
		}

//		System.out.println("METZ " + functionAsResource + " " + inputs);
		
		Object value = null;
		Combinatory c = new Combinatory(new int[] { dimensionInputA.size(), dimensionInputB.size() });
		
		existsA = false;
		existsB = false;
		
		loop:
		while (c.hasNext()) {
			int[] p = c.next();
			
			Map<String, Object> inputs = new HashMap<>();
			Object vA = dimensionInputA.get(p[0] - 1);
			Object vB = dimensionInputB.get(p[1] - 1);
			
			boolean localExistsA = true;
			boolean localExistsB = true;
			
			if (preprocess != null) {
				Object sA = vA.toString();
				Object sB = vB.toString();

				for (PreprocessInstruction mp : preprocess) {
					
					Map<String, Object> valueMapA = new HashMap<>();
					if (mp.getParameters() != null) {
						for (DataServiceParameterValue vp : mp.getParameters()) {
							valueMapA.put(vp.getName(), vp.getValue());
						}
					}
					valueMapA.put(D2RMLOPVocabulary.input, sA);
					
					List<FunctionResult<Object>> resA = D2RMLOPVocabulary.evaluateFunction(0, null, ResourceFactory.createResource(mp.getFunction()), valueMapA);					

					if (resA.size() > 1) {
						sA = new ArrayList<>();
						for (FunctionResult<Object> fr : resA) {
							((List)sA).add(fr.getDefaultValue());
						}
						
						continue;
					}
					
					Object rA = resA.get(0).getDefaultValue();

					if (rA instanceof Boolean) {
						if (((boolean)rA == false && mp.getModifier() == null) || ((boolean)rA == true && mp.getModifier() != null && mp.getModifier().equals(D2RMLOPVocabulary.logicalNot.toString()))) {
							
							if (out != null) {
								out.println("METRIC " + this.name + ":" + getFirstDimensions() +  getSecondDimensions() + " : " +  functionAsResource + " : " + vA + " / " + vB + " = NULL");
							}

//							continue loop;
							localExistsA = false;
							break;
						}
					} else {
						sA = rA;
					}
				}
				
				for (PreprocessInstruction mp : preprocess) {
					
					Map<String, Object> valueMapB = new HashMap<>();
					if (mp.getParameters() != null) {
						for (DataServiceParameterValue vp : mp.getParameters()) {
							valueMapB.put(vp.getName(), vp.getValue());
						}
					}
					valueMapB.put(D2RMLOPVocabulary.input, sB);

					List<FunctionResult<Object>> resB = D2RMLOPVocabulary.evaluateFunction(0, null, ResourceFactory.createResource(mp.getFunction()), valueMapB);

					if (resB.size() > 1) {
						sB = new ArrayList<>();
						for (FunctionResult<Object> fr : resB) {
							((List)sB).add(fr.getDefaultValue());
						}
						
						continue;
					}
					

					Object rB = resB.get(0).getDefaultValue();

					if (rB instanceof Boolean) {
						if (((boolean)rB == false && mp.getModifier() == null) || ((boolean)rB == true && mp.getModifier() != null && mp.getModifier().equals(D2RMLOPVocabulary.logicalNot.toString()))) {
							
							if (out != null) {
								out.println("METRIC " + this.name + ":" + getFirstDimensions() +  getSecondDimensions() + " : " +  functionAsResource + " : " + vA + " / " + vB + " = NULL");
							}

//							continue loop;
							localExistsB = false;
							break;
						}
					} else {
						sB = rB;
					}
				}
				
				vA = sA;
				vB = sB;
			}
			
			existsA |= localExistsA;
			existsB |= localExistsB;

//			System.out.println("VA " + vA + " " + localExistsA + " " + existsA);
//			System.out.println("VB " + vB + " " + localExistsB + " " + existsB);

			if (!localExistsA || !localExistsB) {
				continue;
			}
			
			inputs.put("#0", vA);
			inputs.put("#1", vB);
			
			List<FunctionResult<Object>> res = FunctionProcessor.evaluate(functionAsResource, inputs, null);

			if (out != null) {
				out.println("METRIC " + this.name + ":" + getFirstDimensions() +  getSecondDimensions() + " : " +  functionAsResource + " : " + vA + " / " + vB + " = "  + res);
			}

			Object newValue = res != null ? res.get(0).getDefaultValue() : null;
			
			if (value == null) {
				value = newValue;
			} else if (newValue != null) {
				value = Math.min((double)value, (double)newValue);
			}
			
			if (value != null && (Double)value == 0.0) {
				break;
			}
		}
		
		if (value != null) {
			return new MetricResult(this, value);
		} else {
			if (!existsA && !existsB) {
				return new MetricResult(this, -2); // both missing value
			} else {
				return new MetricResult(this, -1); // one missing value
			}			
		}
	}

	public List<PreprocessInstruction> getPreprocess() {
		return preprocess;
	}

	public void setPreprocess(List<PreprocessInstruction> preprocess) {
		this.preprocess = preprocess;
	}

	public List<String> getDimensionsB() {
		return dimensionsB;
	}

	public void setDimensionsB(List<String> dimensionsB) {
		this.dimensionsB = dimensionsB;
	}
	
	public List<String> getFirstDimensions() {
		return dimensions;
	}

	public List<String> getSecondDimensions() {
		if (dimensionsB != null) {
			return dimensionsB;
		} else {
			return dimensions;
		}
	}

}
