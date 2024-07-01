package ac.software.semantic.model.expr;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.jena.rdf.model.Resource;
import org.nfunk.jep.JEP;

import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.common.math.Combinatory;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;
import edu.ntua.isci.ac.lod.vocabularies.XSDVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Logic {

	private String expression;

//	@JsonIgnore
	private String datatype;

	private String condition;

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

//	public static void main(String[] args) throws Exception {
//		JEP mathParser = new JEP();
//		mathParser.addStandardFunctions();
//		mathParser.addStandardConstants();
//	
//		String expr = "! $x && y > 0";
//		
//		mathParser.addVariable("$x", 0);
//		mathParser.addVariable("y", 1);
//		
//		System.out.println(mathParser.evaluate(mathParser.parse(expr)));
//	}

	public LogicResult evaluate(ComputationResult cr, PrintStream out) throws Exception {
		JEP mathParser = new JEP();
		mathParser.addStandardFunctions();
		mathParser.addStandardConstants();

//		System.out.println("EVAL EXPR " + expression);
		List<Double> expressionResultList = evaluate(mathParser, cr, expression, false, out);
		Double expressionResult = null;

		if (expressionResultList == null) {
			return null;
		} else {
			expressionResult = expressionResultList.get(0);

			for (int i = 1; i < expressionResultList.size(); i++) {
				expressionResult = Math.min(expressionResult, expressionResultList.get(i));
			}
		}

//		System.out.println("EXPR " + expressionResultList + " / " + expressionResult);

		mathParser.addVariable("expression", expressionResult);

		String effectiveReturnCondition = condition;

		effectiveReturnCondition = effectiveReturnCondition.replaceAll("\\{expression\\}", "expression");

//		System.out.println("EVAL COND " + expression);
		List<Double> returnConditionResultList = evaluate(mathParser, cr, effectiveReturnCondition, true, out);

		if (returnConditionResultList == null) {
			return null;
		} else {
			Double returnConditionResult = returnConditionResultList.get(0);

			for (int i = 1; i < returnConditionResultList.size(); i++) {
				returnConditionResult = Math.max(returnConditionResult, returnConditionResultList.get(i));
			}

//			System.out.println("COND " + returnConditionResultList + " / " + returnConditionResult);

			return new LogicResult(this, expressionResult, returnConditionResult == 1.0);
		}
	}

	private List<Map<String, Object>> projectDimensions(List<Map<String, Object>> flatDimensions, Set<String> cdims) {
		List<Map<String, Object>> res = new ArrayList<>();

		for (Map<String, Object> v : flatDimensions) {
			Map<String, Object> mA = new HashMap<>();
			for (String cdim : cdims) {
				Object nv = v.get(cdim);
				if (nv != null) {
					mA.put(cdim, nv);
				}
			}

			if (mA.size() == 0) {
				continue;
			}

			boolean add = true;
			for (Map<String, Object> exv : res) {
				boolean same = true;
				if (exv.size() != mA.size()) {
					same = false;
				} else {
					for (Map.Entry<String, Object> ev : exv.entrySet()) {
						Object v2 = mA.get(ev.getKey());
						if (v2 == null || !ev.getValue().equals(mA.get(ev.getKey()))) {
							same = false;
							break;
						}
					}
				}
				if (same) {
					add = false;
					break;
				}
			}

			if (add) {
				res.add(mA);
			}
		}

		return res;
	}

	class Variable {
		String expr;
		String cleanExpr;
		String storeExpr;
		String name;
		String fullName;
		Double defaultValue;
		boolean existVariable = false;

		Variable(String expr, String name, Double defaultValue) {
			this.expr = expr;
			this.cleanExpr = expr.substring(1, expr.length() - 1);
			this.storeExpr = cleanExpr;
			this.name = name;
			this.fullName = name;
			this.defaultValue = defaultValue;

			if (name.startsWith("$")) {
				existVariable = true;
				this.name = name.substring(1);
				this.storeExpr = storeExpr.substring(1);
			}
		}
	}

	private List<Double> evaluate(JEP mathParser, ComputationResult cr, String expression, boolean isCondition, PrintStream out) throws Exception {
		Matcher expressionMatcher = Computation.variablePattern.matcher(expression);

		Set<Variable> expressionVariables = new HashSet<>();
		while (expressionMatcher.find()) {
//			System.out.println(expressionMatcher.group(0));
//			System.out.println(expressionMatcher.group(1));
//			System.out.println(expressionMatcher.group(2));
			expressionVariables.add(new Variable(expressionMatcher.group(0), expressionMatcher.group(1), expressionMatcher.group(2) != null ? Double.parseDouble(expressionMatcher.group(2)) : null));
		}

//		System.out.println();
//		System.out.println("EXPR " + expression);
//		System.out.println("VARS " + expressionVariables);
//		System.out.println("FA " + cr.getFlatDimensionsA());
//		System.out.println("FB " + cr.getFlatDimensionsB());
//		System.out.println("VM " + cr.metricsMap);

		Set<String> cdims = new HashSet<>();
		String effectiveExpression = expression;

		List<Double> expressionResult = new ArrayList<>();

		if (expressionVariables.size() > 0) {
			for (Variable vvar : expressionVariables) {
//				System.out.println("VAR " + vvar.expr + "" + vvar.cleanExpr + " " + vvar.fullName + " " + vvar.name);
//				 System.out.println("VAR " + var + " " + cr.getMetric(var).getDimension());
//				String var = vvar.name;
//				if (var.startsWith("$")) {
//					var = var.substring(1);
//				}
				Metric m = cr.getMetric(vvar.name);
				
				cdims.addAll(m.getFirstDimensions());
				cdims.addAll(m.getSecondDimensions());
				
//				System.out.println(m.getFirstDimensions() + " " + m.getSecondDimensions() + " " + m.getDimensions() + " " + m.getDimensionsB());
			}

			List<Map<String, Object>> fA = projectDimensions(cr.getFlatDimensionsA(), cdims);
			List<Map<String, Object>> fB = projectDimensions(cr.getFlatDimensionsB(), cdims);

			Combinatory c = new Combinatory(new int[] { fA.size(), fB.size() }, true);
//			System.out.println("DIMS " + cdims);
//			System.out.println("INPUT A " + fA);		
//			System.out.println("INPUT B " + fB);

			loop: while (c.hasNext()) {
				int[] p = c.next();

//				System.out.println("COMB " + Arrays.toString(p));
				for (Variable vvariable : expressionVariables) {
//					String variable = vvariable.name;
//					 System.out.println("VAR 1 " + vvariable.name);

					Object value = null;

					Map<String, Object> sA = fA.get(p[0] - 1);
					Map<String, Object> sB = fB.get(p[1] - 1);

					Metric metric = cr.getMetric(vvariable.name);

					List<Object> vListA = null;
					List<Object> vListB = null;
					
					List<String> dimF = metric.getFirstDimensions();
					List<String> dimS = metric.getSecondDimensions();
					
					for (String dim : dimF) {
						Object vA = sA.get(dim);
						if (vA != null) {
							if (vListA == null) {
								vListA = new ArrayList<>();
							}
							if (vA instanceof List) {
								vListA.addAll((List) vA);
							} else {
								vListA.add(vA);
							}
						}
					}
					
					for (String dim : dimS) {
						Object vB = sB.get(dim);
						if (vB != null) {
							if (vListB == null) {
								vListB = new ArrayList<>();
							}
							if (vB instanceof List) {
								vListB.addAll((List) vB);
							} else {
								vListB.add(vB);
							}
						}
					}
					
					if (dimF != dimS && (vListA == null || vListB == null)) { //try to invert if different dimensions and one missing
						for (String dim : dimS) {
							Object vA = sA.get(dim);
							if (vA != null) {
								if (vListA == null) {
									vListA = new ArrayList<>();
								}
								if (vA instanceof List) {
									vListA.addAll((List) vA);
								} else {
									vListA.add(vA);
								}
							}
						}
						
						for (String dim : dimF) {
							Object vB = sB.get(dim);
							if (vB != null) {
								if (vListB == null) {
									vListB = new ArrayList<>();
								}
								if (vB instanceof List) {
									vListB.addAll((List) vB);
								} else {
									vListB.add(vB);
								}
							}
						}
					}
					
					MetricResult metricResult = cr.getMetricResults().get(vListA + "/" + vListB + "//" + vvariable.storeExpr);
//						System.out.println("GET " + vA + "/" + vB + "//" + vvariable.storeExpr + " >> " + metricResult);

					if (metricResult == null) {
						metricResult = cr.evaluateMetric(vvariable, metric, vListA, vListB, out);
					}

//					 System.out.println("VAR 3 " + metricResult.getValue() + " " + vvariable.fullName);

					Object v = metricResult.getValue();
					
					if (v instanceof Integer && ((int)v) < 0) {
						if (vvariable.existVariable) {
							value = ((int)v) + 2;  // 0 no value, 1 one value, 2 two values
						} else {
							continue loop;
						}
					} else {
						if (vvariable.existVariable) {
							value = 2;
						} else {
							value = metricResult.getValue();
						}
					}


					mathParser.addVariable(vvariable.fullName, value);
//					 System.out.println(vvariable.fullName + " > " + value);

					if (vvariable.existVariable) {
						effectiveExpression = effectiveExpression.replaceAll("\\{\\" + vvariable.cleanExpr + "\\}", "\\" + vvariable.fullName);
					} else {
						effectiveExpression = effectiveExpression.replaceAll("\\{" + vvariable.cleanExpr + "\\}", vvariable.fullName);
					}

				}

				// System.out.println(effectiveExpression);
				expressionResult.add((Double) mathParser.evaluate(mathParser.parse(effectiveExpression)));
			}
		} else {
			expressionResult.add((Double) mathParser.evaluate(mathParser.parse(effectiveExpression)));
		}

		if (expressionResult.size() == 0) {
			expressionResult = null;
		}

//		System.out.println("RESULT " + expressionResult);
		return expressionResult;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

}
