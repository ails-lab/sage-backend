package ac.software.semantic.model.expr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Dimension {

	private String name;
	
	private String datatype;
	
	private List<Dimension> dimensions;
	
//	@JsonIgnore
	private String column;
	
//	@JsonIgnore
	private String template;
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getTemplate() {
		return template;
	}

	public void setTemplate(String template) {
		this.template = template;
	}
	
//	public void prepare() {
//		if (filters != null) {
//	    analyzer = new Analyzer() {
//	        @Override
//	        protected TokenStreamComponents createComponents(String s) {
//	            Tokenizer tokenizer = new NoTokenizer();
//	            TokenStream result = null;
//				try {
//					for (int i = 0 ; i < filters.size(); i++) {
//						Filter filt = filters.get(i);
//						if (i == 0) {
//							result = Computation.filters.get(filt.getName()).newInstance(tokenizer, filt.getParameters());
//						} else {
//							result = Computation.filters.get(filt.getName()).newInstance(result, filt.getParameters());
//						}
//					}
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//	            
//	        
//	            return new TokenStreamComponents(tokenizer, result);
//	        } };
//		}		
//	}
	
	public DimensionResult evaluate(Map<String, Object> inputs) {
		Object value = null;
		
//		System.out.println("HERE");
//		System.out.println(inputs);
//		System.out.println(column);
		if (column != null) {
			value = inputs.get(column);
			
		} else if (template != null) {
			Matcher matcher = Computation.variablePattern.matcher(template);

			Set<String> variables = new HashSet<>();
			while (matcher.find()) {
				variables.add(matcher.group(1));
			}
			
			String v = template;
			for (String expr : variables) {
				Object exprValue = inputs.get(expr);
				if (exprValue == null) {
					v = null;
					break;
				}
				v = v.replaceAll("\\{" + expr + "\\}", exprValue.toString());
			}
			
			value = v;
		} else if (dimensions != null) {
			value = new ArrayList<Map<String, DimensionResult>>();
			ArrayList<Map<String, Object>> array = (ArrayList<Map<String, Object>>)inputs.get(name);
			
			if (array == null) {
				System.out.println("ERROR INPUTS " + inputs);
				System.out.println("ERROR NAME " + name);
			}
			for (Map<String, Object> entry : array) {
				Map<String, DimensionResult> r = new HashMap<>();
				for (Dimension dim : dimensions) {
					DimensionResult v2 = dim.evaluate(entry);
					if (v2.getValue() != null) {
						r.put(dim.getName(), v2);
					}
				}
				
				((ArrayList<Map<String, DimensionResult>>)value).add(r);
			}
		}
		
//		System.out.println(" DVAL " + name + " : " + value);
		
		return new DimensionResult(this, value);
	}

	public List<Dimension> getDimensions() {
		return dimensions;
	}

	public void setDimensions(List<Dimension> dimensions) {
		this.dimensions = dimensions;
	}
	
}
