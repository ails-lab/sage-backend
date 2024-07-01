package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PagedValidationOption {
	private String code;
	private String label; 
	private List<PagedValidationCode> dimensions;
	
	private String allQuery;
	private String annotatedQuery;
	private String unannotatedQuery;
	
	public class PagedValidationCode {
		private String code;
		private String name;
		
		public PagedValidationCode(String code, String name) {
			this.code = code;
			this.name = name;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
	
	public PagedValidationOption(String code, String label, List<PagedValidationCode> dimensions) {
		this.code = code;
		this.label = label;
		this.dimensions = dimensions;
	}
	
	public PagedValidationOption(String code, List<String> components, String all, String annotated, String unannotated) {
		String[] dims = code.split("-");
		
		label = "By "; 
		dimensions = new ArrayList<>();
		for (int i = 1; i < dims.length; i++) {
			dimensions.add(new PagedValidationCode(dims[i], components.get(i -1)));
			if (i > 1) {
				label += " & ";
			}
			label += components.get(i -1);
		}
		
		this.code = code;
		this.allQuery = all;
		this.annotatedQuery = annotated;
		this.unannotatedQuery = unannotated;
	}
	
	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public List<PagedValidationCode> getDimensions() {
		return dimensions;
	}

	public void setDimensionsComponents(List<PagedValidationCode> dimensions) {
		this.dimensions = dimensions;
	}
	
	public String getAllQuery() {
		return allQuery;
	}
	
	public void setAllQuery(String allQuery) {
		this.allQuery = allQuery;
	}

	public String getAnnotatedQuery() {
		return annotatedQuery;
	}

	public void setAnnotatedQuery(String annotatedQuery) {
		this.annotatedQuery = annotatedQuery;
	}

	public String getUnannotatedQuery() {
		return unannotatedQuery;
	}

	public void setUnannotatedQuery(String unannotatedQuery) {
		this.unannotatedQuery = unannotatedQuery;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}
	
}