package ac.software.semantic.payload.request;

import java.util.List;

import ac.software.semantic.model.AnnotationEditFilter;

public class FilterAnnotationValidationUpdateRequest implements UpdateRequest {

	private String name; 
	private List<AnnotationEditFilter> filters;
	
	public FilterAnnotationValidationUpdateRequest() { }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<AnnotationEditFilter> getFilters() {
		return filters;
	}

	public void setFilters(List<AnnotationEditFilter> filters) {
		this.filters = filters;
	}

	
}