package ac.software.semantic.payload;

import java.util.List;

import org.apache.jena.rdf.model.Property;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.index.IndexElementSelector;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyIndexElementResponse {
	private String property; 
	private ClassIndexElementResponse element;
//	private List<String> language;
	
//	private Integer index;
//	private String target;
	
	private List<IndexElementSelector> selectors;
	
	public PropertyIndexElementResponse() {		
	}

	public PropertyIndexElementResponse(String property, ClassIndexElementResponse element) {
		this.property = property;
		this.element = element;
	}

	public PropertyIndexElementResponse(Property property, ClassIndexElementResponse element) {
		this.property = property.toString();
		this.element = element;
	}

	public PropertyIndexElementResponse(String property) {
		this.property = property;
		this.element = null;
	}

	public PropertyIndexElementResponse(Property property) {
		this.property = property.toString();
		this.element = null;
	}

	public String getProperty() {
		return property;
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public ClassIndexElementResponse getElement() {
		return element;
	}
	
	public void setElement(ClassIndexElementResponse element) {
		this.element = element;
	}

//	public Integer getIndex() {
//		return index;
//	}
//
//	public void setIndex(Integer index) {
//		this.index = index;
//	}
//
//	public List<String> getLanguage() {
//		return language;
//	}

//	public void setLanguage(List<String> language) {
//		this.language = language;
//	}

//	public String getTarget() {
//		return target;
//	}
//
//	public void setTarget(String target) {
//		this.target = target;
//	}

	public List<IndexElementSelector> getSelectors() {
		return selectors;
	}

	public void setSelectors(List<IndexElementSelector> selectors) {
		this.selectors = selectors;
	}
}